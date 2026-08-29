# 基于 tRPC-Agent-Go 的多租户节点化 Agent 平台架构设计

## 1. 设计目标

平台面向多个企业、团队或业务线提供 Agent 服务。每个租户可以创建自己的 Agent App，绑定不同 IM 入口，选择不同数据后端，配置模型、工具、知识库和审计策略，并支持多个 Worker 节点水平扩展。

核心目标是把单 Agent 进程扩展成可运营的平台服务：

- 租户之间配置、数据、工具、密钥、日志和成本隔离。
- Worker 尽量无状态，不依赖 sticky session。
- 同一 session 的消息有序执行，避免多节点并发覆盖状态。
- IM callback、Gateway 入队、Worker 执行、Tool 调用、Session/Memory 写入和 IM 回复由同一个 `trace_id` 串联。
- InMemory 只用于本地开发和测试，生产使用共享后端。

## 2. 总体架构

平台在逻辑上分为控制面、数据面、状态面、密钥面和观测面。最小部署可以同进程运行；生产环境按流量、权限和故障范围独立部署。

```text
IM 平台 -> Channel Adapter ---------------------> Agent Gateway
HTTP/RPC -> 协议入口 + Auth --------------------> Agent Gateway
Agent Gateway -> PostgreSQL Execution + Dispatch Outbox -> Redis Streams
Redis Streams -> Agent Worker -> tRPC-Agent-Go Runner
Agent Worker -> Session / Memory / Knowledge / Artifact 后端 + 平台 SQL Audit Store
Agent Event / Reply -> 协议入口或 Channel Adapter
```

控制面由 Admin API、版本化配置和凭据管理组成，负责租户、Agent App、通道绑定和运行策略的创建、发布与回滚；它不直接调用 Runner。数据面由 Channel Adapter、协议入口、Agent Gateway、Worker 和 Runner 组成，负责接收消息、执行 Agent 并返回结果。状态面保存任务协调状态和租户选择的共享数据后端；密钥面由 Secret Manager/KMS 提供；观测面由 Telemetry Collector 汇聚指标、链路和脱敏日志。

外部消息先由 Channel Adapter 或 HTTP/RPC 协议入口转换为统一请求。入口完成认证或通道验签后，Agent Gateway 根据可信身份确定 Tenant、App 和 Session，并以原子准入方式固定配置版本、完成幂等校验、分配同一 Session 的顺序号，同时持久化待执行任务和可靠投递记录。这样入口可以快速确认请求，不需要等待模型执行完成。

任务通过可靠投递链路进入 Redis Streams。任意 Agent Worker 都可以消费任务，并按任务中固定的租户、应用和配置版本装配 tRPC-Agent-Go Runner 与对应的数据后端。Worker 不保存长期会话状态：Session、Memory、Knowledge 和 Artifact 均在共享后端中保存，因此同一会话的后续消息可由不同节点处理。PostgreSQL 的会话顺序号与 Redis Session Lease 共同保证同一 Session 串行执行；不同 Session 可并行扩展。

Storage Adapter 根据租户配置选择 Session、Memory、Knowledge 和 Artifact 后端；Audit Event 固定写入平台 SQL Audit Store，审计策略只控制保留、脱敏和查询权限。执行事件可以被持久化并映射回流式 HTTP/RPC 响应；IM 回复则由 Channel Adapter 按通道限制发送。Secret Manager/KMS 只向有作用域的 Worker 提供运行密钥。Telemetry Collector 使用 `tenant_id`、`app_id` 和 `trace_id` 串联入口、执行和存储调用，但不作为审计的唯一副本。

## 3. 多租户与节点部署

平台以 `tenant_id` 作为最高隔离边界。一个 Tenant 下可以有多个 Agent App，每个 App 使用版本化 AppConfig 管理模型配置、工具权限、后端配置、审计策略和 `secret_ref`。Channel Binding 是独立的租户应用资源，负责外部入口的凭据、路由与状态。配置版本不可变；Gateway 入队时绑定 `config_version`，Worker 按该版本装配运行时，避免执行过程中配置漂移。

Channel Binding 的粒度是 `tenant_id + app_id + binding_id`，即一个外部 IM 账号绑定到某个租户下的某个 Agent App。HTTP/RPC 请求的租户身份来自认证 claims、API Key 或内部 RPC 身份；IM 请求的租户身份来自已验签的 Channel Binding。平台不信任外部 payload 中自带的租户字段。入站 API Key 由平台生成，原文只展示一次，数据库只保存绑定 `tenant_id/app_id` 的单向 digest；它与通过 `secret_ref` 获取的模型、IM、数据库和 Tool/MCP 运行密钥是两类凭据。

HTTP/RPC 的 Tenant/App 身份与真实用户身份分开处理。经验证的终端用户 claims 或内部 RPC 用户身份生成 `user_id`，单聊默认以该用户作为 `session_principal_id`。只有 API Key 的服务请求，`user_id` 和 `session_principal_id` 固定归属到该 Credential 的服务主体；调用方只能提供该主体命名空间内的 `session_id`。服务若要代表特定终端用户，必须携带可验证的委托用户身份；普通 payload 不能声明、覆盖或切换 `user_id`、`session_principal_id`。

节点部署采用 `Channel Adapter / HTTP-RPC Entry -> Agent Gateway -> PostgreSQL Execution + Dispatch Outbox -> Redis Streams -> Agent Worker -> Storage Adapter -> Runner`。Gateway 只生成可被 Worker 消费的任务，不绑定具体 Worker；Worker 按任务中的租户、应用、配置版本和会话信息重新装配运行时。

平台不依赖 sticky session。默认部署使用 PostgreSQL 协调配置和任务，平台表与 tRPC-Agent-Go Session 表使用独立逻辑 Schema；Session 后端可按租户配置选择。平台 migration 只管理协调表，选定的 Session Provider 按其实际 DSN 与 Schema 准备框架 Session 表。任意 Worker 都可以处理任意 Session 的下一个可执行 Job；本地只保留当前执行状态和可重建缓存。

Session 的权威分区键为 `tenant_id + app_id + session_principal_id + session_id`。PostgreSQL 为每个分区分配递增 `turn_seq`，保证同一 Session 的后一轮不会越过前一轮；Redis Session Lease 在 Runner 生命周期内串行化实际执行。`execution` 的 `lease_owner`、`run_token` 与 `lease_until` 管理运行状态所有权，续租和完成均要求租约未过期。不同 Session 可以并行，Worker 无需 sticky session，也不使用覆盖 Runner 生命周期的长事务。

Redis Session Lease 续租失败会取消 Runner，Worker 仍排空事件通道；PostgreSQL `run_token` 阻止失效 Worker 覆盖新的 `execution` 状态。本方案不向 tRPC-Agent-Go Session Provider 传递写入栅栏，因此不能保证失去 Redis Lease 的旧 Runner 已停止其最后一次 Session 写入。这是明确接受的残余风险；严格旧写入拒绝仅作为后续可选增强，在所选 Session Provider 支持原子版本条件写入时单独启用。

Session 路由需要区分会话主体和真实发言人。单聊时 `session_principal_id` 通常是用户；群聊时它是群或 thread；`user_id` 始终表示当前真正发送消息的用户。这样群聊可以共享上下文，同时审计、权限和个人 Memory 仍能追踪真实用户。

tRPC-Agent-Go 使用 `app_name + user_id + session_id` 定位 Session。平台将规范化的 `tenant_id/app_id` 编码为稳定 `app_name`，把 `session_principal_id` 传给 Runner 的 `userID` 参数。`app_name` 不包含显示名称或 `config_version`，避免配置升级切断会话。

租户隔离覆盖配置、数据、工具、日志和密钥。控制面和任务协调默认使用共享 PostgreSQL、共享表，主键、唯一键、外键和查询显式包含 `tenant_id/app_id`。标准租户使用共享默认数据层；高隔离租户可通过 `BackendConfig` 选择独立 Schema 或数据库。

迁移分为 PostgreSQL 平台 Schema Migration 和跨后端数据迁移。前者由 PostgreSQL
适配器维护方言相关 DDL、事务和版本校验；后者在 Agent App 的维护窗口内由包外编排层
协调源端、目标端、全量复制、校验、配置切换和回滚窗口，不能归入某一个后端实现。

所有配置带 `tenant_id/app_id/config_version`；Redis key、对象路径和向量过滤也带租户作用域。Tool 在模型可见前过滤一次，执行前再校验一次；日志和 trace 脱敏 token、DSN、API key、完整 PII 和原始 Tool 参数；运行时可取回 Secret 只通过 `secret_ref` 从 Secret Manager 或 KMS 获取。

Tenant 或 Agent App 进入 `SUSPENDED` 后，Gateway 拒绝受影响范围的新请求，Worker 不再 claim 该范围的新 Job；已排队 Job 保持等待，恢复后继续按原顺序执行，运行中的 Job 允许完成。删除必须先暂停并等待任务进入终态，再按保留策略异步清理。

后端迁移由 `data_migration` 的 `DRAINING`、`COPYING`、`VERIFYING` 非终态维护：Gateway 拒绝该 App 的新请求，Worker 继续排空已经接受的 Job；HTTP/RPC 返回带 `Retry-After` 的可重试错误，IM Adapter 在验签后 ACK 但不创建 Execution，并按通道能力发送维护提示。平台不延后执行迁移窗口内的新输入，也不依赖外部 IM 长期重投。确认没有 Session 写入者后执行全量复制和校验，再原子切换后端配置。迁移失败时标记 `FAILED`，旧配置保持 active 并恢复接收。模型、Tool Policy 和审计策略可以通过普通 `config_version` 发布；Session、Memory、Knowledge、Artifact 的权威后端变更只能作为迁移目标版本，经该流程切换，不能直接激活。

## 4. 数据后端与同步策略

平台不重新实现所有存储能力，而是在平台层做租户级后端路由。不同租户可以选择不同后端：

- SQL：适合 Tenant/App 配置、Inbox/Outbox、Audit、metadata，也是默认可选的 Session Provider。
- Redis：适合队列、限流、缓存和可选 Session 热数据。
- 向量库：适合 Memory 和 Knowledge 的检索索引。
- 对象存储：适合文件、附件、Artifact、Knowledge 原文和解析产物。
- InMemory：只用于本地开发和测试。

Session 使用 append-only Event 模型。Worker 执行时按顺序追加 user event、tool/model event、assistant event；Event 持久化成功后再更新 State；Summary 基于已提交 event 异步生成，并记录 `up_to_event_seq`。本方案通过 Redis Session Lease 取消失效 Runner，并以 PostgreSQL `run_token` 保护执行状态；Session Provider 本身不承诺拒绝过期 Worker 的迟到写入。

IM 入站消息使用 `tenant_id + app_id + binding_id + external_message_id` 去重。HTTP/RPC 请求使用认证主体和 client idempotency key 去重。出站回复使用 `tenant_id + app_id + request_id + part_no` 去重，避免重试时重复发消息。

## 5. IM 接入

Channel Adapter 只处理平台协议差异，不保存 Agent 执行状态。它把外部 IM 回调转换成平台标准消息，再把 Agent Event 或回复结果转换回 IM 平台支持的文本、卡片、文件或异步回复。

| 差异 | 企业微信 | 飞书 | 平台处理 |
| --- | --- | --- | --- |
| Binding 与入站校验 | 配置 URL 时以 GET 校验 `msg_signature` 并解密 `echostr`；后续 POST 校验签名并解密回调。Binding 保存 CorpID、AgentID、Token 与 EncodingAESKey | 事件订阅先处理 `url_verification` 并原样返回 `challenge`；后续事件按应用的 Verification Token 及已启用的签名或加密配置校验。Binding 保存 App ID 与对应安全配置 | 已验证 Binding 才能导出 Tenant/App，之后标准化为同一种平台消息 |
| 外部消息与会话标识 | 从解密后的回调提取成员、群聊和消息标识 | 从事件体提取发送者 `open_id`、`chat_id` 和 `message_id` | 在 Binding 作用域内映射 `user_id`、`session_principal_id` 和稳定 `session_id`，并按外部消息标识去重 |
| ACK 与回复 | 平台要求在 5 秒内响应；Inbox 持久化后可返回空 `200`，再通过主动发消息接口异步回复；被动回复必须按协议加密 | URL 校验返回 `challenge`；正常事件在 Inbox 持久化后按事件订阅协议确认，通过消息 Open API 异步发送或更新回复 | Inbox/Execution/Dispatch Outbox 持久化后才 ACK；Reply Outbox 在实际 Adapter 接入时负责异步发送、分片、重试和 DLQ |
| 限流与异常 | Adapter 处理该接口的限流、凭据失效和可用回复格式 | Adapter 处理该接口的限流、凭据失效和可用回复格式 | 不把通道规则泄漏到 Gateway、Worker 或 Session |

企业微信和飞书的具体官方接入形态由实际可用能力决定，上表不承诺某一种固定回调格式；协议细节只留在各自 Adapter 内。

若某个通道提供已验证的消息撤回事件，Adapter 按 `binding_id + external_message_id` 定位原 Inbox/Job：`PENDING` Job 取消；等待审批的 Job 取消并使审批失效；运行中的 Job 请求取消但不承诺撤销已发生的模型、Tool 或回复副作用；已完成 Job 只追加 Audit，不自动删除 Session、Memory、Tool 结果或已发送回复。

## 6. 治理、监控与安全

治理策略通过 Plugin、Guardrail 和 Callback 接入 Runner 链路。

工具权限采用两层控制：Runner 构建前过滤模型可见工具；Tool 执行前再次校验租户、应用、用户、通道、预算、参数、密钥和危险等级。危险工具可以返回 `allow`、`deny` 或 `needs_human_review`。审批状态机和 Tool Result 的持久化接管属于后续治理能力；接入时复用 Execution 条件状态更新和 Dispatch Outbox，不假设当前 Session Provider 存在写入令牌。

监控指标至少包含请求量、错误率、p95/p99 延迟、Runner 执行耗时、模型调用耗时、Tool 调用耗时、Session/Memory 后端延迟、IM 投递成功率、token 消耗、租户成本和队列积压。

审计日志至少包含：

```text
tenant_id, channel, user_id, session_id, agent_name,
tool_name, decision, latency, error_type, cost, trace_id
```

日志、trace 和错误报告不能包含 IM token、模型 API key、数据库密码、Authorization、Cookie、DSN、完整 PII 或原始 Tool 参数。

Trace、Metrics 和普通日志允许异步上报，采集失败不阻塞任务执行。审计日志承担合规与追责时属于业务权威记录，应写入 SQL Audit Store；跨后端投递使用事务 Outbox，不能把 Telemetry Collector 作为唯一副本。

## 7. 故障恢复与运维

节点故障时，Redis Pending 条目由 Consumer Group 重领，过期的 Execution 运行租约由 PostgreSQL 恢复并重新写入 Dispatch Outbox；IM 重试由 Inbox 幂等表拦截；IM 发送失败的 Reply Outbox 在实际 IM Adapter 接入时退避重试。PostgreSQL 暂时不可用时，Gateway 不伪造受理成功：IM 不 ACK 以请求通道重投，HTTP/RPC 返回可重试错误。非幂等 Tool 的网络结果未知时不自动重试，标记为 `UNKNOWN`，待人工处置并持久化结果后恢复关联 Session。

模型超时和节点关停都通过 `context.Context` 控制。Worker 取消任务后仍要消费 Runner Event Channel 到关闭，避免 goroutine 泄漏，并在退出前 flush telemetry。

配置采用版本化发布。模型、工具和策略变更生成新的 `config_version`，新请求使用新版本，已入队任务继续使用入队时记录的版本。权威数据后端的目标配置不能直接成为 active version，必须通过 `data_migration` 迁移切换。发现问题时保留旧 active version，不修改历史配置内容。

模型、工具和审计策略支持租户级灰度发布：以稳定 Session 分区键确定性分桶，在同一规则和比例下，同一 Session 始终选择同一候选或稳定版本。扩大比例、回滚只影响后续准入任务，已入队和运行中的 Execution 保持固定 `config_version`。容量由压测确定，初始估算为 Worker 并发不低于峰值已接收消息速率乘以 p95 Agent 执行时长；持续观察 Redis Pending 与 Lease 等待、PostgreSQL QPS、模型 token 速率和 IM 回调峰值后留出余量。

最小部署使用一个 `all` 角色进程、一个 PostgreSQL 实例和一个 Redis 实例跑通主链路。PostgreSQL 承载配置、Credential digest、默认 PostgreSQL Session、消息去重、Job、Execution Event、Reply Outbox 和 Audit；Redis 承载 Dispatch Stream 与 Session Lease。控制面与数据面虽然同进程运行，仍通过明确接口隔离。

生产环境可把 Channel Adapter、协议入口/Gateway、Worker、Admin API 和 Telemetry Collector 独立部署并分别扩缩容。PostgreSQL 默认承担配置、准入、Job 顺序、租约协调和 Audit；Redis 可承担缓存、限流或满足原子性和恢复契约的权威 Session 职责。向量库承担 Memory/Knowledge 派生索引，对象存储保存文件和知识库原文；Session Provider 负责其 Session Event、State 和 Summary，SQL 保存平台配置、审计和 metadata。

## 8. tRPC-Agent-Go 复用边界

| 平台需求 | 直接复用 tRPC-Agent-Go | 平台新增能力 |
| --- | --- | --- |
| Agent 编排 | LLMAgent、GraphAgent、Chain、Parallel、Cycle | 租户级 Agent 注册、发布和路由 |
| 执行 | `runner.Runner`、`runner.ManagedRunner`、Event Channel、`context.Context` | QueuedRunner、多租户 Worker 调度、Execution 条件认领与状态所有权控制、配置装配；流式入口按需启用 Execution Event Journal |
| Session / Memory / Artifact / Knowledge | 框架已有接口和多后端实现 | 租户级后端选择、scope 包装、迁移策略 |
| Tool / MCP / Skill | Tool、MCP、Skill 能力 | 工具白名单、执行前校验、密钥注入、危险操作审批 |
| 治理 | Plugin、Guardrail、Callbacks | 租户策略、预算、审计字段 |
| 服务化 | 按产品启用的 `server/*` | Auth、QueuedRunner、原子准入、任务生成；协议 Server 不直连真实 Runner |
| IM 接入 | OpenClaw Gateway / Channel 的职责思想 | 企业微信、飞书及后续通道的绑定、验签、去重和回复适配 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户成本和 SLO 聚合；可靠审计单独持久化 |
