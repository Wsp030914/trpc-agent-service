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

平台运行链路分为入口层、调度层、执行层、状态层和控制面。

```text
IM 平台 / HTTP-RPC 请求
-> Channel Adapter 或协议入口
-> Agent Gateway
-> Session Queue / SQL Job Outbox
-> Agent Worker
-> tRPC-Agent-Go Runner
-> Session / Memory / Knowledge / Artifact / Audit 后端
-> Reply Outbox / Channel Adapter
```

Channel Adapter 是 IM 协议入口，负责企业微信、微信客服、Telegram 等平台的验签、去重、协议解析、ACK 和回复适配。HTTP/RPC 协议入口负责处理 OpenAI-compatible、A2A、内部 RPC 等服务化请求。两类入口最终都转换成平台标准消息，再进入 Agent Gateway。

Agent Gateway 负责认证、租户解析、应用解析、配置版本选择、请求幂等、附件检查和任务生成，不执行 Runner，也不保存 Session、Memory 或 Summary。Gateway 生成带 `tenant_id/app_id/session_id/config_version` 的任务后写入 Session Queue 或 SQL Job Outbox。

Agent Worker 从队列或 SQL Job Outbox 消费任务，加载租户配置，装配模型、工具策略和数据后端，调用 `runner.Runner` 执行 Agent；需要取消和状态管理时使用 `runner.ManagedRunner`。Worker 持续消费 Event Channel 到关闭，并把回复写入 Reply Outbox。

Storage Adapter 根据租户后端配置选择 Session、Memory、Knowledge、Artifact、Audit 等后端。Admin API 管理租户、应用、配置版本、通道绑定、后端配置和策略发布。Telemetry Collector 汇聚各节点带 `tenant_id/app_id/trace_id` 的观测数据。

## 3. 多租户与节点部署

平台以 `tenant_id` 作为最高隔离边界。一个 Tenant 下可以有多个 Agent App，每个 App 使用版本化 AppConfig 管理模型配置、工具权限、通道绑定、后端配置、审计策略和 `secret_ref`。配置版本不可变；Gateway 入队时绑定 `config_version`，Worker 按该版本装配运行时，避免执行过程中配置漂移。

Channel Binding 的粒度是 `tenant_id + app_id + binding_id`，即一个外部 IM 账号绑定到某个租户下的某个 Agent App。HTTP/RPC 请求的租户身份来自认证 claims、API key 或内部 RPC 身份；IM 请求的租户身份来自已验签的 Channel Binding。平台不信任外部 payload 中自带的租户字段。

节点部署采用 `Channel Adapter / HTTP-RPC Entry -> Agent Gateway -> PostgreSQL SQL Job Queue -> Agent Worker -> Storage Adapter -> Runner`。Gateway 只生成可被 Worker 消费的任务，不绑定具体 Worker；Worker 按任务中的租户、应用、配置版本和会话信息重新装配运行时。

平台不依赖 sticky session。首期配置、任务协调和 Session 共用 PostgreSQL 实例，平台表与 tRPC-Agent-Go Session 表使用独立逻辑 Schema。任意 Worker 都可以处理任意 Session 的下一个可执行 Job；本地只保留当前执行状态和可重建缓存。

Session 的权威分区键为 `tenant_id + app_id + session_principal_id + session_id`。PostgreSQL 为每个分区分配递增 `turn_seq`，通过 `lease_epoch` 管理 Job 所有权，并在 Runner 执行期间持有 connection-level advisory lock。同一 Session 严格串行，不同 Session 可以并行；Worker 故障后由租约到期和数据库锁释放触发接管。该设计不使用覆盖 Runner 生命周期的长事务。

Session 路由需要区分会话主体和真实发言人。单聊时 `session_principal_id` 通常是用户；群聊时它是群或 thread；`user_id` 始终表示当前真正发送消息的用户。这样群聊可以共享上下文，同时审计、权限和个人 Memory 仍能追踪真实用户。

tRPC-Agent-Go 使用 `app_name + user_id + session_id` 定位 Session。平台将规范化的 `tenant_id/app_id` 编码为稳定 `app_name`，把 `session_principal_id` 传给 Runner 的 `userID` 参数。`app_name` 不包含显示名称或 `config_version`，避免配置升级切断会话。

租户隔离覆盖配置、数据、工具、日志和密钥。控制面和任务协调默认使用共享 PostgreSQL、共享表，主键、唯一键、外键和查询显式包含 `tenant_id/app_id`。标准租户使用共享默认数据层；高隔离租户可通过 `BackendConfig` 选择独立 Schema 或数据库。RLS 可以作为平台自有共享表的生产防御措施，但不替代应用层作用域和数据库约束。

所有配置带 `tenant_id/app_id/config_version`；Redis key、对象路径和向量过滤也带租户作用域。Tool 在模型可见前过滤一次，执行前再校验一次；日志和 trace 脱敏 token、DSN、API key、完整 PII 和原始 Tool 参数；真实密钥只通过 `secret_ref` 从 Secret Manager 或 KMS 获取。

Tenant 进入 `SUSPENDED` 后，Gateway 拒绝新请求，Worker 不再 claim 该租户的新 Job；已排队 Job 保持等待，恢复后继续按原顺序执行，运行中的 Job 允许完成。删除必须先暂停并等待任务进入终态，再按保留策略异步清理。

## 4. 数据后端与同步策略

平台不重新实现所有存储能力，而是在平台层做租户级后端路由。不同租户可以选择不同后端：

- SQL：适合 Tenant/App 配置、Session/Event、Inbox/Outbox、Audit、metadata。
- Redis：适合队列、限流、缓存和可选 Session 热数据。
- 向量库：适合 Memory 和 Knowledge 的检索索引。
- 对象存储：适合文件、附件、Artifact、Knowledge 原文和解析产物。
- InMemory：只用于本地开发和测试。

Session 使用 append-only Event 模型。Worker 执行时按顺序追加 user event、tool/model event、assistant event；Event 持久化成功后再更新 State；Summary 基于已提交 event 异步生成，并记录 `up_to_event_seq`。

IM 入站消息使用 `tenant_id + app_id + binding_id + external_message_id` 去重。HTTP/RPC 请求使用认证主体和 client idempotency key 去重。出站回复使用 `tenant_id + app_id + request_id + part_no` 去重，避免重试时重复发消息。

## 5. IM 接入

Channel Adapter 只处理平台协议差异，不保存 Agent 执行状态。它把外部 IM 回调转换成平台标准消息，再把 Agent Event 或回复结果转换回 IM 平台支持的文本、卡片、文件或异步回复。

企业微信 / 微信客服侧重点：

- 使用 `msg_signature`、`timestamp`、`nonce`、corp_id/agent_id 或客服账号信息验签。
- 回调可能包含加密消息，需要先解密再进入 Gateway。
- access token、encoding key 等凭据只通过 `secret_ref` 获取。
- 平台要求快速 ACK，Agent 执行通常异步完成，回复通过 Reply Outbox 发送。

Telegram 侧重点：

- 使用 webhook secret 和 bot token 校验来源。
- 用 `bot_id + update_id` 做入站去重。
- 话题群需要用 `chat_id + message_thread_id` 区分 session。
- 遇到 429 按 `Retry-After` 退避发送，401/403 标记绑定异常。

## 6. 治理、监控与安全

治理策略通过 Plugin、Guardrail 和 Callback 接入 Runner 链路。

工具权限采用两层控制：Runner 构建前过滤模型可见工具；Tool 执行前再次校验租户、应用、用户、通道、预算、参数、密钥和危险等级。危险工具可以返回 `allow`、`deny` 或 `needs_human_review`。

监控指标至少包含请求量、错误率、p95/p99 延迟、Runner 执行耗时、模型调用耗时、Tool 调用耗时、Session/Memory 后端延迟、IM 投递成功率、token 消耗、租户成本和队列积压。

审计日志至少包含：

```text
tenant_id, channel, user_id, session_id, agent_name,
tool_name, decision, latency, error_type, cost, trace_id
```

日志、trace 和错误报告不能包含 IM token、模型 API key、数据库密码、Authorization、Cookie、DSN、完整 PII 或原始 Tool 参数。

## 7. 故障恢复与运维

节点故障时，未完成 Job 由队列或 SQL Outbox 重投；IM 重试由 Inbox 幂等表拦截；IM 发送失败进入 Reply Outbox 退避重试，超过次数进入 DLQ 或人工处理。

模型超时和节点关停都通过 `context.Context` 控制。Worker 取消任务后仍要消费 Runner Event Channel 到关闭，避免 goroutine 泄漏，并在退出前 flush telemetry。

配置采用版本化发布。新配置生成新的 `config_version`，新请求使用新版本，已入队任务继续使用入队时记录的版本。发现问题时只切回旧 active version，不修改历史配置内容。

最小部署可以先用 SQL 跑通主链路，把配置、Session、消息去重、任务队列、回复队列和审计日志放在同一个数据库里。生产环境再按访问特点拆分后端：Redis 承担队列、限流和缓存，向量库承担 Memory/Knowledge 检索索引，对象存储保存文件和知识库原文，SQL 继续保存配置、事件、审计和 metadata。

## 8. tRPC-Agent-Go 复用边界

| 平台需求 | 直接复用 tRPC-Agent-Go | 平台新增能力 |
| --- | --- | --- |
| Agent 编排 | LLMAgent、GraphAgent、Chain、Parallel、Cycle | 租户级 Agent 注册、发布和路由 |
| 执行 | `runner.Runner`、`runner.ManagedRunner`、Event Channel、`context.Context` | 多租户 Worker 调度、任务幂等、配置装配 |
| Session / Memory / Artifact / Knowledge | 框架已有接口和多后端实现 | 租户级后端选择、scope 包装、迁移策略 |
| Tool / MCP / Skill | Tool、MCP、Skill 能力 | 工具白名单、执行前校验、密钥注入、危险操作审批 |
| 治理 | Plugin、Guardrail、Callbacks | 租户策略、预算、审计字段 |
| 服务化 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、租户鉴权、任务生成 |
| IM 接入 | OpenClaw Gateway / Channel 的职责思想 | 企业微信、微信客服、Telegram 绑定、验签、去重和回复适配 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户成本、审计日志、SLO 聚合 |

## 9. 交付物索引

- 系统架构图与组件职责：`docs/deliverables/system-architecture.md`
- 核心时序图与 `trace_id/request_id` 链路：`docs/deliverables/sequence-flow.md`
- 数据模型设计：`docs/deliverables/data-model.md`
- 数据同步、并发、幂等和迁移：`docs/deliverables/data-sync-idempotency.md`
- 多后端适配方案：`docs/deliverables/backend-adapter.md`
- 生产风险清单：`docs/deliverables/risk-list.md`
