# 多租户与节点部署详细设计

> 开发用设计草稿，不作为题目交付物。正式交付物以 `docs/deliverables/` 为准。

## 1. 租户模型

平台以 `tenant_id` 作为最高级别的资源和权限隔离边界。一个 Tenant 可以对应一个企业、团队或业务组织，租户下可以创建多个 Agent App。每个 Agent App 使用版本化配置，配置发布后生成不可变的 `AppConfig Version`。

```text
Tenant
-> Agent App
-> AppConfig Version
```

`AppConfig` 承载一个 Agent App 在某个版本下的运行配置，主要包含：

```text
model_config
tool_policy
backend_config
audit_policy
secret_refs
```

Channel Binding 是独立于 AppConfig 的入口资源，负责外部账号、凭据、路由和启停；绑定粒度是：

```text
tenant_id + app_id + binding_id
```

当前共享执行层只接受 `model_config.provider = openai`，可配置 `temperature`、`top_p`、`max_tokens`、`presence_penalty`、`frequency_penalty` 和推理参数。`base_url` 只能作为候选值交给运行时 Endpoint Policy；Policy 必须按密钥作用域解析到受运维批准的 HTTPS 端点。模型 API Key 不能放入 `parameters`；模型配置只保存显式 `APIKeyRef`，Worker 通过运行时 SecretProvider 解析其值。

也就是一个 IM 外部账号绑定到某个租户下的某个 Agent App。这样 Gateway 收到来自已验签通道的消息时，可以直接定位到正确的 `tenant_id` 和 `app_id`。

## 2. 节点部署拓扑

```text
IM -> Channel Adapter -------------------------> Agent Gateway
HTTP/RPC -> 已启用的 tRPC-Agent-Go server/* -> Auth
                                  -> QueuedRunner -> Agent Gateway
-> PostgreSQL Execution + Dispatch Outbox -> Redis Streams
-> Agent Worker
-> Storage Adapter
-> runner.Runner
-> 按需 Execution Event Journal / Reply Outbox
```

| 组件 | 职责 |
| --- | --- |
| Channel Adapter | 处理企业微信、飞书等 IM 平台的验签、去重、协议解析、ACK 和回复适配；不绑定某一种企业微信消息格式 |
| tRPC-Agent-Go `server/*` | 按产品启用对应协议的解析、事件编码和协议级取消能力 |
| QueuedRunner | 实现 `runner.Runner` 适配协议 Server，把一次 Run 转成 Gateway 入队；仅流式或断线恢复入口订阅 Execution Event；不执行真实 Agent |
| Agent Gateway | 执行原子准入：认证结果复核、租户和应用状态校验、配置版本选择、幂等检查、分配 turn、写 Inbox/Execution/Dispatch Outbox 和 `partition_key` |
| PostgreSQL Execution + Dispatch Outbox | 保存最小可恢复执行状态和可靠发布记录；Relay 发布到 Redis Streams |
| Agent Worker | 消费 Job，加载配置，装配模型、工具策略和数据后端，调用 Runner 执行 Agent |
| Storage Adapter | 根据租户 `backend_config` 选择并装配 Session、Memory、Knowledge、Artifact 后端 |
| Platform SQL Audit Store | 保存可靠、追加型 Audit Event；`audit_policy` 只控制保留、脱敏和查询权限 |
| Admin API | 管理 Tenant、Agent App、API Credential、AppConfig Version、Channel Binding、Role Binding、Backend Migration、Tool Policy、Audit Policy 和 SecretRef |
| Telemetry Collector | 汇聚 Channel Adapter、Gateway、Worker、Storage Adapter、Admin API 上报的 trace、metrics 和日志 |

Gateway 不直接调用某台 Worker，而是在准入事务中写入 PostgreSQL Execution 与 Dispatch Outbox，由 Relay 发布到 Redis Streams。这样 IM 回调可以在事务提交后快速 ACK，Worker 故障后 Pending 条目和过期运行记录均可恢复，Gateway 不需要维护 Worker 注册表。

部署拓扑按职责分为控制面、数据面、状态面、密钥面和观测面。Admin API 属于控制面；协议入口、Channel Adapter、Gateway、Worker 和 Runner 属于数据面；PostgreSQL 及租户选择的共享后端属于状态面；Secret Manager/KMS 属于密钥面；Telemetry Collector 属于观测面。最小部署允许这些角色运行在同一进程；生产环境按流量、权限和故障范围独立部署。

Telemetry 采集失败不阻塞主链路。需要可靠留存的 Audit Event 写入平台 SQL Audit Store，不能把 Telemetry Collector 当作审计权威存储。

## 3. 请求入口与租户解析

IM 请求先进 Channel Adapter。Channel Adapter 完成验签、解密和协议解析后，将 IM 原始消息转换为平台标准输入，并根据已验签的 Channel Binding 得到可信的租户来源；Gateway 的 Inbox、Execution 和 Job 事务提交后才 ACK 外部 IM。

HTTP/RPC 请求走协议入口，例如 OpenAI-compatible、A2A 或内部 RPC。该入口负责解析认证信息，请求进入 Gateway 时，`tenant_id/app_id` 必须来自认证 claims、API key 或内部 RPC 身份。

HTTP/RPC 的终端用户身份不能由请求 payload 自称。经验证的用户 claims 或内部 RPC 用户身份生成 `user_id`，单聊默认令 `session_principal_id = user_id`。只有 API Key 的服务请求固定归属到该 Credential 的服务主体，调用方只能提供该主体命名空间内的 `session_id`；代表具体终端用户必须携带可验证的委托用户身份。

平台不信任外部 payload 中携带的租户字段。租户来源只允许两类：

```text
authenticated_claims
verified_channel_binding
```

Gateway 基于可信来源生成 Job。Job 至少携带：

```text
request_id
tenant_id
app_id
config_version
session_id
session_principal_id
user_id
trace_id
message
```

同时生成稳定 `partition_key`：

```text
tenant_id + app_id + session_principal_id + session_id
```

该分区键与 Runner 的 Session 身份维度一致。`user_id` 不进入分区键，否则群聊会按发言人拆分；`config_version` 也不进入分区键，否则配置升级会切断原有会话。

### 3.1 协议 Server 接入

按产品启用的协议入口复用 tRPC-Agent-Go 对应的 `server/*` 实现，但注入的是平台 `QueuedRunner`，不是 Worker 中执行 Agent 的真实 Runner。调用链固定为：

```text
enabled server/* -> Auth -> QueuedRunner -> Gateway -> PostgreSQL Outbox -> Redis Streams
                 <- Execution Event Journal <- Worker <- real runner.Runner
```

`QueuedRunner.Run` 从认证上下文读取可信的租户、应用和用户或服务主体身份，将允许持久化的输入转换成平台命令并提交 Job。需要流式响应或断线恢复的协议入口再订阅 `execution_event`，映射回 `runner.Runner` 的 Event Channel；其他入口只返回其协议定义的已接受或最终结果。外部 `runtimeState` 和 RunOption 不能覆盖已经由入口固定的 `tenant_id`、`app_id`、`user_id`、`session_principal_id`、`config_version`；不可序列化或未列入白名单的 RunOption 直接拒绝。

当前 HTTP API Key 只认证 Tenant/App，并固定产生对应的服务主体，不认证终端用户。`X-User-ID` 或类似请求字段不能声明用户身份；上游若代表具体用户调用，必须提供可验证的委托用户 claims。`X-Session-ID` 只能在当前服务主体或已验证用户主体的命名空间内选择会话，不能跨主体访问。

各协议入口按各自协议语义决定连接断开是否取消尚未完成的 Job；Worker 仍须排空已经创建的 Runner Event Channel。启用流式返回或断线恢复时，`execution_event` 是跨节点返回的持久化日志，不能用进程内 channel 作为唯一来源。

### 3.2 原子准入与配置版本

认证中间件可以先解析 API Key 或 claims，但生产准入必须由 PostgreSQL 中一个短事务完成。该事务按顺序执行：

1. 按固定顺序读取并锁定 Credential 或 Channel Binding、Tenant 和 Agent App，校验它们均为可接收新请求的状态；凭据撤销和状态更新事务取得相同权威行的冲突锁。
2. 从已锁定的 Agent App 读取当前 active config version；配置切换事务更新同一行，保证两者串行。
3. 校验 idempotency key；重复且内容一致时返回原 `request_id`，冲突时拒绝。
4. 锁定或创建 Session lane，分配 `turn_seq`。
5. IM 请求同时写入 Inbox、Execution 和 Dispatch Outbox；HTTP/RPC 写入 Execution 和 Dispatch Outbox。

Admission 事务提交是准入的线性化点。配置切换先提交时，新 Job 使用新版本；Admission 先提交时，该 Job 固定使用旧版本。Worker 始终按 Job 中不可变的 `config_version` 重建 Agent，不读取执行时的 active version。模型、工具和审计策略可以普通发布；Session、Memory、Knowledge、Artifact 的权威后端变更只能作为 `MIGRATING` 的目标配置，不能直接切换 active version。生产准入统一使用原子 Admission；测试可以注入 `Admitter`，但不能用分段读取和独立入队替代该事务。

## 4. Agent Worker 与无状态执行

Agent Worker 从 Redis Streams Consumer Group 接收任务，再在 PostgreSQL 条件认领 Execution。执行前，Worker 按以下字段加载当前任务绑定的配置：

```text
tenant_id
app_id
config_version
```

然后装配运行所需资源：

```text
model_config
tool_policy
Session backend
Memory backend
Knowledge / Artifact backend
Platform SQL Audit Store
```

Relay 将事务内创建的 `dispatch_outbox` 发布到 Redis Streams；Worker 调用 `runner.Runner` 执行 Agent，按 Redis Session Lease 串行同一分区。Runner 返回 Event Channel 后，Worker 必须持续消费到 channel 关闭。完成后按当前 `lease_owner`、未过期 `lease_until` 和 `run_token` 条件更新 Execution 和必要 Audit，成功后才 XACK Redis 条目。

Worker 不保存长期 Session、Memory 或 Summary 状态。它只持有当前任务的运行上下文。任务完成后，状态留在共享后端中，不留在某个 Worker 进程内。

## 5. 不依赖 Sticky Session

平台不需要 sticky session。

Gateway 不把用户请求固定到某个 Worker，Worker 也不保存长期会话状态。一次消息可以由 Worker 1 执行，下一次消息可以由 Worker 3 执行，只要它们都能访问同一套共享 Session / Memory 后端。

共享后端可以复用 tRPC-Agent-Go 的能力，例如：

```text
session/redis
session/mysql
session/postgres
```

会话历史、事件、状态和长期记忆都从共享后端读取和写入。同一 Session 的执行顺序由 PostgreSQL `turn_seq` 和 Redis Session Lease 共同保证；不同 Session 可以并行执行。

## 6. Session 路由

Session 不能只按真实发言人划分。入口层使用以下可信维度解析 Session 路由：

```text
tenant_id + app_id + channel + binding_id + conversation/thread
-> session_principal_id + session_id
```

`session_principal_id` 是平台内部的会话主体，`session_id` 是该主体内的会话标识，不要求在整个 Agent App 内全局唯一。外部用户、群和 thread ID 必须经过已验签 Channel Binding 的映射，不能直接作为可信平台身份。

典型归一化结果：

```text
单聊：session_principal_id = user-1，session_id = default 或业务会话 ID
群聊：session_principal_id = group-1，session_id = default 或业务会话 ID
话题：session_principal_id = thread-1，session_id = default 或业务会话 ID
```

平台需要区分：

```text
session_principal_id  // 会话主体，例如单聊用户、群或 thread
user_id               // 当前真正发送消息的用户
```

群聊中，多个用户共享同一个群 Session，但每条消息仍记录真实 `user_id`。这样既能保持群上下文连续，又能在工具权限、审计日志和个人 Memory 中追踪真实发言人。

平台只保留 `user_id` 和 `session_principal_id` 两种身份，不增加第三种用户 ID。入口层负责把不同会话类型归一化：

```text
单聊：user_id = user-1，session_principal_id = user-1
群聊：user_id = user-1，session_principal_id = group-1
话题：user_id = user-1，session_principal_id = thread-1
```

tRPC-Agent-Go Runner 使用 `app_name + user_id + session_id` 定位 Session。为了让群聊中的不同发言人共享同一个 Session，Worker 调用 Runner 时，把平台的 `session_principal_id` 传给 Runner 的 `userID` 参数：

```text
Runner.Run(ctx, session_principal_id, session_id, message)
```

这里 Runner 的 `userID` 表示 Session 归属主体，不表示当前真实发言人。Worker 仍把平台 `user_id` 放入 runtime state；工具权限、审计、个人 Memory 和成本归属使用该 `user_id`，不能从 Runner 的 Session 主体反推真实发言人。

Runner 的 `app_name` 不能直接使用 `tenant_id + "/" + app_id` 拼接，因为当前 ID 契约只要求非空，分隔符可能产生歧义。平台使用与 `tenant.Scope.Key` 一致的分段转义规则生成稳定值：

```text
tenant:{escaped_tenant_id}:app:{escaped_app_id}:runner
```

`app_name` 不包含显示名称或 `config_version`。该值一旦用于生产 Session 就属于持久化身份契约，后续修改必须迁移已有数据。

### 6.1 顺序、执行租约与 Session Lease

PostgreSQL 为每个 `partition_key` 维护递增的 `turn_seq`。入队事务锁定对应 Session lane 并分配序号；Worker 只能 claim 当前最小的未完成序号。

PostgreSQL 条件 claim 使用短事务写入 `lease_owner`、`run_token`、`lease_until` 和 `attempt`，执行期间由 Worker 续租。完成、续租和重投操作都必须携带当前 `run_token`；续租和完成还必须要求 `lease_until` 晚于数据库当前时间，租约到期本身即撤销旧 Worker 的 Execution 状态更新权。

Redis Session Lease 负责实际 Runner 串行化：Worker 获取由 `partition_key` 派生的 Redis key，续租失败时取消 Runner context 并继续 drain Event Channel。PostgreSQL `run_token` 只保护 Execution、Event Journal 和 Audit 的状态更新。当前不实现 Session Provider 写入栅栏，因此 Lease 丢失后旧 Runner 仍可能完成最后一次 Event、State 或 Summary 写入；这个残余风险不影响新旧 Worker 对 Execution 终态的覆盖保护。严格旧写入拒绝仅在特定 Session Provider 具备原子条件写入时作为后续能力接入。

机制职责：`turn_seq` 保证消息顺序，Redis Session Lease 避免正常 Worker 并发执行，`run_token` 保护 Execution 所有权和状态，Dispatch Outbox 解决 PostgreSQL 到 Redis 的可靠投递。Job 和外部 Tool 副作用仍采用至少一次语义，非幂等 Tool 需要业务幂等键。

## 7. 租户隔离

配置隔离：

```text
tenant_id + app_id + config_version
```

一次 Job 在入队时绑定固定 `config_version`。执行过程中不切换配置，新配置只影响后续请求。

数据隔离：

- SQL 主键、唯一键和查询条件带 `tenant_id/app_id`。
- Redis key 使用租户和应用前缀。
- 对象存储路径带租户和应用前缀，但对象路径不作为权限依据。
- 向量检索必须带 `tenant_id/app_id` filter。

物理隔离采用分层混合模式：

- Tenant、Agent App、AppConfig、Credential、Job 和 Execution 默认使用共享 PostgreSQL、共享表，并显式携带 `tenant_id/app_id`。
- 默认 tRPC-Agent-Go PostgreSQL Session 与协调库共用 PostgreSQL 实例，但放在独立逻辑 Schema 中。框架物理表通过规范化 `app_name` 表达租户和应用作用域。
- `BackendConfig` 可以为高隔离租户选择独立 Schema 或独立数据库，上层 Worker 和 Runner 契约不变。
- 不默认每租户一个 Schema 或数据库，避免租户数量直接放大迁移、连接池和运维成本。
- 框架 Session 表依赖规范化 `app_name` 或独立 Schema 隔离；选择 PostgreSQL Provider 时，其 Schema 与表由 Provider 按实际 DSN 准备，平台 PostgreSQL migration 不创建或迁移这些表。

迁移边界分为两层：`trpcservice/postgres/migrations` 只负责 PostgreSQL 平台表的
Schema Migration，包括 DDL、约束和迁移版本校验；Redis → SQL、SQL → SQL、向量索引
重建和对象数据迁移属于包外的数据迁移编排。编排层在 App 的 `MIGRATING` 维护窗口内暂停
新请求、排空旧 Job、全量复制校验、切换配置或恢复旧配置；各后端只提供自己的读写与后端特有准备能力。

工具权限隔离：

```text
Runner 构建前：过滤模型可见工具
Tool 执行前：再次校验租户、应用、用户、通道和预算
```

日志脱敏：

日志、trace 和错误报告不记录 token、DSN、API key、完整 PII 和原始 Tool 参数。

密钥管理：

平台需要区分两类凭据：

- 入站 API Credential 用于外部业务系统调用平台。平台生成至少 32 个随机字节的高熵 API Key，原文只展示一次；数据库保存 `credential_id`、租户应用绑定、`key_digest`、可展示的 `key_prefix`、状态和过期时间，不保存原文，也不使用 `secret_ref` 找回原文。
- 模型 API Key、IM token/签名密钥、数据库凭据和 Tool/MCP 凭据属于运行时可取回 Secret。数据库和配置只保存带租户应用作用域的 `secret_ref`，Worker 通过 SecretProvider 从 Secret Manager 或 KMS 按需解析。

API Credential 的 SHA-256 digest 方案依赖平台生成的高熵随机 Key；不得把低熵用户密码直接套用该方案。

## 8. 配置发布与控制面

Admin API 是配置管理入口，负责管理：

```text
Tenant
Agent App
AppConfig Version
Channel Binding
Backend Config
Tool Policy
Audit Policy
API Credential
SecretRef
Role Binding
Backend Migration
```

配置发布生成新的 `config_version`，并通过切换 active version 生效。Gateway 入队时记录当时使用的 `config_version`，Worker 后续按这个版本执行。这样即使管理员发布了新版本，已经进入队列的任务也不会在执行中途切换模型、工具或后端。

Tenant 或 Agent App 进入 `SUSPENDED` 后，Gateway 立即拒绝受影响范围的新请求，Worker 不再 claim 该范围的新 Job。已排队 Job 保持 `PENDING`，恢复为 `ACTIVE` 后继续按原 `turn_seq` 执行；已经运行的 Job 允许完成，避免强制中断造成 Session 半写入或 Tool 副作用状态不确定。`MIGRATING` 只用于 App 的后端维护：Gateway 暂停新请求，但 Worker 继续 claim 并排空已接受 Job；完成或失败后恢复 `ACTIVE`。

删除必须先暂停租户并等待 Job 进入终态，再按保留策略异步清理，不提供同步硬删除。安全事件下立即终止运行任务属于后续治理与可靠性专题的紧急停用能力，不复用普通 `SUSPENDED`。

## 9. 当前 PostgreSQL 运行闭环

当前闭环验证多租户与节点部署的 PostgreSQL 路径：租户配置可以发布，可信请求可以入队，多个 Worker 可以依赖共享 Session 执行同一 Agent App。完整 IM、多后端、治理和运维设计见各自专题；是否实现由整体实施计划安排。

### 9.1 当前实现边界

- PostgreSQL 作为当前准入、任务协调和默认 Session 后端，保存租户配置、任务、执行状态和共享 Session。平台自有表固定放在 `platform` Schema，框架 Session 表使用独立逻辑 Schema。
- HTTP API key 作为首个可信入口；平台生成原始 Key，数据库只保存 digest，凭据绑定 `tenant_id/app_id`，请求 payload 不能指定或覆盖租户身份。
- Gateway 和 Worker 不依赖 sticky session。Gateway 只写入带 `tenant_id/app_id/session_principal_id/session_id` 分区键的任务，Worker 可以由任意节点 claim。
- Gateway 使用单个 PostgreSQL 准入事务复核身份和状态、固定 `config_version`、处理幂等、分配 `turn_seq` 并写入 Execution/Dispatch Outbox。
- 同一 Session 使用 `turn_seq`、Redis Session Lease 和 PostgreSQL `run_token` 管理顺序、实际执行与终态更新；当前没有 Session 写入栅栏。
- 复用 tRPC-Agent-Go `session/postgres`、LLMAgent 和 `runner.Runner`，不重新实现框架已有 Session 或 Runner。
- 按产品启用的协议入口复用 tRPC-Agent-Go `server/*`，统一注入 QueuedRunner；真实 Runner 只存在于 Worker；流式入口才启用 Execution Event Journal。
- 运行时可取回 Secret 只保存 `secret_ref`，运行时通过 SecretProvider 解析。
- InMemory 只允许测试和本地显式启用，生产角色启动时拒绝 InMemory Session。

### 9.2 实施顺序

1. 补充身份契约测试，修正 Worker 的 Runner 参数，验证单聊独立 Session 和群聊共享 Session。
2. 实现 HTTP API key 认证和 TenantResolver，校验 Tenant、Agent App 与凭据状态。
3. 增加 PostgreSQL migration 和仓储，覆盖 Tenant、Agent App、不可变 AppConfig Version、API Credential、Session Lane、Execution、Dispatch Outbox、Execution Event 和 Audit。
4. 已完成原子 Admission 事务：复核 Credential/Tenant/App、固定 `config_version`、处理幂等、分配 `turn_seq` 并写 Execution/Dispatch Outbox。Worker 只能从已提交的 Execution 恢复命令。
5. 已完成 PostgreSQL Execution 状态与 Dispatch Outbox、Redis Streams Relay、Consumer Group Pending 重领及 Redis Session Lease。
6. 已完成共享执行基础层：按 `tenant_id/app_id/config_version` 缓存装配 OpenAI-compatible Model、LLMAgent、PostgreSQL Session 和真实 Runner；`Worker.Run` 在 Runner 生命周期内持有 Redis Session Lease。模型密钥和 Session DSN 只由注入式解析器提供，不落配置或日志；`base_url` 还必须经过 Endpoint Policy。
7. 已完成 `run_token` 条件状态更新；当前明确不提供 Session Provider 的严格旧写入拒绝。
8. 为实际启用的 tRPC-Agent-Go `server/*` 实现 QueuedRunner；需要流式响应或断线恢复时增加持久化 Execution Event Journal。真实 Runner 仅由 Worker 持有。Verified Channel Binding 在完成绑定持久化、验签/去重、Admission 事务复核，以及 `channel`/`binding_id` 在 Job 中持久化和恢复之前保持未启用，不能作为公开入口。
9. 已完成：Runner 构建前按 `tool_policy` 过滤可见工具，Worker 在框架执行前再次校验执行权限；模型密钥由带作用域的 `APIKeyRef` 和 SecretProvider 解析。Worker 仅向日志和 trace 传递字段白名单，向 PostgreSQL 追加可靠 Audit，并在失去 Job 租约后拒绝追加。
10. 已完成最小 Admin API：创建 Tenant/Agent App、发布不可变配置版本和切换 active version；生成 API Credential 时仅持久化 SHA-256 digest 且原文仅返回一次，撤销与 Admission 锁定同一 Credential 行并且数据库拒绝重新激活。
11. 已完成 `cmd/trpc-service` 装配：显式选择 `gateway`、`worker` 或 `all` 角色；`/livez` 与依赖 PostgreSQL、Redis 的 `/readyz` 分离；Gateway 与 `all` 角色在同一 HTTP 服务启用 OpenAI-compatible `POST /v1/chat/completions`。入口使用 Bearer API Key 解析可信 tenant/app，再把请求头中的 request、幂等、session 和用户身份放入 QueuedRunner 上下文；请求体不能覆盖可信范围。收到终止信号后先变为 not-ready，Worker 停止 claim 新 Job 并在窗口内继续续租和排空已认领 Job。`worker`/`all` 需要稳定的 `TRPC_AGENT_SERVICE_WORKER_ID`。运行时 Secret 由按 tenant/app/ref 十六进制编码的 `TRPC_AGENT_SERVICE_SECRET_<tenant>_<app>_<name>_<version>` 环境变量注入；配置库只保留引用。
12. 已完成 PostgreSQL、一个 Gateway 和两个 Worker 的 Docker Compose 部署。Compose 在 Gateway 暴露健康端口和 OpenAI-compatible 聊天入口。真实 PostgreSQL 集成测试验证两个 Consumer 同时处理不同 Session 的 Job，并分别以独立 Worker Owner 完成。公共 API 第二遍设计审查确认本轮只有 `Consumer.StopClaiming` 新增导出，供进程层停止认领而保留已认领 Job 的排空职责；其余新增符号均保持包内。完整 Go 验证在交付前执行。

### 9.3 闭环验收

- 两个 Worker 可以交替处理同一 Session，历史连续且不依赖节点亲和。
- 同一 Session 的并发消息严格按 `turn_seq` 执行，不同 Session 可以并行。
- 旧 Worker 丢失 Redis Session Lease 后会被取消；新 Worker 获得同一 Lease 后可执行，PostgreSQL `run_token` 阻止旧 Worker 覆盖 Execution 终态。Session Provider 写入不具备严格 Fence 保证。
- 不同租户即使使用相同 `app_id/session_id`，配置、任务和 Session 也不能互相访问。
- Admission 与配置切换并发时只有一种提交顺序；Job 入队后切换 active config version，不影响已入队任务。
- 群聊中不同 `user_id` 共享同一 `session_principal_id` 对应的 Runner Session；runtime state 仍保留真实 `user_id`。
- 所有已启用的 HTTP/RPC 协议入口均经过 QueuedRunner 入队，不能绕过队列直接调用真实 Runner。
- API Credential 原文只展示一次且数据库不可恢复；运行时 Secret 只能通过有作用域的 `secret_ref` 解析。
- Worker 中断后，未完成 Job 在 lease 到期后可以重投。当前 PostgreSQL 闭环提供至少一次执行语义，不承诺非幂等 Tool 副作用恰好一次。
- `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`、`golangci-lint run --timeout=10m`、gofmt 和 goimports 检查通过。

### 9.4 当前未覆盖的能力

当前 PostgreSQL 闭环不覆盖企业微信或飞书 Adapter、Memory/Knowledge/Artifact 的多后端迁移、完整 Inbox/Reply Outbox、预算审批、审计平台、DLQ、容量评估和 Kubernetes 部署。这些能力的完整设计已在对应专题收敛，后续按整体实施计划实现。

### 9.5 跨专题依赖

以下问题不在本设计中单独定稿，后续完成对应专题后再回到本设计复核：

- 数据同步与多后端：维护窗口迁移、权威数据和派生数据边界已由对应专题定义。
- IM 渠道接入：外部用户、群和 thread 到 `user_id/session_principal_id/session_id` 的稳定映射已由对应专题定义。
- 治理、可观测性与安全：紧急停用、租户配额、审计和敏感信息处理已由对应专题定义。
- 可靠性与运维：请求与 Tool 副作用幂等、Outbox、连接池容量、重试和优雅停机已由对应专题定义。
