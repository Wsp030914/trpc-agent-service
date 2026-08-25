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
channel_binding_ids
backend_config
audit_policy
secret_refs
```

`channel_binding_ids` 表示当前 App 版本启用哪些 IM 通道绑定。完整通道配置放在 Channel Binding 中，绑定粒度是：

```text
tenant_id + app_id + binding_id
```

也就是一个 IM 外部账号绑定到某个租户下的某个 Agent App。这样 Gateway 收到来自已验签通道的消息时，可以直接定位到正确的 `tenant_id` 和 `app_id`。

## 2. 节点部署拓扑

```text
Channel Adapter / HTTP-RPC Entry
-> Agent Gateway
-> PostgreSQL SQL Job Queue
-> Agent Worker
-> Storage Adapter
-> runner.Runner
-> Reply Outbox
```

| 组件 | 职责 |
| --- | --- |
| Channel Adapter | 处理企业微信、微信客服、Telegram 等 IM 平台的验签、去重、协议解析、ACK 和回复适配 |
| Agent Gateway | 认证、租户解析、应用解析、配置版本选择、请求幂等、附件检查、生成 Job 和 `partition_key` |
| PostgreSQL SQL Job Queue | 接收 Gateway 生成的任务，通过顺序号、租约和 Session 锁协调多 Worker 执行 |
| Agent Worker | 消费 Job，加载配置，装配模型、工具策略和数据后端，调用 Runner 执行 Agent |
| Storage Adapter | 根据租户 `backend_config` 选择并装配 Session、Memory、Knowledge、Artifact、Audit 后端 |
| Admin API | 管理 Tenant、Agent App、AppConfig Version、Channel Binding、Backend Config、Tool Policy、Audit Policy 和 SecretRef |
| Telemetry Collector | 汇聚 Channel Adapter、Gateway、Worker、Storage Adapter、Admin API 上报的 trace、metrics 和日志 |

Gateway 不直接调用某台 Worker，而是把任务写入 PostgreSQL SQL Job Queue。这样 IM webhook 可以快速 ACK，Worker 故障后任务也可以重新消费，Gateway 不需要维护 Worker 注册表。首期不引入 Redis、Kafka 等独立消息队列；队列接口保持可替换，达到明确容量瓶颈后再增加其他实现。

## 3. 请求入口与租户解析

IM 请求先进 Channel Adapter。Channel Adapter 完成验签、去重、协议解析和 ACK 后，将 IM 原始消息转换为平台标准输入，并根据已验签的 Channel Binding 得到可信的租户来源。

HTTP/RPC 请求走协议入口，例如 OpenAI-compatible、A2A 或内部 RPC。该入口负责解析认证信息，请求进入 Gateway 时，`tenant_id/app_id` 必须来自认证 claims、API key 或内部 RPC 身份。

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

## 4. Agent Worker 与无状态执行

Agent Worker 从 PostgreSQL SQL Job Queue 消费 Job。执行前，Worker 按以下字段加载当前任务绑定的配置：

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
Audit backend
```

Worker 调用 `runner.Runner` 执行 Agent；需要取消和状态管理时使用 `runner.ManagedRunner`。Runner 返回 Event Channel 后，Worker 必须持续消费到 channel 关闭，再写入执行状态和 Reply Outbox。

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

会话历史、事件、状态和长期记忆都从共享后端读取和写入。同一 Session 的执行顺序由 PostgreSQL SQL Job Queue 保证，避免多个 Worker 同时修改同一段会话。不同 Session 可以并行执行。

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

### 6.1 顺序、租约与 Session 锁

PostgreSQL 为每个 `partition_key` 维护递增的 `turn_seq`。入队事务锁定对应 Session lane 并分配序号；Worker 只能 claim 当前最小的未完成序号。

claim 使用短事务写入 `lease_owner`、`lease_until`、`lease_epoch` 和 `attempt`，执行期间由 Worker 续租。完成、续租和重投操作都必须携带当前 `lease_epoch`，旧 Worker 不能更新新租约拥有者的 Job 状态。

租约只能表达 Job 所有权，不能阻止租约失效的旧 Runner 继续写 Session。因此 Worker 在执行 Runner 期间还要持有由 `partition_key` 派生的 PostgreSQL connection-level advisory lock：

- 不开启覆盖整个 Runner 生命周期的数据库事务。
- 新 Worker 只有在旧租约过期且成功取得 advisory lock 后才能执行。
- 锁连接断开时立即取消 Runner context。
- Runner 结束后使用当前 `lease_epoch` 条件更新 Job 终态，再释放锁。

该模型保证正常节点遵循同一权威协调点。Job 和外部 Tool 副作用仍采用至少一次语义；请求幂等和副作用去重由后续数据一致性与可靠性设计补全。

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
- 首期 tRPC-Agent-Go PostgreSQL Session 与协调库共用 PostgreSQL 实例，但放在独立逻辑 Schema 中。框架物理表通过规范化 `app_name` 表达租户和应用作用域。
- `BackendConfig` 可以为高隔离租户选择独立 Schema 或独立数据库，上层 Worker 和 Runner 契约不变。
- 不默认每租户一个 Schema 或数据库，避免租户数量直接放大迁移、连接池和运维成本。
- PostgreSQL RLS 是平台自有共享表的生产防御措施，不是唯一隔离机制；框架 Session 表依赖规范化 `app_name` 或独立 Schema 隔离。

工具权限隔离：

```text
Runner 构建前：过滤模型可见工具
Tool 执行前：再次校验租户、应用、用户、通道和预算
```

日志脱敏：

日志、trace 和错误报告不记录 token、DSN、API key、完整 PII 和原始 Tool 参数。

密钥管理：

数据库只保存 `secret_ref`。真实密钥由 Secret Manager 或 KMS 管理，Worker 根据当前租户和运行时权限按需获取。

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
SecretRef
```

配置发布生成新的 `config_version`，并通过切换 active version 生效。Gateway 入队时记录当时使用的 `config_version`，Worker 后续按这个版本执行。这样即使管理员发布了新版本，已经进入队列的任务也不会在执行中途切换模型、工具或后端。

Tenant 进入 `SUSPENDED` 后，Gateway 立即拒绝新请求，Worker 不再 claim 该租户的新 Job。已排队 Job 保持 `PENDING`，恢复为 `ACTIVE` 后继续按原 `turn_seq` 执行；已经运行的 Job 允许完成，避免强制中断造成 Session 半写入或 Tool 副作用状态不确定。

删除必须先暂停租户并等待 Job 进入终态，再按保留策略异步清理，不提供同步硬删除。安全事件下立即终止运行任务属于后续治理与可靠性专题的紧急停用能力，不复用普通 `SUSPENDED`。

## 9. 第一阶段运行闭环实施计划

第一阶段只闭环“多租户与节点部署”需求：租户配置可以发布，可信请求可以入队，多个 Worker 可以依赖共享 Session 执行同一 Agent App。具体 IM 协议、多后端同步和完整治理运维能力留给后续阶段。

### 9.1 技术边界

- PostgreSQL 作为第一阶段唯一权威协调点，保存租户配置、任务、执行状态和共享 Session。平台表与框架 Session 表使用独立逻辑 Schema。
- HTTP API key 作为首个可信入口；凭据绑定 `tenant_id/app_id`，请求 payload 不能指定或覆盖租户身份。
- Gateway 和 Worker 不依赖 sticky session。Gateway 只写入带 `tenant_id/app_id/session_principal_id/session_id` 分区键的任务，Worker 可以由任意节点 claim。
- 同一 Session 使用 SQL Job Queue 的 `turn_seq + claim/lease_epoch` 管理顺序和所有权，并用 PostgreSQL connection-level advisory lock 防止新旧 Worker 同时执行。
- 复用 tRPC-Agent-Go `session/postgres`、LLMAgent 和 `runner.Runner`，不重新实现框架已有 Session 或 Runner。
- 数据库和配置只保存 `secret_ref`；运行时通过 SecretProvider 解析真实凭据。
- InMemory 只允许测试和本地显式启用，生产角色启动时拒绝 InMemory Session。

### 9.2 实施顺序

1. 补充身份契约测试，修正 Worker 的 Runner 参数，验证单聊独立 Session 和群聊共享 Session。
2. 实现 HTTP API key 认证和 TenantResolver，校验 Tenant、Agent App 与凭据状态。
3. 收紧 Gateway 路由契约：入队时固化 `config_version`、`request_id`、`trace_id` 和包含 `session_principal_id` 的 `partition_key`，运行路径强制使用 RoutedEnqueuer；Runner 使用规范化 `app_name`。
4. 增加 PostgreSQL migration 和仓储，覆盖 Tenant、Agent App、不可变 AppConfig Version、API Credential、Execution 和 Session Job。
5. 实现 PostgreSQL Session Job Queue，事务分配 `turn_seq`，支持 claim、`lease_epoch`、续租、完成和租约到期重投。
6. 接入共享执行层，按 `tenant_id/app_id/config_version` 装配 OpenAI-compatible Model、LLMAgent、PostgreSQL Session 和 Runner；执行期间持有 Session advisory lock。
7. 把工具可见性与执行权限、secret ref、日志字段白名单和 trace 传播接入实际执行链路。
8. 实现最小 Admin API，支持创建 Tenant/Agent App、发布不可变配置版本和切换 active version。
9. 实现数据面 API：提交消息返回 `request_id`，查询 execution 状态和最终回复。
10. 完成 `cmd/trpc-service` 装配，支持 `gateway`、`worker`、`all` 角色、健康检查和优雅停机。
11. 提供 PostgreSQL、一个 Gateway 和两个 Worker 的 Docker Compose 部署与集成测试。
12. 完成公共 API 第二遍设计审查和完整 Go 验证。

### 9.3 闭环验收

- 两个 Worker 可以交替处理同一 Session，历史连续且不依赖节点亲和。
- 同一 Session 的并发消息严格按 `turn_seq` 执行，不同 Session 可以并行。
- 旧 Worker 租约过期但尚未退出时，新 Worker 不能在取得同一 Session advisory lock 前执行。
- 不同租户即使使用相同 `app_id/session_id`，配置、任务和 Session 也不能互相访问。
- Job 入队后切换 active config version，不影响已入队任务。
- 群聊中不同 `user_id` 共享同一 `session_principal_id` 对应的 Runner Session；runtime state 仍保留真实 `user_id`。
- Worker 中断后，未完成 Job 在 lease 到期后可以重投。本阶段提供至少一次执行语义，不承诺非幂等 Tool 副作用恰好一次。
- `go test ./...`、`go test -race ./...`、`go build ./...`、`go vet ./...`、`golangci-lint run --timeout=10m`、gofmt 和 goimports 检查通过。

### 9.4 非本阶段范围

第一阶段不完整实现企业微信、微信客服或 Telegram Adapter，不实现 Memory、Knowledge、Artifact 的多后端同步与迁移，不实现完整 Inbox/Reply Outbox、预算审批、审计平台、DLQ、容量评估和 Kubernetes 部署。这些能力只保留可继续扩展的边界。

### 9.5 跨专题依赖

以下问题不在本设计中单独定稿，后续完成对应专题后再回到本设计复核：

- 数据同步与多后端：共享后端迁移到租户专属 Schema 或数据库、权威数据和派生数据边界。
- IM 渠道接入：外部用户、群和 thread 到 `user_id/session_principal_id/session_id` 的稳定映射。
- 治理、可观测性与安全：紧急停用、租户配额、审计和敏感信息处理。
- 可靠性与运维：请求与 Tool 副作用幂等、Outbox、连接池容量、重试和优雅停机。
