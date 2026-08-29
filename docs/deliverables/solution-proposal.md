# 多租户节点化 Agent 平台建设方案

## 1. 项目定位与建设目标

项目以 8 月 21 日的 `main` 分支为代码基线，以 tRPC-Agent-Go 为 Agent 运行时基础，建设面向多个企业、团队和业务线的节点化 Agent 平台。平台不重新实现 Agent 编排、模型调用、Session、Memory、Knowledge、Artifact、Tool、Guardrail、Plugin 或 Telemetry，而是在这些框架能力之上补齐多租户隔离、可信接入、跨节点任务协调、后端选择、IM 接入和运营治理。

目标是将单个 Agent 进程演进为可管理的平台服务。租户能够独立创建 Agent App，配置模型、工具权限、知识库、审计策略、IM 通道和数据后端；Worker 可按负载横向扩展；同一会话保持处理顺序，不同会话并行执行；节点或外部依赖发生短暂故障后，任务和回复可恢复；所有关键行为可按租户、应用、用户和请求追踪。

本方案以完整需求为目标架构，不以当前代码完成度划分功能边界。阶段实施按依赖关系推进，但最终交付的架构、数据模型和验收标准保持一致。

## 2. 设计原则与关键不变量

平台的设计围绕以下四项不变量展开。

1. **身份先于执行。** Tenant/App 只能来自认证 claims、平台生成的 API Credential 或已验签的 Channel Binding。HTTP payload、IM 消息体和运行时参数不能声明或覆盖租户、应用、真实用户或会话主体。
2. **配置快照先于副作用。** Gateway 受理请求时固定 `config_version`。Worker 只按该版本装配模型、工具、策略和后端，不在执行中读取“最新配置”；发布、灰度和回滚只影响后续准入任务。
3. **共享状态是真相。** Session、Memory、任务状态、幂等记录、Outbox 和租约保存在共享后端；Worker 本地只保存当前执行上下文和可重建缓存，不依赖 sticky session。
4. **可靠提交点先于外部确认。** IM 回调在 Inbox、Execution 和 Dispatch Outbox 提交后确认；模型执行完成后，Execution 终态、审计和 Reply Outbox 在同一协调事务中提交；跨后端发送通过 Outbox 重试，而不是依赖进程内 channel。

`user_id` 表示真实发言人，`session_principal_id` 表示会话主体。单聊中二者通常相同；群聊或话题中前者仍是发言人，后者是群或 thread。由此可以让群上下文连续，同时把工具权限、审计、个人 Memory 和成本归属到真实用户。

## 3. 总体架构

```text
企业微信 / 飞书 -> Channel Adapter ----------------> Agent Gateway
HTTP / RPC -> server/* + Auth + QueuedRunner ------> Agent Gateway

Agent Gateway -> PostgreSQL Execution + Dispatch Outbox -> Redis Streams
Redis Streams -> Agent Worker -> tRPC-Agent-Go Runner
Agent Worker -> Session / Memory / Knowledge / Artifact Provider
Agent Worker -> Platform SQL Audit Store / Reply Outbox -> Channel Adapter

Admin API -> Tenant / App / Config / Credential / Binding / Policy
Secret Manager / KMS -> Worker 按作用域解析运行密钥
各组件 -> Telemetry Collector
```

系统由控制面、接入与数据面、运行时、存储适配和观测五个层次组成。控制面管理“允许什么”；数据面处理“当前执行什么”；二者通过不可变配置版本和不含明文密钥的 Execution 记录连接。

| 组件 | 核心职责 | 关键边界 |
| --- | --- | --- |
| Admin API | 管理 Tenant、Agent App、配置版本、Credential、Binding、策略和迁移 | 不调用 Runner；所有管理变更审计化 |
| Channel Adapter | 企业微信、飞书协议验签、解密、标准化、ACK 和回复 | 不自行选择租户，不直接调用 Runner |
| Auth / QueuedRunner | 建立可信 HTTP/RPC 身份并把协议请求转为入队命令 | payload 不可覆盖可信身份 |
| Agent Gateway | 原子准入、幂等、配置快照、Session 顺序分配 | 只创建可恢复 Execution，不绑定具体 Worker |
| PostgreSQL | 控制面、Inbox、Execution、Outbox、Audit、默认 PostgreSQL Session | 事务是准入与终态的权威提交点 |
| Redis | Dispatch Stream、Consumer Group Pending 重领、Session Lease、限流缓存 | 不保存平台控制面真相 |
| Agent Worker | 条件认领任务、装配租户运行时、调用真实 Runner | 无状态，不自行改写配置版本 |
| Storage Adapter | 选择带租户作用域的 Session、Memory、Knowledge、Artifact Provider | Provider 访问必须携带 scope，不能直接使用物理 DSN |
| Secret Manager/KMS | 按 Tenant/App/用途解析运行密钥 | 配置和日志仅保存 `secret_ref` |
| Telemetry 与 Audit | 汇聚指标、Trace、脱敏日志；可靠审计独立持久化 | Telemetry 失败不阻塞执行，Audit 不依赖 Telemetry |

最小部署为一个 `all` 角色服务、一个 PostgreSQL 实例和一个 Redis 实例。生产环境将 Channel Adapter、协议入口/Gateway、Worker 和 Admin API 独立部署，按入站流量、队列积压和管理负载分别扩缩容。

## 4. 核心请求生命周期

一次消息从入口到回复按以下步骤处理。

1. **可信接入。** Channel Adapter 先定位候选 Binding，再按通道协议验签、解密和校验外部账号；HTTP/RPC 由 Auth 验证 Credential、claims 或内部调用身份。只有验证成功后才生成 Tenant/App、`user_id` 和 `session_principal_id`。
2. **原子准入。** Gateway 在一个 PostgreSQL 短事务中锁定并复核 Credential 或 Binding、Tenant 和 Agent App，读取 active config，校验幂等键，锁定 `session_lane` 分配 `turn_seq`，同时写 Inbox（IM）、Execution 和 Dispatch Outbox。事务提交是平台接受请求的线性化点。
3. **可靠投递。** Relay 认领 Dispatch Outbox 并发布到 Redis Streams。Worker 通过 Consumer Group 接收消息；Relay 失败可重试，Worker 崩溃后 Redis Pending 与过期 Execution 租约允许其他节点恢复。
4. **有序执行。** Worker 只能认领同一 Session 最小的未完成 `turn_seq`，取得 Redis Session Lease 后按 `tenant_id/app_id/config_version` 装配 tRPC-Agent-Go Runner。Session 的分区键为 `tenant_id + app_id + session_principal_id + session_id`，不同分区可并行。
5. **状态与回复。** Runner 将 Event、State、Summary 写入租户选定的共享 Session Provider，Worker 一直消费 Event Channel 到关闭。完成后以 `run_token`、租约和状态条件提交 Execution 终态、Audit 与 Reply Outbox，再对 Redis 条目 `XACK`。Reply Sender 只重试回复，不重跑已完成的 Agent。

`turn_seq` 负责顺序，Redis Session Lease 避免正常情况下两个 Worker 同时运行，PostgreSQL `run_token` 防止失效 Worker 覆盖新的 Execution 终态。本方案不启用严格 Session 写入 Fence：Lease 失效时旧 Runner 的最后一次 Session 迟到写入可能发生。这是明确记录的限制；涉及非幂等 Tool 的业务必须使用自身幂等键，未知结果不自动重试。

## 5. 重点技术方案

### 5.1 多租户隔离与配置治理

Tenant 是最高资源和权限边界，Tenant 下可创建多个 Agent App。每个 App 的 `AppConfig Version` 不可变，包含模型、工具策略、后端配置、审计策略和 `secret_ref`。Channel Binding 独立于配置版本，固定关联一个 Tenant/App，负责外部账号、凭据、启停和路由。

默认租户使用共享后端与应用层作用域约束：平台表主键、唯一键、外键和查询条件都带 `tenant_id/app_id`；Redis key、对象路径和向量过滤带相同前缀；个人 Memory 额外带 `user_id`。高隔离租户可以选择独立 Schema、数据库或专属后端实例。平台不启用 RLS，隔离验证通过作用域测试、引用校验和审计查询完成。

模型、工具和审计策略支持基于稳定 Session 分区键的确定性灰度。相同规则和比例下，同一 Session 始终命中同一候选或稳定版本；扩大比例或回滚只影响后续准入任务，已经受理的 Execution 保持原 `config_version`。

### 5.2 多后端与数据迁移

平台不设计覆盖所有后端的通用 CRUD。Session、Event、State、Summary 是 Provider 管理的逻辑记录，具体表或 key 由 tRPC-Agent-Go 的 PostgreSQL、Redis、MySQL 等 Session Provider 决定；平台 PostgreSQL 只保存 Session lane、Execution、Inbox/Outbox、配置和 Audit 等协调状态。

Memory 的权威记录写入共享 Memory Store 后对全部 Worker 可见；向量库只承担 Knowledge 和可选语义 Memory 的派生检索，允许索引延迟。Knowledge 原文、附件和 Artifact 保存于对象存储，SQL metadata 承担权限和生命周期控制。

后端切换通过 `data_migration` 维护窗口处理：记录按 `PENDING -> DRAINING -> COPYING -> VERIFYING -> SUCCEEDED` 推进，Gateway 在 `DRAINING` 起停止接收该 App 的新业务请求，Worker 排空已接受 Job，确认没有 Session 写入者后全量复制并校验记录数、稳定 ID、Event 顺序和校验和，再原子切换配置。HTTP/RPC 返回带 `Retry-After` 的可重试错误；IM 验签后 ACK 并提示维护，不延后执行迁移窗口内的新输入。失败时标记 `FAILED`，旧后端保持 active。

### 5.3 IM 通道接入

企业微信和飞书通过统一 Channel Adapter 接入。企业微信 Adapter 处理 URL 校验、回调签名、密文解密和短时响应；飞书 Adapter 处理 `url_verification`、事件订阅校验和消息 Open API 回复。外部协议差异仅留在 Adapter 内，Gateway、Worker 和 Session 不感知通道格式。

IM 入站以 `tenant_id + app_id + binding_id + external_message_id` 去重。单聊映射为“用户主体 + 默认或业务 Session”，群聊映射为“群主体 + 默认或业务 Session”，话题映射为“thread 主体 + 默认或业务 Session”。回复按 `request_id + part_no` 去重，超长消息分片，失败经 Reply Outbox 退避重试和死信处理。

### 5.4 治理、审计与可观测性

治理分两层执行：构建 Runner 前过滤模型可见 Tool；真实调用前再次按 Tenant、App、用户、通道、预算、参数、密钥和风险等级校验。危险 Tool 进入持久化审批，审批保存精确参数和摘要；Tool Executor 以条件状态和 `approval_id` 保证只执行一次。非幂等 Tool 的网络结果未知时标记 `UNKNOWN`，等待人工处置。

`trace_id` 贯穿 IM/HTTP 入口、Gateway、Worker、Runner、Tool、Session、Memory 和 Reply Outbox。指标覆盖请求量、错误率、p95/p99 延迟、队列积压、模型/Tool/存储耗时、IM 投递成功率、Token 消耗和租户成本。Audit 是平台 SQL 中的追加型权威记录，至少记录 Tenant、通道、用户、Session、Agent、Tool、决策、延迟、错误类型、成本和 Trace；日志、Trace 和错误报告不记录 Token、DSN、API Key、完整 PII 或原始 Tool 参数。

## 6. 运行、恢复与容量

节点故障后，Redis Consumer Group 重领 Pending 条目，PostgreSQL 依据过期租约恢复 Execution 并重新写 Dispatch Outbox。Gateway 或 PostgreSQL 暂时不可用时，IM 不 ACK 以请求通道重投，HTTP/RPC 返回可重试错误；模型超时、429 或 5xx 按限制次数退避重试；IM 回复失败只重试 Reply Outbox。进程停止时先变为 not-ready、停止接收和 claim 新任务，已运行 Job 在窗口内续租；超时后取消 Context，但仍排空 Runner Event Channel 并回收子 goroutine。

容量通过压测确定。Worker 并发初值不低于“峰值已接收消息速率 × p95 Agent 执行时长”，再根据模型配额、Tool 耗时和 Session Lease 等待设置余量。持续观测 Redis Pending、Lease 等待和 QPS、PostgreSQL QPS 与连接池、模型 Token 速率、对象/向量吞吐和 IM 回调峰值。生产环境以这些指标驱动 Gateway、Worker 和后端实例的独立扩缩容。

## 7. 预期效果与验收口径

- **安全隔离：** 不同租户即使使用相同 App、外部用户或 Session 标识，也不能互相访问配置、任务、Session、Memory、知识库、工具权限、Audit 或密钥。
- **执行一致性：** 同一 Session 的并发输入严格按 `turn_seq` 执行，不同 Session 可由不同 Worker 并行处理；Execution 终态不会被失效 Worker 覆盖。
- **可靠交付：** IM 重复回调、HTTP/RPC 重试和 Worker 故障不会生成第二个业务 Execution；任务发布、消费和回复都有可恢复记录与幂等键。
- **可演进性：** 模型、工具、审计策略可版本化发布、灰度和回滚；Session、Memory、Knowledge、Artifact 后端可按 App 路由，并通过维护窗口迁移。
- **可运营性：** 入口、执行、存储、Tool、回复可由同一个 `trace_id` 关联；可靠 Audit 独立保存；容量、积压、错误率和租户成本可监控告警。

验收覆盖企业微信或飞书的完整消息链路、跨 Worker Session 连续性、不同 Session 并行、Tenant/App 隔离、配置版本固定、Outbox 与 Pending 恢复、迁移维护窗口、审计脱敏与优雅停机。

## 8. 时间规划（8 月 21 日至 9 月 11 日）

| 时间 | 工作重点 | 阶段产出与验收 |
| --- | --- | --- |
| 8 月 21 日 - 8 月 23 日 | 阅读 `main` 基线与 tRPC-Agent-Go，梳理可复用能力，完成领域边界和总体设计 | 租户模型、组件职责、数据模型和架构图确定 |
| 8 月 24 日 - 8 月 28 日 | 建立 Tenant/App/配置版本/Credential、可信身份解析和原子准入 | Tenant/App 不可由 payload 冒充；配置版本在准入时固定 |
| 8 月 29 日 - 9 月 2 日 | 建立 Execution、Dispatch Outbox、Redis Streams、Session Lease 和 Worker 恢复 | 同一 Session 有序、不同 Session 并行；节点故障可恢复 |
| 9 月 3 日 - 9 月 6 日 | 建立 Storage Adapter、作用域包装、幂等、Event/State/Summary 顺序与迁移编排 | 多后端路由、Memory 可见性和 `data_migration` 流程可验证 |
| 9 月 7 日 - 9 月 9 日 | 完成企业微信/飞书 Adapter、Tool 治理、Audit、Telemetry、灰度和容量设计 | 完整 IM 闭环、审计脱敏、关键指标与风险策略明确 |
| 9 月 10 日 - 9 月 11 日 | Docker 部署、故障演练、文档整理和验收 | 最小部署可运行，交付架构、时序、数据模型、同步、风险和方案文档 |

实施遵循“先可信准入和共享状态，再可靠投递和运行时，再通道、治理和运维”的依赖顺序。每一阶段保留前一阶段的不变量，不以局部演示替代最终租户隔离、恢复和验收要求。
