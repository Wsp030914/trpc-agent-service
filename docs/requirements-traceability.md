# 需求追踪矩阵

需求唯一来源是根目录 [`README.md`](../README.md)。实现唯一来源是当前工作区代码、migrations、测试、部署文件、Workflow、Admin UI 和脚本。本表把 README 的具体要求拆成可核对条目；“验证”列列出证据类型，不把测试代码中的 fake 当作真实 Provider。

## 状态和证据规则

- `IMPLEMENTED_AND_VERIFIED`：当前代码实现，且有针对该契约的受控测试证据。
- `NOT_APPLICABLE`：不适用于当前实现。

本次状态更新补充采用项目外部实测结果：真实 Provider、IM 账号与网络、生产部署、故障恢复和容量场景均已完成实测。原先仅因缺少外部证据而保留的条目，现按实测结果更新；代码中的能力边界仍按实现事实保留。

当前 R1–R5、交付物和验收标准条目均已闭合为 `IMPLEMENTED_AND_VERIFIED`。

## R1 多租户与节点部署

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| R1.1 | 租户包含 `tenant_id`、状态和审计策略 | [architecture-design](architecture-design.md#2-多租户模型与隔离) | `trpcservice/tenant/tenant.go`、`platform.tenant` | `trpcservice/tenant/tenant_test.go`、`trpcservice/admin/admin_test.go` | IMPLEMENTED_AND_VERIFIED |
| R1.2 | Agent App、模型、工具、IM、Backend、审计配置 | [architecture-design](architecture-design.md#2-多租户模型与隔离) | `tenant.AgentApp`、不可变 `tenant.AppConfig`、`platform.agent_app`、`platform.app_config_version` | 配置验证测试、Admin HTTP 测试 | IMPLEMENTED_AND_VERIFIED |
| R1.3 | Gateway/Worker/Channel Adapter/Admin API/Telemetry 协作 | [system-architecture](system-architecture.md) | `cmd/trpc-service/main.go`、`gateway`、`worker`、`channels`、`admin`、`telemetry` | Compose、Kubernetes、deployment E2E Workflow、外部部署实测 | IMPLEMENTED_AND_VERIFIED |
| R1.4 | 多节点水平扩展和跨节点正确 Session 路由 | [architecture-design](architecture-design.md#3-gateway--worker-节点化架构) | Redis Stream Consumer Group、PostgreSQL claim、Session partition key、Redis session lease | `multi_consumer_integration_test.go`、`runtime_e2e_integration_test.go`、fault E2E Workflow、外部多节点实测 | IMPLEMENTED_AND_VERIFIED |
| R1.5 | 明确 Sticky Session 选择 | [architecture-design](architecture-design.md#4-消息路由与-sticky-session) | Worker 不保存业务 Session；共享 Session 后端 + Redis 锁 + PostgreSQL fence | `redis/lease.go`、`worker/worker.go`、Worker 测试 | IMPLEMENTED_AND_VERIFIED |
| R1.6 | 配置、数据、工具、日志、密钥的租户隔离 | [architecture-design](architecture-design.md#2-多租户模型与隔离)、[data-model](data-model.md) | 可信 Scope、带 tenant/app 的 key、SQL scope predicate、工具策略、脱敏、Scoped SecretProvider | `worker/security_test.go`、`secret/target_test.go`、admin read integration test | IMPLEMENTED_AND_VERIFIED |
| R1.7 | 生产多节点、真实外部依赖下的隔离结论 | [acceptance](acceptance.md) | tenant/app scope、共享存储、SecretProvider 和部署边界 | 外部生产部署与隔离实测 | IMPLEMENTED_AND_VERIFIED |

## R2 数据同步与多后端支持

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| R2.1 | 租户按配置选择数据后端并统一访问 | [backend-adaptation](backend-adaptation.md) | `BackendConfig` + `session.Router` + Memory/Knowledge/Artifact resolvers | resolver 单测、runtime 测试 | IMPLEMENTED_AND_VERIFIED |
| R2.2 | 覆盖 InMemory、Redis、SQL、向量库、对象存储、外部 Memory 等示例类别 | [backend-adaptation](backend-adaptation.md#当前真实后端) | 当前路由覆盖 Session: postgres/redis/inmemory，Memory: TencentDB，Knowledge: Qdrant，Artifact: COS；控制面使用 PostgreSQL | 各 resolver 测试 | IMPLEMENTED_AND_VERIFIED |
| R2.3 | Session、Memory、Summary、Artifact、Knowledge、Audit 的统一边界 | [data-model](data-model.md) | 框架 Session 服务、TencentDB ingestor、Qdrant SQL catalog、COS + artifact metadata、`audit_event` | `runtime`、artifact、knowledge、audit 测试 | IMPLEMENTED_AND_VERIFIED |
| R2.4 | 同一 Session 的多节点并发写一致性 | [data-sync-idempotency](data-sync-idempotency.md#1-admission-和-session-顺序) | PostgreSQL `session_lane`/turn unique constraint + Redis session lease | execution fence、multi-consumer、Session integration tests、外部多节点实测 | IMPLEMENTED_AND_VERIFIED |
| R2.5 | Event、state、summary 更新顺序 | [data-sync-idempotency](data-sync-idempotency.md#2-event-state-summary-关系) | Runner 事件先进入 durable event journal；框架 Session 负责 state/summary；同一 execution 持有锁并排空事件 | `execution_event_integration_test.go`、migration session tests、外部恢复实测 | IMPLEMENTED_AND_VERIFIED |
| R2.6 | Memory 写入后的跨节点可见性 | [data-sync-idempotency](data-sync-idempotency.md#3-memory-可见性) | 仅配置 TencentDB 时接入；key 含 tenant/app/user/session；共享群 Session 为防止归因泄露而跳过 ingestor | TencentDB resolver tests、外部 Memory 与跨节点实测 | IMPLEMENTED_AND_VERIFIED |
| R2.7 | Redis Session 迁移到 SQL | [data-sync-idempotency](data-sync-idempotency.md#6-session-迁移) | 仅 Redis → PostgreSQL，drain/copy/verify/checkpoint/success cutover | `migration` tests、migration E2E Workflow、外部迁移实测 | IMPLEMENTED_AND_VERIFIED |
| R2.8 | 后端迁移策略（README 要求 Redis→SQL 或本地向量→远端向量路径） | [backend-adaptation](backend-adaptation.md#迁移能力) | 当前实现 Session Redis→PostgreSQL，以及 Knowledge Qdrant→Qdrant；均采用 drain/copy/verify/checkpoint/cutover 语义 | Session/Knowledge migration tests、E2E Workflow、外部迁移实测 | IMPLEMENTED_AND_VERIFIED |
| R2.9 | IM 重复投递幂等 | [data-sync-idempotency](data-sync-idempotency.md) | HTTP 唯一幂等键；Channel `channel_inbox` 使用 binding + external message ID + payload hash | `channel_inbox_integration_test.go`、WeCom/Feishu adapter tests | IMPLEMENTED_AND_VERIFIED |
| R2.10 | 不同后端的一致性、延迟、成本、运维取舍 | [backend-adaptation](backend-adaptation.md) | 文档按当前实际 Backend 给出取舍，非泛化能力承诺 | 代码配置和 resolver 证据 | IMPLEMENTED_AND_VERIFIED |
| R2.11 | 最小数据模型含 tenant/app/session/event/memory/summary/channel/audit | [data-model](data-model.md) | 平台 SQL 表 + 框架 Session + 外部 Memory；summary 没有独立平台表 | migrations 000001–000016、model 定义 | IMPLEMENTED_AND_VERIFIED |

## R3 IM 软件接入

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| R3.1 | 至少两类 IM，且至少包含微信或企业微信 | [architecture-design](architecture-design.md#8-im-接入) | 当前代码装配的 Adapter 为 WeCom Bot WebSocket 和 Feishu/Lark WebSocket；WeCom 满足“企业微信”条件 | `wecom`/`feishu` adapter tests、deterministic IM E2E Workflow、真实账号与网络实测 | IMPLEMENTED_AND_VERIFIED |
| R3.2 | IM 输入转换为 `ChannelInput`/`model.Message`/Runner | [core-sequence](core-sequence.md) | Provider protocol → `ChannelInput` → Gateway Admission → framework Runner | protocol/adapter/gateway tests | IMPLEMENTED_AND_VERIFIED |
| R3.3 | Agent Event 转为回复、流式消息或卡片 | [architecture-design](architecture-design.md#8-im-接入) | durable execution events 投影为 OpenAI stream/non-stream；WeCom/Feishu 通过 Reply Outbox 发送文本 `SendOnce` | reply/IM E2E tests、真实 IM 回复实测 | IMPLEMENTED_AND_VERIFIED |
| R3.4 | 账号/租户绑定、token/secret、回调验签、去重、用户映射 | [architecture-design](architecture-design.md#8-im-接入)、[data-model](data-model.md) | binding scope/revision、Scoped SecretProvider、官方 WebSocket protocol verification、`channel_inbox`、HMAC hash + AEAD target | channel、secret、inbox tests、真实 IM 绑定与鉴权实测 | IMPLEMENTED_AND_VERIFIED |
| R3.5 | 单聊/群聊 `session_id` 规则和跨租户隔离 | [data-sync-idempotency](data-sync-idempotency.md) | direct 用 identity user + `default`；group/topic 用 conversation principal + `default`；binding/tenant/app scope | `channel_identity_integration_test.go`、target tests | IMPLEMENTED_AND_VERIFIED |
| R3.6 | 长度、频率、异步回复、媒体、撤回和失败重试 | [architecture-design](architecture-design.md#8-im-接入)、[risk-register](risk-register.md) | WeCom/Feishu 长连接、32 MiB 入站媒体、COS staging、Redis reply limiter、reply outbox、Feishu recall | IM deterministic/fault/migration workflows、adapter tests、真实 IM 功能与网络实测 | IMPLEMENTED_AND_VERIFIED |
| R3.7 | 当前 WeCom/Feishu 通道在真实第三方账号和网络条件下的可用性 | [acceptance](acceptance.md) | 当前 WeCom/Feishu adapter、绑定、入站去重和 Reply Outbox | 真实第三方账号、网络和回复链路实测 | IMPLEMENTED_AND_VERIFIED |

## R4 治理、监控和安全

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| R4.1 | Plugin/Guardrail/Callbacks 的租户级治理设计 | [architecture-design](architecture-design.md#9-tool-approval-和-governance) | 当前可执行治理链路由 Callbacks、ToolPolicy、Budget、Approval、Secret scope 和 audit 组成，并由 ConfigVersion 固化；Runtime 扩展边界在设计文档中明确 | runtime/governance tests | IMPLEMENTED_AND_VERIFIED |
| R4.2 | 工具白名单和未知工具 fail closed | [data-sync-idempotency](data-sync-idempotency.md) | `ToolCatalog` 当前只注册 framework `todo_write`，Worker 再校验可执行/可见/审批策略 | `runtime/tool_catalog_test.go`、`tool/tool_test.go` | IMPLEMENTED_AND_VERIFIED |
| R4.3 | PII/凭据脱敏且不进 raw trace | [architecture-design](architecture-design.md#10-telemetry-audit-和-secret) | audit/log regex redaction、raw payload tracing disabled、错误安全截断 | `audit_test.go`、`log_test.go`、security Workflow | IMPLEMENTED_AND_VERIFIED |
| R4.4 | 租户预算和 token/cost 记录 | [architecture-design](architecture-design.md#9-tool-approval-和-governance) | 每 execution budget callback；用量缺失 fail closed；可选 operator model pricing；audit/metrics 记录 | governance E2E、metrics tests、外部模型与预算实测 | IMPLEMENTED_AND_VERIFIED |
| R4.5 | 危险工具二次确认 | [architecture-design](architecture-design.md#9-tool-approval-和-governance) | `tool_approval` + `WAITING_APPROVAL`，批准/拒绝重新排队并复用 session lane | approval/worker/governance E2E、外部审批实测 | IMPLEMENTED_AND_VERIFIED |
| R4.6 | IM 用户权限校验 | [architecture-design](architecture-design.md#2-多租户模型与隔离) | `IMAccessPolicy` 对 mapped user/conversation 做 allowlist 判断；拒绝写入 durable inbox | channel inbox/admission tests | IMPLEMENTED_AND_VERIFIED |
| R4.7 | 请求、模型、工具、IM、Session/Memory 等指标 | [architecture-design](architecture-design.md#10-telemetry-audit-和-secret) | OTel metrics recorder、operations gauges、Grafana dashboard | `metrics_test.go`、dashboard、operations tests | IMPLEMENTED_AND_VERIFIED |
| R4.8 | IM callback → Runner → Tool/Session/Memory → IM reply 的 trace | [core-sequence](core-sequence.md) | W3C trace parent/state 随 admission、Redis dispatch、Worker、reply outbox 传递；各边界建 span | `telemetry_test.go`、`runtime/observability_test.go`、外部 Collector 链路实测 | IMPLEMENTED_AND_VERIFIED |
| R4.9 | 审计字段满足 README 最低集合 | [data-model](data-model.md) | `platform.audit_event` 含 tenant/channel/user/session/agent/tool/decision/latency/error/cost/trace/request/config | audit integration、admin read tests | IMPLEMENTED_AND_VERIFIED |
| R4.10 | IM token、模型 key、DB 密码不出现在日志/trace/报告 | [architecture-design](architecture-design.md#10-telemetry-audit-和-secret) | 外部 SecretProvider；API key 只存 digest；provider target HMAC/AES-GCM；Workflow 扫描产物 | secret/log/audit tests、security gates、外部密钥与日志实测 | IMPLEMENTED_AND_VERIFIED |

## R5 故障恢复与运维

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| R5.1 | 节点故障和重复 delivery 恢复 | [data-sync-idempotency](data-sync-idempotency.md) | Redis XAUTOCLAIM、Postgres execution lease/run token、ACK after durable transition | fault E2E、execution fence、多 consumer tests、外部故障恢复实测 | IMPLEMENTED_AND_VERIFIED |
| R5.2 | IM 重试、数据库/Redis 暂时不可用 | [risk-register](risk-register.md) | bounded backoff、readiness gating、dispatch/reply lease recovery；Provider 结果 uncertain 时不自动重试 | fault/reply recovery tests、外部依赖故障实测 | IMPLEMENTED_AND_VERIFIED |
| R5.3 | 模型超时、工具失败、预算失败 | [data-sync-idempotency](data-sync-idempotency.md) | model timeout + context cancel；retryable/permanent/uncertain typed error；预算 fail closed | worker/runtime/governance tests | IMPLEMENTED_AND_VERIFIED |
| R5.4 | `context.Context`、goroutine 生命周期、Runner event channel 排空 | [architecture-design](architecture-design.md#12-故障恢复) | Worker 在释放 session lease 前排空 Runner events；shutdown 先 readiness false、停止 claim、等待 in-flight | worker/main tests | IMPLEMENTED_AND_VERIFIED |
| R5.5 | 灰度发布和租户级配置回滚 | [architecture-design](architecture-design.md#11-灰度与回滚) | immutable ConfigVersion、稳定/Canary、deterministic session hash、pause/disable/rollback/promote Admin API | canary tests、Admin E2E Workflow、外部发布与回滚实测 | IMPLEMENTED_AND_VERIFIED |
| R5.6 | 容量评估：并发 Session、token、Redis/SQL QPS、IM 峰值 | [capacity](capacity.md) | `capacity-evaluate`、`capacity-observe.py` 测试 HTTP 吞吐并采样 Redis/Postgres/容器资源；reply limiter 默认为每 binding 5/s | capacity Workflow 和 evaluator tests、外部容量实测 | IMPLEMENTED_AND_VERIFIED |
| R5.7 | 本地最小部署和生产推荐部署 | [deployment](deployment.md) | Compose、Dockerfile、Kustomize dev/staging/production、HPA/PDB/probes/shutdown | deployment validation、deployment E2E Workflow、外部部署实测 | IMPLEMENTED_AND_VERIFIED |
| R5.8 | 真实生产故障、Kubernetes、外部 Provider 恢复目标 | [acceptance](acceptance.md) | Redis/PostgreSQL/Provider 故障分类、租约接管、Kubernetes 部署和恢复流程 | 外部生产故障、Kubernetes 和 Provider 恢复实测 | IMPLEMENTED_AND_VERIFIED |

## 交付物追踪

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| D1 | 架构设计文档 | [architecture-design.md](architecture-design.md) | 本文档集 | 文档审查 | IMPLEMENTED_AND_VERIFIED |
| D2 | 系统架构图 | [system-architecture.md](system-architecture.md) | `diagrams/system-architecture.mmd`、`.svg` | Mermaid 源图 + SVG 文件检查 | IMPLEMENTED_AND_VERIFIED |
| D3 | 核心时序图 | [core-sequence.md](core-sequence.md) | `diagrams/core-sequence.mmd`、`.svg` | `sequenceDiagram` 源图 + SVG 文件检查 | IMPLEMENTED_AND_VERIFIED |
| D4 | 数据模型 | [data-model.md](data-model.md) | 对照 migrations/model 整理 | schema 抽查 | IMPLEMENTED_AND_VERIFIED |
| D5 | 数据同步和幂等策略 | [data-sync-idempotency.md](data-sync-idempotency.md) | 对照 Admission/Worker/Outbox/Migration | 文档一致性审查 | IMPLEMENTED_AND_VERIFIED |
| D6 | 多后端适配方案 | [backend-adaptation.md](backend-adaptation.md) | 对照各 resolver 和生产组装 | resolver 测试 | IMPLEMENTED_AND_VERIFIED |
| D7 | 至少 8 个生产风险 | [risk-register.md](risk-register.md) | 16 个当前架构风险 | 风险表检查 | IMPLEMENTED_AND_VERIFIED |
| D8 | 基于设计的 GitHub 实现代码 | [architecture-design](architecture-design.md) | 当前仓库已有覆盖本文实现路径的 Go、部署和 Admin UI 代码 | CI/Workflow 定义与外部环境实测 | IMPLEMENTED_AND_VERIFIED |

## README 验收标准追踪

| ID | Requirement | Design Document | Implementation | Validation | Status |
| --- | --- | --- | --- | --- | --- |
| A1 | 覆盖多租户、节点化、同步、多后端、IM、治理监控、恢复 | 本文档集 | R1–R5 全部有条目 | 本矩阵 + [acceptance](acceptance.md) + 外部实测 | IMPLEMENTED_AND_VERIFIED |
| A2 | 数据模型能表达 tenant/agent/channel/session/event/memory/summary/audit | [data-model](data-model.md) | 平台表、框架 Session、外部 Memory | migrations + model tests | IMPLEMENTED_AND_VERIFIED |
| A3 | 至少两种 IM，包含微信或企业微信 | [core-sequence](core-sequence.md)、[backend-adaptation](backend-adaptation.md) | WeCom + Feishu | deterministic IM E2E workflow、真实 IM 实测 | IMPLEMENTED_AND_VERIFIED |
| A4 | 至少三类后端及同步策略 | [backend-adaptation](backend-adaptation.md)、[data-sync-idempotency](data-sync-idempotency.md) | PostgreSQL、Redis、Qdrant、COS、TencentDB 等 | resolver/migration tests、外部后端与迁移实测 | IMPLEMENTED_AND_VERIFIED |
| A5 | 完整消息时序并贯穿 trace/request | [core-sequence](core-sequence.md) | Gateway/Worker/Reply W3C context | telemetry/runtime tests、外部链路实测 | IMPLEMENTED_AND_VERIFIED |
| A6 | 至少 8 个风险及措施 | [risk-register](risk-register.md) | 16 项 | 风险表检查 | IMPLEMENTED_AND_VERIFIED |
| A7 | 明确框架复用与平台新增 | [architecture-design](architecture-design.md) | Runtime 使用 tRPC-Agent-Go；调度/租户/迁移/安全为平台代码 | 源码和 go.mod 抽查 | IMPLEMENTED_AND_VERIFIED |
