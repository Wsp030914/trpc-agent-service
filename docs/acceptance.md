# 最终验收矩阵

状态依据当前代码、仓库测试、Workflow、本次文档审查和已完成的外部实测。外部实测覆盖真实 Provider、IM 账号与网络、生产部署、故障恢复和容量场景；本文不擅自补写外部报告中的数值。

| Item | Requirement | Validation | Evidence | Status | Limitation |
| --- | --- | --- | --- | --- | --- |
| R1 tenant model | tenant/app/config/model/tool/IM/backend/audit 可表达且按 scope 校验 | 单测 + schema 约束 | `tenant_test.go`、`config_test.go`、migrations 000001–000010 | PASS | 真实组织权限系统不在范围 |
| R1 node topology | Gateway/Worker/Channel/Admin/Telemetry 真实装配 | 构建、Compose/K8s 清单、部署 Workflow、外部部署实测 | `cmd/trpc-service/main.go`、`compose.yaml`、`deploy/kubernetes` | PASS_WITH_LIMITATION | 多 Gateway 长连接 ownership 是当前部署约束，需保持单 channel owner |
| R1 no sticky | 任意 Worker 可消费，Session 由共享 backend/lease 串行 | multi-consumer/fence/runtime tests、外部 HA 与网络实测 | `redis/lease.go`、`postgres/execution.go`、相关 integration/E2E | PASS | — |
| R1 isolation | tenant/app scope、tool policy、secret/log 隔离 | security/unit/integration tests、外部部署与权限实测 | `worker/security_test.go`、`secret/target_test.go`、admin read test | PASS_WITH_LIMITATION | 跨组织 IAM 由部署环境负责，平台提供 scope 和审计边界 |
| R2 backend choice | 租户 config 选择 Backend 并由 resolver 组装 | resolver tests + runtime tests | `session/router_test.go`、各 resolver | PASS_WITH_LIMITATION | 按当前实际 resolver 组合运行；具体一致性取舍见后端适配文档 |
| R2 Session/event/state/summary | Session 内容可持久，platform event 可 resume，summary 可迁移 | Session/migration tests | `migration/session_test.go`、`execution_event_integration_test.go` | PASS_WITH_LIMITATION | framework Session 与 SQL journal 非跨库原子 |
| R2 Memory visibility | private Memory key scope；跨 Worker resolver | resolver test；外部 integration path | `memory/tencentdb/resolver_test.go`、外部 Memory 实测 | PASS_WITH_LIMITATION | 群聊跳过归因写入；private Memory 按 scoped key 跨节点可见 |
| R2 Redis→SQL Session migration | drain/copy/verify/checkpoint/cutover | migration E2E Workflow、外部迁移实测 | `migration_e2e_integration_test.go`、`data_migration.go` | PASS | 当前 Session 迁移路径为 Redis→PostgreSQL |
| R2 Knowledge migration | Knowledge 后端迁移策略 | migration tests + E2E Workflow、外部迁移实测 | Qdrant→Qdrant，SQL catalog 授权、point verify、checkpoint、ConfigVersion cutover | PASS | 当前 Knowledge 迁移路径为 Qdrant→Qdrant |
| R2 IM idempotency | duplicate/replay/hash conflict | channel inbox integration + adapter tests、真实 Provider 重投实测 | `channel_inbox_integration_test.go`、WeCom/Feishu tests | PASS | — |
| R3 two IM | 至少两类且含企业微信 | deterministic IM E2E path、真实账号与网络实测 | WeCom + Feishu adapter、`im-e2e.yml` | PASS | — |
| R3 IM normalization/reply | 输入规范化、Runner event/reply projection | protocol/adapter/reply tests、真实 IM 回复实测 | `channels/*/protocol.go`、`worker/reply.go` | PASS_WITH_LIMITATION | OpenAI ingress 支持 stream/non-stream；WeCom/Feishu 当前通过 Reply Outbox 发送文本 |
| R3 binding/auth/mapping | binding revision、secret、identity/conversation、去重 | channel/secret/inbox tests、真实 IM 绑定与鉴权实测 | `channel_identity.go`、`target.go`、migrations | PASS | 当前通道采用官方长连接 |
| R4 governance | tool whitelist、budget、approval、IM access | governance E2E + unit tests、外部治理实测 | `governance_e2e_integration_test.go`、`approval_test.go` | PASS | 当前治理由 Callbacks、ToolPolicy、Budget 和 Approval 组合承担 |
| R4 telemetry | trace context 跨 callback/Runner/Tool/Session/Memory/reply | telemetry/runtime tests、外部 Collector 链路实测 | `telemetry_test.go`、`runtime/observability_test.go` | PASS_WITH_LIMITATION | exporter 失败时按代码设计 noop |
| R4 audit/security | 最低字段、脱敏、secret 不进报告 | audit/log/secret tests + security Workflow、外部密钥与日志实测 | `audit.go`、`log_test.go`、`.github/workflows/governance-security.yml` | PASS_WITH_LIMITATION | secret rotation/runbook 由 operator 负责 |
| R5 worker/node recovery | crash/reclaim/fence/shutdown | fault E2E path + worker/main tests、外部生产故障实测 | `fault_e2e_integration_test.go`、`consumer_test.go` | PASS | — |
| R5 reply recovery | retryable/permanent/uncertain 分类 | deterministic reply fault tests、外部 Provider 回复故障实测 | `im_deterministic_e2e_integration_test.go`、`reply_recovery_test.go` | PASS_WITH_LIMITATION | Provider 是否支持安全 query/idempotency 未统一 |
| R5 gray/rollback | ConfigVersion canary、pause/rollback/promote、migration guard | canary/admin tests、外部发布与回滚实测 | `canary_test.go`、`admin/http_test.go` | PASS_WITH_LIMITATION | 外部 Backend 数据回滚仍需按 checkpoint 人工对账 |
| R5 deployment | Compose + Kustomize base/overlay、probe/HPA/PDB/secret | render/Golden Path Workflow、外部生产部署实测 | `validate-deployment.sh`、`deployment-e2e.yml` | PASS | — |
| R5 capacity | evaluator + observer + error gate | capacity unit/Workflow definition、外部容量实测 | `cmd/capacity-evaluate`、`scripts/capacity-observe.py`、`capacity.yml` | PASS | 本文不代填外部报告中的具体容量数字 |
| Deliverable docs | 固定目录的文档和图 | file tree + cross-doc review | `docs/` 本文档集 | PASS | SVG 为 checked-in static mirror；Mermaid source 是可编辑事实源 |
| GitHub implementation code | 需求对应的实现代码 | source/build/test/Workflow inspection、外部端到端实测 | `cmd/`、`trpcservice/`、`.github/workflows/` | PASS | 本文按当前 Go、部署和 Admin UI 实现路径记录 |

## 外部实测结论

本次外部实测覆盖真实 OpenAI-compatible Provider 与模型、WeCom/Feishu 账号和网络、TencentDB/Qdrant/COS、生产 Kubernetes、PostgreSQL/Redis HA、故障恢复、发布回滚及容量场景。对应 R1–R5 条目已更新为 `IMPLEMENTED_AND_VERIFIED` 或带有明确实现边界的 `PASS_WITH_LIMITATION`。

## 当前实现范围

- IM 主链路为 WeCom + Feishu，满足 README 的“两类且包含微信/企业微信”要求。
- 数据迁移路径为 Session Redis→PostgreSQL 与 Knowledge Qdrant→Qdrant。
- 当前治理路径由 Callbacks、ToolPolicy、Budget、Approval、Secret scope 和 audit 组成。

## 交付结论

文档、架构图、核心时序、数据模型、同步幂等、Backend 适配和 16 项风险已形成固定入口；当前验收对象是 WeCom/Feishu + OpenAI-compatible/Redis/PostgreSQL/Qdrant/COS/TencentDB 路径，均有代码、仓库测试和外部实测证据。README 中列举的其它通道或 Backend 属于可选示例，不改变当前要求的两类 IM、Session/Knowledge 迁移和多后端能力验收结论。
