# 最终验收矩阵

本文只根据当前工作树中的源码、部署清单、测试和 Workflow 定义判断。Workflow 定义不等于已经发生的 CI run；deterministic/stub Provider 不等于真实第三方 Provider。当前工作树没有保存可复核的外部报告、run number 或 artifact URL，因此不写 `EXTERNALLY_VERIFIED`。

状态含义：

- `IMPLEMENTED`：实现路径存在，但当前证据不足以标成仓库验证。
- `REPO_VERIFIED`：可由仓库源码、静态检查、单测、集成/E2E 测试或 CI Workflow 证据核对。
- `EXTERNALLY_VERIFIED`：仓库中保存了真实 Provider、真实 IM、真实 Kubernetes/HA 或容量报告，可由评审复核。
- `EXTERNAL_VERIFICATION_NOT_INCLUDED`：实现边界已明确，但当前仓库没有外部运行证据。
- `NOT_APPLICABLE`：不适用于当前实现路径。

## R1–R5

| Item | Requirement | Implementation / evidence | Status | External boundary |
| --- | --- | --- | --- | --- |
| R1 tenant model | tenant/app/config/model/tool/IM/backend/audit 可表达并按 scope 校验 | `trpcservice/tenant`、PostgreSQL migrations、Admin/config tests | REPO_VERIFIED | 真实组织 IAM 不在仓库范围 |
| R1 node topology | Gateway、Channel Adapter、Worker、Admin、Telemetry 真实分工 | `cmd/trpc-service/main.go`；`gateway`/`channel`/`worker` roles；Compose/Kustomize | REPO_VERIFIED | 生产 Kubernetes admission、网络和 SecretProvider 未外部验证 |
| R1 scaling/no sticky | Gateway 可多副本；Worker pull/claim 可扩；不依赖 sticky Session | Redis Stream Consumer Group、PostgreSQL execution lease/fence、Redis Session Lease/Lock、multi-consumer/fault tests | REPO_VERIFIED | 生产 HA backend 未外部验证 |
| R1 channel ownership | Gateway 多副本不拥有 IM 长连接；Channel 单 owner | Channel role、Compose `channel`、K8s `replicas: 1` + `Recreate`、deployment validation | REPO_VERIFIED | 尚无 distributed binding lease/leader election/channel sharding |
| R1 isolation | tenant/app/session/tool/secret/log/audit 隔离 | scoped keys、SQL scope predicates、ToolPolicy、SecretProvider、redaction/security tests | REPO_VERIFIED | 真实跨组织 IAM/RBAC 未外部验证 |
| R2 backend choice | Session/Memory/Knowledge/Artifact/Queue 等按 ConfigVersion 选择 | Session Router、TencentDB/Qdrant/COS resolvers、Redis/PostgreSQL store tests | REPO_VERIFIED | 外部服务 SLA/容量未外部验证 |
| R2 ordering and migration | Session lane、event/state/summary 顺序、Redis→SQL 与 Qdrant→Qdrant 迁移 | `session_lane`、leases/fencing、migration executor/copy/verify tests/workflows | REPO_VERIFIED | 没有外部迁移报告 |
| R2 Memory visibility | private Memory scope 跨 Worker 可解析；群聊保守跳过归因写入 | TencentDB resolver and runtime scope tests | IMPLEMENTED | 真实 TencentDB 可见性未外部验证 |
| R2 IM idempotency | duplicate/replay/hash conflict 可持久判定 | `channel_inbox`、adapter/integration tests | REPO_VERIFIED | 真实 Provider 重投行为未外部验证 |
| R3 two IM | 至少两类 IM，含企业微信 | WeCom Bot WebSocket + Feishu/Lark WebSocket adapters and deterministic IM workflow | REPO_VERIFIED | 无真实第三方账号/网络报告 |
| R3 protocol/auth | 官方 WebSocket/long connection 认证、binding revision、secret scope；webhook URL/HTTP callback signature 对当前模式 N/A | adapter protocol tests、binding/secret tests、`architecture-design.md` IM section | REPO_VERIFIED | 不把 WebSocket authentication 写成 webhook 验签 |
| R3 mapping/session/reply | normalize、direct/group/topic isolation、identity mapping、async text reply、dedupe/retry | `ChannelInput`、identity/conversation、Reply Outbox、IM deterministic tests | REPO_VERIFIED | provider-specific receipt/limit 真实行为未外部验证 |
| R4 governance | tool whitelist、executable/review-required、approval、IM access、per-execution budget | ToolCatalog、Approval、Budget callbacks、governance E2E/unit tests | REPO_VERIFIED | 当前不是 daily/monthly/aggregate tenant billing quota |
| R4 telemetry/audit/security | trace propagation、metrics、audit fields、redaction、secret isolation | telemetry/runtime/audit/log/secret tests、security Workflow | REPO_VERIFIED | 真实 Collector/KMS/log pipeline 未外部验证 |
| R5 recovery | node/queue/DB/Redis/model/tool/cancel/reply failure classification and recovery | lease/fence, XAUTOCLAIM, retry/uncertain, shutdown, fault/reply tests/workflows | REPO_VERIFIED | 无生产故障演练记录 |
| R5 canary/rollback | immutable ConfigVersion、pause/rollback/promote、migration gate | canary/Admin tests and migration guards | REPO_VERIFIED | 无生产流量编排报告 |
| R5 deployment | Compose、Kustomize、probe、HPA/PDB、single-owner Channel | `compose.yaml`、`deploy/kubernetes`、`scripts/validate-deployment.sh`、deployment Workflow | IMPLEMENTED | 本工作树未执行真实 Kubernetes admission/HA 发布 |
| R5 capacity | evaluator、observer、error gate、规划公式 | `cmd/capacity-evaluate`、`scripts/capacity-observe.py`、capacity Workflow | IMPLEMENTED | 没有真实 Provider/IM/生产数据库容量报告 |
| R5 external operations | 生产 HA、RTO/RPO、真实 Provider/IM、生产容量 | 当前仅有受控测试进程和部署清单 | EXTERNAL_VERIFICATION_NOT_INCLUDED | 不把清单或 Workflow 定义写成生产验证 |

## 交付物

| ID | Deliverable | Evidence | Status |
| --- | --- | --- | --- |
| D1 | 架构设计文档 | `docs/architecture-design.md` | REPO_VERIFIED |
| D2 | 系统架构图 | `docs/diagrams/system-architecture.mmd/.svg`、`system-architecture.md` | REPO_VERIFIED |
| D3 | 核心时序图 | `docs/diagrams/core-sequence.mmd/.svg`、`core-sequence.md` | REPO_VERIFIED |
| D4 | 数据模型 | `data-model.md`、migrations、logical/external Memory/Summary model | REPO_VERIFIED |
| D5 | 数据同步和幂等策略 | `data-sync-idempotency.md`、Admission/lease/outbox/migration code | REPO_VERIFIED |
| D6 | 多后端适配方案 | `backend-adaptation.md`、Session/Memory/Knowledge/Artifact resolvers | REPO_VERIFIED |
| D7 | 至少 8 个生产风险 | `risk-register.md`，当前 16 项 | REPO_VERIFIED |
| D8 | 基于设计的 GitHub 实现代码 | `cmd/`、`trpcservice/`、部署文件、Workflows | REPO_VERIFIED |

## 明确未包含的外部证据

- 真实 OpenAI-compatible Provider/model 的 token、费用、限流和网络行为。
- 真实 WeCom/Feishu 账号、网络、重投、撤回、回执和第三方连接 ownership。
- 真实 TencentDB/Qdrant/COS 的 SLA、容量、跨节点可见性和故障恢复。
- 生产 Kubernetes、HPA/PDB、HA PostgreSQL/Redis、备份恢复以及 RTO/RPO。
- 生产容量、SLO、IM 峰值、Session 并发上限和租户级成本。
