# 部署与运维

本轮部署增强的实施状态与验收矩阵见
[部署能力补强实施追踪](deployment-enhancement.md)。本文是实际运行手册；
`secret.example.yaml` 只提供字段示例，不能作为真实 Secret 提交或直接应用。

## 最小可运行拓扑

`compose.yaml` 提供以下最小拓扑：PostgreSQL、Redis、Qdrant、Gateway、`worker-1`、`worker-2`、OpenTelemetry Collector、Jaeger、Prometheus 和 Grafana。模型、TencentDB、COS、Feishu、WeCom 是外部服务，通过租户 immutable AppConfig 与运行时 Secret 注入连接，不在本地伪造。

```powershell
Copy-Item .env.example .env
# 在 .env 填入仅开发环境使用的 PostgreSQL / Redis / Admin 值；真实 provider 密钥由忽略的 .env 或外部 secret 注入。
docker compose up -d --build --wait
docker compose ps
Invoke-WebRequest http://127.0.0.1:8080/readyz
```

默认宿主端口为 PostgreSQL `55432`、Redis `56379`、Qdrant `56333/56334`、Gateway
`8080`、Jaeger `16686`、Prometheus `19090`、Grafana `13000`。并行运行 disposable
项目时，在 `.env` 或进程环境覆盖 `TRPC_AGENT_SERVICE_COMPOSE_*_PORT`，不要停掉其他项目。

Gateway 与两个 Worker 都以 `/readyz` 作为 Compose healthcheck。服务启动完成后，再用 Admin/配置流程创建租户配置，并通过已配置的 Feishu/WeCom 长连接发送一条真实消息，验证 Admission → Redis Stream → Worker → Reply Outbox 完整链路。没有为本地环境新增第二个模型或任务模拟器。

Compose 默认将 `TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT=30s` 传入 Gateway/Worker，并使用
`stop_grace_period=45s`。若在 `.env` 中覆盖 shutdown timeout，同时覆盖
`TRPC_AGENT_SERVICE_COMPOSE_STOP_GRACE_PERIOD`，确保后者更长。

部署门禁可直接运行 `bash scripts/validate-deployment.sh`：它会检查 Compose/Kustomize
渲染、Deployment/Service 探针与端口、ConfigMap 的敏感字段边界、OTel Collector 配置，
并构建 Gateway 与 Admin UI 镜像。完整构建与渲染由 Deployment E2E 负责；普通 CI 只做
Go 基础门禁，避免重复构建。

真实 Worker 崩溃验收由 Workflow
`.github/workflows/fault-e2e.yml` 驱动：它使用隔离测试租户，等待 Worker A
将执行持久化为 `RUNNING` 后发送 `SIGKILL`，再由 Worker B 等待 lease 到期接管，最后
校验执行进入终态、dispatch outbox 全部 `CONSUMED`、Redis Stream pending 为零。
故障 Workflow 使用 deterministic Worker 进程控制阻塞、退出和依赖中断，不依赖外部服务。

`/healthz` 仅说明进程存活，恒为 `200`；`/readyz` 还要求节点尚未进入 shutdown 且 PostgreSQL、Redis 可用。Collector 将 traces 转发到 Jaeger、metrics 暴露给 Prometheus，但不在关键路径；其不可用时主链路继续运行并以 no-op telemetry 降级。Prometheus 加载 `deploy/otel/alerts.yml`，Grafana 自动 provision Prometheus 数据源和 `trpc-agent-service Operations` Dashboard。默认本地入口为 Jaeger `16686`、Prometheus `19090`、Grafana `13000`，可用 `TRPC_AGENT_SERVICE_*_URL` 覆盖 Admin Operations 外链。审计 retention 由 `go run ./cmd/audit-purge` 按租户策略批量清理。

## 本地 TencentDB Agent Memory Gateway 验收

仓库的 TencentDB Memory 适配器只连接运维配置的 Agent Memory Gateway，不直连
PostgreSQL、Redis 或 Qdrant。TencentDB Agent Memory 提供 standalone Gateway：数据落在本地
SQLite/文件目录，默认监听 `127.0.0.1:8420`。Windows 本地验收可使用官方仓库的
`MemoryCore` 目录：

```powershell
git clone https://github.com/TencentCloud/TencentDB-Agent-Memory.git .tmp/tencentdb-agent-memory
Set-Location .tmp/tencentdb-agent-memory/MemoryCore
npm install --ignore-scripts

$env:TDAI_GATEWAY_CONFIG = "$PWD/tdai-gateway.standalone.yaml"
$env:TDAI_LLM_API_KEY = "<由外部 Secret 注入>"
$env:TDAI_LLM_BASE_URL = "<OpenAI-compatible base URL>"
$env:TDAI_LLM_MODEL = "<model>"
$env:TDAI_GATEWAY_API_KEY = "<独立的本地 Gateway token>"
$env:TDAI_DATA_DIR = "$PWD/../../tencentdb-data"
node --import tsx src/gateway/server.ts
```

`TDAI_LLM_API_KEY` 只给 Gateway 调用模型使用；不要把模型 key 当作 Gateway
Bearer token。主仓库的真实适配器验收使用本地 Gateway token：

```powershell
$env:TRPC_AGENT_SERVICE_TENCENTDB_GATEWAY_URL = "http://127.0.0.1:8420"
$env:TRPC_AGENT_SERVICE_TENCENTDB_API_KEY = "<与 TDAI_GATEWAY_API_KEY 相同>"
go test -tags='integration external' ./trpcservice/memory/tencentdb `
  -run TestRealGatewayCaptureAndSearch -count=1
```

该测试依次验证 Gateway health、resolver 创建 tenant-scoped Service、真实 capture、
session end，以及按 session scope 的 conversation search；未设置环境变量时自动跳过，
不会影响默认单元测试。测试所用的 LLM、Gateway token 和本地数据目录都不进入仓库。

## 生命周期

收到 `SIGTERM`/`SIGINT` 后，服务按以下边界收口：

1. 先标记 Not Ready；Gateway 不再接收新请求，Worker 不再 receive/claim。
2. 取消 Relay、Reply Sender、数据迁移等辅助循环；它们的 sleep/retry 均响应 context。
3. 已取得 Lease 的执行保留到 grace period 内完成：Runner 的 events 继续 drain，随后 `Runner.Close`，再释放 Session Lock。
4. grace period 到达仍未结束时取消执行 context；最后关闭 HTTP、DB、Redis、Telemetry 客户端并退出。

默认 shutdown grace 由 `TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT` 控制；模型调用上限由 `TRPC_AGENT_SERVICE_MODEL_TIMEOUT` 控制。二者应由生产 SLO 共同设定，Kubernetes `terminationGracePeriodSeconds` 必须大于该 shutdown timeout。

## 生产推荐：Kubernetes

```text
Ingress / Load Balancer
        ↓
Gateway Deployment ── PostgreSQL / Redis
        ↓                    ↓
Worker Deployment ── Model / Tool / Storage
        ↓
OTel Collector → Observability backend
```

Gateway 和 Worker 必须是独立 Deployment、独立 Service、独立 HPA；不要把两者放入同一个固定扩缩容单元。

| 对象 | 最小要求 |
| --- | --- |
| Gateway Deployment | `readinessProbe: /readyz`、`livenessProbe: /healthz`、滚动更新 `maxUnavailable: 0`、`maxSurge: 1`、显式 requests/limits、graceful termination |
| Worker Deployment | 相同 probes；`terminationGracePeriodSeconds > TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT`；滚动更新时保留旧 Pod 直到其已领取执行收口 |
| Service / Ingress | 只将 Gateway 暴露给外部；Worker Service 只作内部 health/运维访问（如需要） |
| HPA | Gateway/Worker 当前均使用 CPU-based `autoscaling/v2`；必须配置 CPU requests。queue lag、pending jobs、running executions 可作为后续扩容信号，本轮不引入 KEDA/custom metrics |
| PDB | Gateway 和 Worker 分别至少保留一个可用副本；副本数与 PDB 不能互相矛盾 |
| Secret | Provider credential、数据库密码用 Secret/external injector 挂载或环境变量注入，禁止进镜像、ConfigMap、日志或 Git |
| Config | 非敏感运行参数进 ConfigMap；租户行为配置只通过 immutable AppConfig Version 发布 |

以下资源数值只是起始 manifest 参数，必须由 [容量评估](capacity.md) 实测后替换，不是性能承诺：

```yaml
resources:
  requests: {cpu: 500m, memory: 512Mi}
  limits:   {cpu: "2", memory: 2Gi}
readinessProbe:
  httpGet: {path: /readyz, port: 8080}
livenessProbe:
  httpGet: {path: /healthz, port: 8080}
terminationGracePeriodSeconds: 45
```

## 灰度、数据库与配置回滚

代码发布顺序是 canary → observe（queue lag、错误率、模型/IM 限流、Reply retry）→ 增加新 Worker 副本 → 完成 rollout。新旧 Worker 共存期间必须兼容 Redis Stream message、Execution 状态、Reply Outbox、ConfigVersion 和 PostgreSQL schema。

数据库变更固定采用 `expand → deploy → migrate/use → contract later`。先添加可空字段/新表/兼容读取，再部署新代码，确认旧 Worker 已退出后才在后续版本 contract；禁止同一版本删旧字段。

租户配置只切换 `active_config_version`：

```text
稳定 v10 → active_config_version = v11
发现问题 → active_config_version = v10
```

已经 Admission 的 Execution 永远使用其固定的 `config_version`（例如 v11）；回滚之后新 Admission 才使用 v10。历史版本 immutable，严禁修改 v10/v11 内容来伪造回滚。

### 多副本 migration

Gateway/Worker 启动时的 schema migration 使用 PostgreSQL transaction-level advisory lock
并校验 migration checksum；多副本可以同时启动，但同一时刻只有持锁实例执行迁移，不需要
额外 migration service。该 schema migration 与 Redis→PostgreSQL/Qdrant 的数据迁移不同，
后者仍由应用现有 Lease、checkpoint 和 state machine 负责。不可逆 schema/data 变更必须
单独制定备份/恢复策略，不能用 rollback image 假设恢复数据库。

## 故障运维边界

Execution delivery 是 **at-least-once**。Worker 崩溃后 Execution Lease 与 Session Lock 最终过期，Redis pending delivery 由其他 Worker 重新领取，并在 PostgreSQL 条件 Claim 中重新验证状态；已完成 Execution 不会再次有效执行。具有外部副作用的 Tool 不做盲目自动重试：若 Tool 支持幂等，必须使用稳定 idempotency key；否则接受 at-least-once 风险并在审计/运行手册中处理。

Gateway Admission 遇 PostgreSQL 不可用直接返回 retryable error，不把请求藏进本地内存。Relay、Worker、Reply Sender 对短暂的 Redis/PostgreSQL 故障使用有上限的指数退避和 jitter；Reply Outbox 的每行发送有最大次数，尊重 provider `Retry-After`，context cancel 会停止等待。Session/Memory 后端不可用不切换到未经租户配置的后端，而是记录 SafeError、Metric、Trace 并按当前 Execution 语义结束。

## 真实 IM Manual Provider Acceptance 契约

真实 Provider 验收只属于最终人工 Acceptance，不属于 GitHub Actions CI Gate。CI 使用本地
deterministic Feishu/WeCom fake provider 验证完整内部链路。人工验收使用官方长连接，不再通过 HTTP callback 返回 200 判断成功。每个 ACTIVE
Binding 启动一个独立 Provider client；收到事件后必须能在现有 Gateway → Inbox →
Worker → Reply Outbox 链路完成一次执行和回复。

第三方只提供以下信息：

```text
WECOM_BOT_ID
WECOM_BOT_SECRET
FEISHU_APP_ID
FEISHU_APP_SECRET
```

项目内部将 Bot Secret/App Secret 保存为对应租户应用 Binding 的 `SecretRef`，不会要求
Webhook URL、Token、EncodingAESKey、Verification Token、Encrypt Key、签名、解密、URL
challenge 或手工 Tenant Key。

## Kubernetes 可执行部署流程

Kubernetes 只部署 Gateway/Worker 应用层。生产 PostgreSQL、Redis、Qdrant 和 OTel
Collector 可使用 Managed Service 或外部服务；不要为了套用清单在 Cluster 内新增同名依赖。

### 1. 准备固定镜像

镜像必须使用 release version、Git commit SHA 或 digest，不使用 `latest` 或本地
`trpc-agent-service:dev`：

```text
<registry>/trpc-agent-service:<git-sha>
<registry>/trpc-agent-service@sha256:<digest>
```

`<registry>` 可为 GHCR、Docker Hub 或企业内部 Registry。编辑所选 Overlay 的
`images` 项设置 `newName` 与 `newTag`（release version 或 Git SHA）；若使用 digest，则
删除 `newTag` 并设置 `digest: sha256:<64-hex-digits>`。Registry credential 由 Cluster
管理员、ServiceAccount 或外部 Secret injector 配置，不进入仓库。

### 2. 创建 Kubernetes Secret

先创建 namespace，再创建固定名称 `trpc-agent-service-secrets`。下面值均为占位符，
不要复制真实凭据到 Git、Issue 或命令输出：

```bash
NAMESPACE=trpc-agent-service-production
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic trpc-agent-service-secrets \
  --from-literal=TRPC_AGENT_SERVICE_POSTGRES_DSN='<managed-postgres-dsn>' \
  --from-literal=TRPC_AGENT_SERVICE_REDIS_URL='<managed-redis-url>' \
  --from-literal=TRPC_AGENT_SERVICE_ADMIN_TOKEN='<admin-token>'
```

租户 AppConfig 启用 Provider SecretRef 时，再把对应的 scoped key 注入同一 Secret 或外部
Secret provider。运行时 key 形状为
`TRPC_AGENT_SERVICE_SECRET_<tenant-id-hex>_<app-id-hex>_<secret-name-hex>_<version-hex>`，四个
后缀均为对应 UTF-8 字节的 hex 编码。不要把 Provider credential 放进 ConfigMap。

本地只使用被 `.gitignore` 忽略的 `.env`。GitHub Actions 的普通部署校验只使用
`.env.example` 假值；若某个集成任务确实需要外部服务，使用 GitHub Repository Secret 或
Environment Secret，在 Workflow 的 `env`/命令参数中通过 `${{ secrets.NAME }}` 注入，不写入
文件、日志或 artifact。生产 Environment 应配置审批和最小权限。

### 3. 配置生产 Overlay

选择 `deploy/kubernetes/overlays/dev`、`staging` 或 `production`。生产部署前修改
`overlays/production/kustomization.yaml` 的固定 image，并修改
`overlays/production/configmap-patch.yaml` 中的 Qdrant/OTel endpoint 示例值。
PostgreSQL DSN、Redis URL 通过上一步 Secret 提供；它们不写入 ConfigMap。

### 4. Render、Apply、检查

```bash
kubectl kustomize deploy/kubernetes/overlays/production > /tmp/trpc-agent-service.yaml
kubectl apply -k deploy/kubernetes/overlays/production
kubectl -n trpc-agent-service-production get pods,svc,hpa,pdb
kubectl -n trpc-agent-service-production rollout status \
  deployment/trpc-agent-service-gateway --timeout=5m
kubectl -n trpc-agent-service-production rollout status \
  deployment/trpc-agent-service-worker --timeout=5m
kubectl -n trpc-agent-service-production logs deployment/trpc-agent-service-gateway --tail=100
kubectl -n trpc-agent-service-production logs deployment/trpc-agent-service-worker --tail=100
```

应用入口仍是 Gateway Service。验收环境可以 port-forward：

```bash
kubectl -n trpc-agent-service-production port-forward \
  svc/trpc-agent-service-gateway 8080:8080
curl -fsS http://127.0.0.1:8080/readyz
```

真实生产入口按集群环境选择 Ingress、Gateway API 或 LoadBalancer；Worker 不配置公网入口。

### 5. 回滚

应用镜像问题使用 Kubernetes Deployment rollback：

```bash
kubectl -n trpc-agent-service-production rollout history \
  deployment/trpc-agent-service-gateway
kubectl -n trpc-agent-service-production rollout undo \
  deployment/trpc-agent-service-gateway
kubectl -n trpc-agent-service-production rollout undo \
  deployment/trpc-agent-service-worker
kubectl -n trpc-agent-service-production rollout status \
  deployment/trpc-agent-service-gateway --timeout=5m
```

这只回滚代码/镜像，不回滚租户配置。ConfigVersion rollback 继续通过项目已有的
immutable ConfigVersion 控制面执行；已经 admission 的 Execution 仍使用其 pin 住的
版本。不可逆数据库 migration 需要独立备份/恢复策略，不能假设镜像 rollback 能恢复数据。

## 部署校验

本地和 GitHub Actions 共用：

```bash
bash scripts/validate-deployment.sh
```

校验包含 Compose render、Compose healthcheck、Dockerfile build、Kustomize base 与三套
Overlay render、离线 API version/kind 检查、Probe/Service/SecretRef/HPA/PDB 结构、生产
镜像和外部 endpoint 规则、ConfigMap 敏感字段扫描及 OTel Collector 配置。普通 CI 不启动
Kind 或真实 Kubernetes Cluster。安装固定版本 `kubeconform` 时脚本会优先使用它；否则使用
`kubectl kustomize` 的 YAML 解析结果加项目资源 allow-list，避免离线 CI 访问 API Server。

Deployment Golden Path 位于 `.github/workflows/deployment-e2e.yml`，覆盖完整 Compose
启动、依赖/Gateway/双 Worker readiness、bootstrap、正式请求、重启一个 Worker、再次请求、
日志/artifact 和带卷清理的 shutdown。
