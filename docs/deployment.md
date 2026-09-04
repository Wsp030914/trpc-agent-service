# 部署与运维

## 最小可运行拓扑

`compose.yaml` 提供以下最小拓扑：PostgreSQL、Redis、Qdrant、Gateway、`worker-1`、`worker-2`、OpenTelemetry Collector、Jaeger 和 Prometheus。模型、TencentDB、COS、Feishu、WeCom 是外部服务，通过租户 immutable AppConfig 与运行时 Secret 注入连接，不在本地伪造。

```powershell
Copy-Item .env.example .env
# 在 .env 填入仅开发环境使用的 PostgreSQL / Redis / Admin 值；真实 provider 密钥由忽略的 .env 或外部 secret 注入。
docker compose up --build
docker compose ps
Invoke-WebRequest http://127.0.0.1:8080/readyz
```

Gateway 与两个 Worker 都以 `/readyz` 作为 Compose healthcheck。服务启动完成后，再用 Admin/配置流程创建租户配置，并通过已配置的 Feishu/WeCom 长连接发送一条真实消息，验证 Admission → Redis Stream → Worker → Reply Outbox 完整链路。没有为本地环境新增第二个模型或任务模拟器。

`/healthz` 仅说明进程存活，恒为 `200`；`/readyz` 还要求节点尚未进入 shutdown 且 PostgreSQL、Redis 可用。Collector 将 traces 转发到 Jaeger、metrics 暴露给 Prometheus，但不在关键路径；其不可用时主链路继续运行并以 no-op telemetry 降级。审计 retention 由 `go run ./cmd/audit-purge` 按租户策略批量清理。

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
| HPA | Gateway 基于 HTTP RPS、IM 事件速率与 Admission p95；Worker 基于 queue lag、running executions、CPU/Memory。需要的自定义指标由现有观测后端提供，不自研 autoscaler |
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

## 故障运维边界

Execution delivery 是 **at-least-once**。Worker 崩溃后 Execution Lease 与 Session Lock 最终过期，Redis pending delivery 由其他 Worker 重新领取，并在 PostgreSQL 条件 Claim 中重新验证状态；已完成 Execution 不会再次有效执行。具有外部副作用的 Tool 不做盲目自动重试：若 Tool 支持幂等，必须使用稳定 idempotency key；否则接受 at-least-once 风险并在审计/运行手册中处理。

Gateway Admission 遇 PostgreSQL 不可用直接返回 retryable error，不把请求藏进本地内存。Relay、Worker、Reply Sender 对短暂的 Redis/PostgreSQL 故障使用有上限的指数退避和 jitter；Reply Outbox 的每行发送有最大次数，尊重 provider `Retry-After`，context cancel 会停止等待。Session/Memory 后端不可用不切换到未经租户配置的后端，而是记录 SafeError、Metric、Trace 并按当前 Execution 语义结束。

## 真实 IM E2E 验收契约

真实 IM 验收使用官方长连接，不再通过 HTTP callback 返回 200 判断成功。每个 ACTIVE
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
