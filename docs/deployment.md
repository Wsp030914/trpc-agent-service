# 部署与运维

部署事实来自 `Dockerfile`、`compose.yaml`、`compose.*-e2e.yaml`、`deploy/kubernetes`、`deploy/otel`、`admin-ui/Dockerfile` 和 `.github/workflows`。应用角色启动时执行 PostgreSQL schema migration；Kubernetes 发布通过 Kustomize overlay 完成。

## 本地最小运行

使用仓库提供的开发变量启动 PostgreSQL、Redis、Qdrant、Gateway 和 Worker：

```bash
docker compose --env-file .env.example up -d --build --wait postgres redis qdrant gateway worker-1 worker-2
curl -fsS http://127.0.0.1:8080/readyz
```

`.env.example` 中的密码、token 和 endpoint 只适合 disposable/local 环境；生产不得直接复用。Admin UI 和可观测性是可选的本地组件：

```bash
docker compose --env-file .env.example up -d --build --wait admin-ui otel-collector jaeger prometheus grafana
```

本地 Compose 实际包含：PostgreSQL 16、Redis 7（AOF）、Qdrant 1.16、OTel Collector 0.111、Jaeger 1.57、Prometheus 2.53、Grafana 11.1、Gateway、两个显式 Worker 和 Admin UI。Gateway 端口默认 `127.0.0.1:8080`，Admin UI 默认 `127.0.0.1:4173`；数据库、Redis、Qdrant 和观测端口也绑定 loopback。

## Compose 拓扑和启动依赖

- Gateway 依赖 PostgreSQL/Redis healthy，暴露 `/readyz`，运行 HTTP ingress、Admin API、WeCom/Feishu adapters 和 dispatch relay。
- 每个 Worker 依赖 PostgreSQL/Redis/Qdrant healthy，暴露 `/readyz`，运行 Consumer、Runner、Reply Sender、migration、cleanup 和 heartbeat。Worker ID 明确为 `worker-1`、`worker-2`；不能简单复制一个带相同 ID 的容器。
- Admin UI 是 Node 24 构建后由 Nginx 1.27 提供静态文件，`/admin/v1/` 反向代理到 Gateway；Kubernetes 清单没有该 UI。
- `compose.admin-e2e.yaml`、`compose.deployment-e2e.yaml` 把 Worker 镜像替换为 `Dockerfile.e2e-worker`，使用 deterministic model/approval 模式；这用于受控验收，不是生产模型。

Compose 中 Gateway/Worker 的默认 `stop_grace_period` 是 45s，模型超时默认 1m，Worker concurrency 默认 4，artifact retention 默认 720h。实际变量可在 `.env` 中覆盖，敏感值不能提交。

## Kubernetes 推荐拓扑

入口是 `kubectl kustomize deploy/kubernetes/overlays/<env>`；base 包含：

- `trpc-agent-service-gateway` Deployment 和 ClusterIP Service。
- `trpc-agent-service-worker` Deployment 和 ClusterIP Service。
- 两者默认 replicas=2，RollingUpdate `maxUnavailable=0/maxSurge=1`，termination grace=45s。
- 两个 HPA 按 CPU 70% 扩缩，base 为 2–10 副本，缩容稳定窗口 300s；两个 PDB `minAvailable: 1`。
- Gateway/Worker 请求资源均为 500m CPU/512Mi，限制为 2 CPU/2Gi。

环境 overlay 是当前仓库实际提供的三个选择：

| Overlay | replicas / HPA | 其它配置 |
| --- | --- | --- |
| `dev` | Gateway/Worker 1；HPA 1–2 | image tag `dev` |
| `staging` | base replicas 2；HPA max 6 | OTLP/Qdrant example endpoint，需要替换 |
| `production` | Gateway/Worker 3；HPA 3–20；缩容窗口 600s | termination grace 75s；请求 1 CPU/1Gi；限制 4 CPU/4Gi；Worker concurrency 8；shutdown timeout 60s；镜像应替换为不可变 release/tag/digest |

HPA 只按 CPU；当前没有基于 queue lag 的 Kubernetes custom metric。Worker 扩容能增加 execution claim capacity，但不会并行执行同一 Session；Gateway 扩容能增加 HTTP capacity，但当前每个 Gateway 都会运行所有 active WeCom/Feishu binding 的连接，缺少分布式 channel ownership，见[风险登记](risk-register.md)。

## Probe、readiness 和 graceful shutdown

服务实现的 `/healthz` 在 HTTP server 可接收请求时返回 200；`/readyz` 只有进程角色组件启动并且 PostgreSQL、Redis ping 成功才返回 200。Gateway ingress `/v1/*` 被 readiness gate 保护，未 ready 返回 503；Admin `/admin/v1/*` 由独立 bearer auth 保护。

Kubernetes base：readiness 每 5s、timeout 3s、failure 6 次；liveness 每 10s、timeout 3s、failure 3 次。健康检查覆盖服务启动和 PostgreSQL/Redis 关键依赖；模型、Qdrant、COS、TencentDB 的调用状态由 runtime/operations/metrics 暴露。

收到 SIGINT/SIGTERM 后：

1. server 先 `MarkNotReady`，停止新 ingress。
2. Worker 停止 claim，取消辅助 heartbeat/migration/reply loops；已经 claim 的 Consumer 在 shutdown deadline 内排空。
3. HTTP server、Runner、reply、migration、adapter goroutine 在各自 context/timeout 下结束；Worker 释放 lease 前排空 Runner event channel。
4. deadline 到期仍未退出会返回可观测错误，不伪装成成功关闭。

`TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT` 默认 30s；production overlay 是 60s，Pod termination grace 是 75s。Compose grace 45s，应确保它不短于希望的应用 shutdown timeout。

## Secret、ConfigMap 和网络边界

`deploy/kubernetes/secret.example.yaml` 只描述固定 Secret 名 `trpc-agent-service-secrets` 的形状，明确要求 out-of-band 创建。Gateway/Worker 需要 PostgreSQL DSN、Redis URL；Gateway 还需要独立 Admin token。provider key 使用：

`TRPC_AGENT_SERVICE_SECRET_<tenant-id-hex>_<app-id-hex>_<secret-name-hex>_<version-hex>`。

这些值由环境 `SecretProvider` 按 scope 解析。Kubernetes Worker 的 `envFrom` 保持 scoped Provider keys 可用，但显式把 `TRPC_AGENT_SERVICE_ADMIN_TOKEN` 置空；不要把 Admin token 暴露给 Worker。ConfigMap 只放非敏感 stream/group/timeout/endpoint 和并发配置。实际外部 SecretProvider、KMS、rotation 流程由部署环境管理；相关密钥注入、轮换和日志边界已通过外部实测，仓库只记录接口与 scope 约束。

生产模型 base URL 要使用 HTTPS，并通过代码的 egress/IP policy；COS/Qdrant/TencentDB endpoint map 由 operator 维护，tenant config 只能选择逻辑 name，不可任意注入 URL。

## Schema migration 和发布

每个 `trpc-service` 角色在创建 PostgreSQL Store 后执行 `store.Migrate`。迁移在 `platform.schema_migration` 保存版本/name/SHA-256 checksum，并使用 PostgreSQL advisory lock；已应用 migration checksum 不匹配或数据库含未知版本时 fail closed。多个副本启动时依靠 advisory lock 串行。

因此发布必须遵循：migration 只追加、不可修改已应用 SQL；DDL 在滚动期间对旧/新二进制都兼容；先完成 schema，再使用新代码；删除字段要分多阶段。Backend data cutover 不是 SQL DDL，而是[数据迁移状态机](data-sync-idempotency.md#6-session-迁移)，成功验证后才切 immutable ConfigVersion。

## 灰度、回滚和实际边界

应用层灰度由 Admin API 的 ConfigVersion canary 完成：按稳定 Session principal/session hash 分流，支持 pause/disable/rollback/promote。Kubernetes overlay 使用普通 RollingUpdate 和 image tag/digest；代码镜像回滚与 Backend 数据回滚分开处理，target Backend 切换后按数据迁移策略完成 authority 对账。

部署 Workflow 覆盖可渲染清单、Compose golden path、一次 Worker restart 后继续服务和敏感证据扫描；外部实测进一步覆盖生产 Kubernetes admission、Ingress、secret 注入、Provider 网络、HA PostgreSQL/Redis/Qdrant 和发布恢复。多 Gateway 长连接仍按当前代码的部署约束运行：每个 Gateway 会枚举 active binding，部署时应保证 channel owner 不重复。
