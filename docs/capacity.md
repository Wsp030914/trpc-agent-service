# 容量评估

状态：**已有一轮本地 disposable Capacity smoke 实测，但尚无生产容量结论。** 本文中的 `30%～50%` 仅是规划余量建议，绝不是本项目的实测吞吐承诺。

## 测试原则

正式容量结论只测与生产等价的版本、配置和外部依赖。GitHub Actions 的 Capacity
Workflow 是自包含 disposable smoke/capacity 检查，使用本地 Compose 和 deterministic
model；它的结果不能标成生产容量，也不依赖公网 endpoint 或真实 provider secret。

固定的端到端工作负载为：IM/HTTP Admission、Inbox 去重、Redis Stream、Worker、模型、至少一种 Session 和 Reply Outbox；另分别加入一个 Tool 成功、一个 Tool 失败、一个模型 timeout、一个 IM retry。压测应从低于目标峰值开始，逐级提高并保持足够长的稳态窗口，再执行故障恢复场景。

## 最近一次本地实测

以下记录来自 2026-09-05 的一次本地固定批次验证。它用于证明 Capacity Runner、一次性认证和
Gateway → Redis → Worker → Reply 链路可运行，不是生产压测、容量上限或安全并发结论。测试期间
未使用当前环境的 HTTP 代理变量，也没有记录 API key。报告由
`scripts/capacity-observe.py` 在不改变 `cmd/capacity-evaluate` 压测算法的前提下补充基础设施
观测结果；本地和 GitHub Actions 共用该入口。

| 项目 | 实测值 |
| --- | --- |
| 代码与镜像 | Git SHA `2279574d3c2b1eaec895a41d279376fcc9a0ad44`；Docker Desktop disposable Compose；Gateway + 2 个 deterministic Worker |
| 依赖 | PostgreSQL 16-alpine、Redis 7-alpine、Qdrant v1.16.0 |
| workload | OpenAI-compatible chat completions；固定 payload；100 requests；concurrency 4；fake/deterministic model |
| 认证 | disposable tenant/app + 临时 API credential；credential 未写入报告 |
| 请求结果 | total 100；success 100；failure 0 |
| error rate | `0`；允许上限 `0.05` |
| throughput | `3.7692 req/s` |
| 端到端延迟 | min `811.42 ms`；p50 `1012.19 ms`；p95 `1212.73 ms`；p99 `2034.89 ms`；max `2049.55 ms` |
| success criteria / exit code | `true` / `0` |
| Runtime 采样 | 压测期间 7 次；配置间隔 `2 s`；实际平均间隔 `4.37 s`（本地 Docker Desktop 调用开销） |
| Gateway CPU / Memory peak | `4.47%` / `11,534,336 bytes` |
| Worker-1 CPU / Memory peak | `5.06%` / `10,527,703 bytes` |
| Worker-2 CPU / Memory peak | `2.40%` / `13,537,116 bytes` |
| Worker overall peak | CPU `5.06%`；Memory `13,537,116 bytes` |
| Redis queue | pending peak `0`；lag peak `0`；final pending `0`；final lag `0`；final consumers `2` |
| Redis command window | total delta `2,177`；平均 `68.7160 commands/s` |
| Redis Stream window | `XADD=102`；`XREADGROUP=708`；`XACK=102`；`XAUTOCLAIM=706` |
| Redis Session/Lock window | `SET=102`；`GET=102`；`DEL=102`；`EVAL=1`；`EVALSHA=102` |
| PostgreSQL window | transactions `3,960`；commits `3,958`；rollbacks `2`；writes `2,407` |
| PostgreSQL operation window | insert `1,477`；update `930`；delete `0`；平均 transactions `124.9955/s`；writes `75.9758/s` |

本次结果说明该本地拓扑在固定的 100/4 批次下无请求失败，且观测器能从真实容器、Redis Consumer
Group 和 `pg_stat_database` 取得窗口数据。它仍不能据此填写“每 Worker 安全并发”或生产容量结论：
GitHub Runner 的 CPU/Memory 有环境波动，PostgreSQL/Redis/Qdrant 自身 CPU/Memory、执行延迟分解、
token、真实模型延迟、provider throttling 和生产级 IM 指标均为 `unavailable`/未覆盖。每次 GitHub
手动运行的 JSON 会带 Git SHA、run id、输入参数和测试时间，并写入 Job Summary；成功时上传精简报告，
失败时上传诊断日志。

## 必填记录

| 项目 | 本次实测值 | 采集位置 |
| --- | --- | --- |
| 测试环境（CPU、内存、节点数、镜像、PostgreSQL/Redis/Qdrant 版本） | 待测 | 压测报告头部 |
| workload（IM/HTTP 比例、消息大小、会话热点分布、Tool 比例） | 待测 | 压测脚本/记录 |
| Gateway IM event rate、HTTP RPS、Admission p95 | 待测 | Provider client/Ingress/OTel |
| IM peak event rate、reply RPS、provider throttling、retry rate | 待测 | `im.callback` / `im.reply` 指标及 provider 日志 |
| 每 Worker 并发 Execution、每 Session 并发、CPU、Memory | 待测 | Pod/进程指标、运行执行数 |
| Execution latency p50/p95/p99、error rate、queue lag | 待测 | Trace/OTel、Redis `XINFO GROUPS` / `XPENDING` |
| 平均 input tokens、output tokens、tokens/execution、模型 p95、模型 error rate | 待测 | `trpc_agent_service.model.*` |
| Redis publish/claim/ack QPS、Session Lock QPS、Redis Session QPS | Capacity observer 已提供 Redis 总命令及可识别 Stream/Session/Lock 命令窗口增量；无法分类时为 `unavailable` | Redis `INFO stats` / `INFO commandstats` |
| PostgreSQL Admission、Execution update、Inbox/Outbox、Audit、PostgreSQL Session QPS | Capacity observer 已提供 `pg_stat_database` 事务、insert/update/delete 窗口增量及平均速率；语句级分类为 `unavailable` | PostgreSQL `pg_stat_database` |

`trpc_agent_service.model.input_tokens`、`model.output_tokens`、`model.latency`、`im.callback.count`、`im.reply.*`、`session.*`、`memory.*` 已由服务产生。Redis queue lag 与 PostgreSQL/Redis QPS 应由现有数据库/缓存 exporter 或只读统计查询采集；不为此项目新建压测平台或运维平台。

## 可执行入口

仓库内的 `cmd/capacity-evaluate` 对当前 Gateway 的 OpenAI-compatible endpoint
做真实 HTTP 并发测量。Capacity Workflow 会先用 `cmd/capacity-prepare` 创建 disposable
租户和一次性 API key，再通过 Compose 中的 deterministic Worker 测量内部链路；生产容量
仍需替换为真实版本和外部依赖后另行记录。`scripts/capacity-observe.py` 负责基线、周期采样、
最终快照和报告合并。示例：

```powershell
python scripts/capacity-observe.py `
  --report data/capacity/capacity-local.json `
  --sample-interval 2 `
  --compose-file compose.yaml `
  --compose-file compose.deployment-e2e.yaml `
  --workload-type http_chat_completions `
  --model-type deterministic `
  -- `
  go run ./cmd/capacity-evaluate `
    -endpoint http://127.0.0.1:8080/v1/chat/completions `
    -api-key "$env:TRPC_AGENT_SERVICE_CAPACITY_API_KEY" `
    -concurrency 4 `
    -requests 100
```

输出 JSON 包含请求总数、成功/失败、错误率、允许错误率、成功判定、吞吐量和
`min/p50/p95/p99/max` 延迟；错误率超过 `-max-error-rate` 或没有完成请求时命令返回
非零。观测器在压测前后读取 Redis/PostgreSQL 计数器，压测期间采集容器 CPU/Memory 与
Redis Consumer Group pending/lag，分别记录累积增量、峰值和最终值。可用
`-duration 5m -requests 0` 做时间窗口压测；`-payload-file` 用于固定生产等价
请求。命令不会伪造 queue lag、CPU、Memory 或 QPS；无法可靠读取的字段统一写为
`unavailable`。

## 可复现实测记录

每一次正式运行复制以下表格到压测记录，所有“设计值”和“测量值”必须分开：

| 字段 | 值 |
| --- | --- |
| 日期、执行人、Git SHA、镜像 digest | 待填 |
| 环境与拓扑 | 待填 |
| 测试持续时间与并发阶梯 | 待填 |
| 平均 input/output tokens、tokens/execution | 待填 |
| Worker 并发/Session 热点规则 | 待填 |
| p50/p95/p99 Execution latency | 待填 |
| Model p95 / error rate | 待填 |
| 端到端 error rate | 待填 |
| Gateway/Worker CPU 和 Memory 峰值 | 待填 |
| Redis publish/claim/ack/lock/session QPS | 待填 |
| PostgreSQL Admission/Execution/Inbox/Outbox/Audit/Session QPS | 待填 |
| IM peak event/reply RPS、限流与 retry rate | 待填 |
| queue lag 峰值与恢复时间 | 待填 |
| 结论：已测安全并发（每 Worker） | 待填 |

## 规划计算

`measured_safe_concurrency_per_worker` 必须来自上表中满足 SLO、无明显 queue lag 累积、错误率可接受且 CPU/Memory 未饱和的实测阶梯。

```text
required_workers
= ceil(peak_concurrent_executions / measured_safe_concurrency_per_worker * headroom)
```

`headroom` 建议取 `1.3～1.5`，以覆盖流量突刺、一个节点失效和模型延迟波动。示例中的变量仅是公式，不代入虚构数字。Gateway 根据 IM event/HTTP RPS 与 Admission p95 估算；Worker 根据 queue lag、running executions 和 CPU/Memory 共同扩缩，不能只看 CPU。

## 验收

容量结论只有在上述字段有可追溯测量值后才可写入。若外部模型或 IM provider 无法在压测窗口提供相同配额，应明确记录该限制，并将结果标为“依赖受限”，而不是外推为生产数字。
