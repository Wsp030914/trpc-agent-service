# 容量评估

状态：**容量模板，尚无压测实测数字。** 本文中的 `30%～50%` 仅是规划余量建议，绝不是本项目的实测吞吐承诺。

## 测试原则

只测与生产等价的版本、配置和外部依赖。每次结果须同时保留 Git SHA、镜像 digest、AppConfig 版本、模型 provider/model、区域和时间窗口。不要把开发机或模拟模型结果标成生产容量。

固定的端到端工作负载为：IM/HTTP Admission、Inbox 去重、Redis Stream、Worker、模型、至少一种 Session 和 Reply Outbox；另分别加入一个 Tool 成功、一个 Tool 失败、一个模型 timeout、一个 IM retry。压测应从低于目标峰值开始，逐级提高并保持足够长的稳态窗口，再执行故障恢复场景。

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
| Redis publish/claim/ack QPS、Session Lock QPS、Redis Session QPS | 待测 | Redis INFO/命令统计或 Redis exporter |
| PostgreSQL Admission、Execution update、Inbox/Outbox、Audit、PostgreSQL Session QPS | 待测 | PostgreSQL exporter / `pg_stat_statements` |

`trpc_agent_service.model.input_tokens`、`model.output_tokens`、`model.latency`、`im.callback.count`、`im.reply.*`、`session.*`、`memory.*` 已由服务产生。Redis queue lag 与 PostgreSQL/Redis QPS 应由现有数据库/缓存 exporter 或只读统计查询采集；不为此项目新建压测平台或运维平台。

## 可执行入口

仓库内的 `cmd/capacity-evaluate` 对当前 Gateway 的 OpenAI-compatible endpoint
做真实 HTTP 并发测量，不调用模型模拟器，也不把结果写入源码。示例：

```powershell
go run ./cmd/capacity-evaluate `
  -endpoint http://127.0.0.1:8080/v1/chat/completions `
  -api-key "$env:TRPC_AGENT_SERVICE_CAPACITY_API_KEY" `
  -concurrency 4 `
  -requests 100
```

输出 JSON 包含请求总数、成功/失败、吞吐量和 `min/p50/p95/p99/max` 延迟。可用
`-duration 5m -requests 0` 做时间窗口压测；`-payload-file` 用于固定生产等价
请求。命令不会伪造 queue lag、CPU、Memory 或 QPS，未接入观测后端的数据保持
缺失，基础设施指标按上表另行采集。

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
