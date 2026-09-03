# 需求收缩式清理报告

本轮只保留前三个需求的最小可信链路：多租户 Agent 执行、多后端配置选择，以及飞书/企业微信收发。删除判断均以删除后是否仍满足核心不变量为准。

## 生产代码统计

按仓库 `HEAD`（清理前代码）和当前工作区非测试 Go 物理行统计；能力行按相关生产文件归类，存在跨层共享文件，因此不相加作为总量：

| 能力 | 修改内容 | Production LOC Before | After | 减少 |
| -- | -- | --: | --: | --: |
| Knowledge | 删除 importer、SourceStore、IndexJob 状态/租约/重试、后台索引循环和 source metadata；保留 Qdrant scoped retrieval | 1,758 | 812 | 946 |
| Artifact | 删除 durable cleanup record/claim/retry/pass；保留对象、metadata、ArtifactRef 和即时补偿 | 2,259 | 1,873 | 386 |
| Runtime | 删除 resolver contract 和转发层；改为具体 provider 组装、每次执行构建 fresh Runner | 1,670 | 1,052 | 618 |
| Worker | 删除 Hook、functional option 和高频取消检查；保留 lease、session lock、event drain | 1,702 | 1,038 | 664 |
| IM Reply | 删除 SEND/UPDATE/FINALIZE、multipart、card、fallback 和 capability 重复层；保留 text Reply Outbox/retry | 2,685 | 1,982 | 703 |
| Recall | 删除 cancel-requested 生命周期和重复 polling；保留 verified recall → SQL CANCELED | 447 | 373 | 74 |
| Tool Policy | 删除参数级 authorizer/admin passthrough/Knowledge Tool；保留 catalog visibility + execution allow/deny | 1,217 | 862 | 355 |

另删除了无生产调用的 Consumer functional options，并移除 Knowledge 对 COS source object 的旧 Admin 约束。

```text
Production Go LOC
Before: 24,647
After: 20,746
Removed: 3,901
Reduction: 15.8%
```

## 删除反证

| 删除项 | 当前调用链 | 删除后调用链 | 保留的不变量 |
| -- | -- | -- | -- |
| Knowledge importer/indexing | `source → SourceStore → Document/Chunk → IndexJob → Qdrant` | `AppConfig BackendRef → Qdrant Provider → scoped Knowledge → Runner` | tenant/app/config/knowledge-base scope；配置版本固定；运行主链可检索 |
| Artifact durable cleanup | `metadata failure → CleanupRecord → claim/renew/retry → object delete` | `metadata failure → best-effort exact object delete；失败记录错误` | metadata 授权、版本约束、tenant/app/session 隔离；主链不依赖 cleanup worker |
| Runtime resolver wrappers | `Worker → interface resolver → one concrete provider → Runner` | `Worker → Runtime concrete assembly → provider → Runner` | config pinning、SecretRef、Provider lifecycle、fresh Runner |
| Worker hooks/options | `New → Option → hook field → execution` | `New(config, runner, locker, eventSink, cancellation) → execution` | Session serialization、Execution Lease、event drain、context cancellation |
| Reply state machine | `event → accumulate/part/update/card/fallback → resolver → provider` | `text event → Reply Outbox → binding revision/target → provider → transient retry` | binding protection、outbox idempotency、Feishu/WeCom send |
| Recall lifecycle | `verified recall → cancel_requested → polling → managed cancel` | `verified recall → SQL CANCELED；Prepare stop；Lease loss/context best-effort cancel` | verified identity、durable canceled state、Execution Lease |
| Tool duplicate paths | `admin validator → runtime validator → catalog hooks → worker authorizer hook` | `AppConfig names → catalog visibility → Worker execution allow/deny` | tenant policy isolation、tool visibility、execution denial |

## 保留原因

- PostgreSQL / Redis Session Provider、Session service lifecycle 和 Memory Provider 仍由各自具体 owner 管理；它们是真实共享资源，不是转发层。
- Qdrant Provider 仍在 SQL 可用性、tenant/app/config/knowledge-base scope 下检索；没有 importer 或 durable indexing job 也不影响配置后端到 Runtime 的检索链。
- Artifact metadata 是授权入口，object key/version 由服务端生成；对象写入和 metadata 写入失败之间保留即时 compensation。清理 orphan object 不是前三需求的读取或主链正确性前置条件。
- SecretProvider 仍是运行时凭据安全边界；Feishu/WeCom 验签、解密和 Binding Revision 复核仍在 Adapter/Outbound 层。
- Inbox、Dispatch Outbox、Redis Stream、Execution Lease、Session Lock、Reply Outbox、Runner event drain 和 Context cancellation 均保留。

## 最终实际主链

```text
Tenant / App / immutable Config Version
→ Binding / Identity Mapping
→ Gateway Admission
→ Inbox
→ Dispatch Outbox
→ Relay / Redis Stream
→ Worker
→ Execution Lease
→ Session Lock
→ fresh Runner
→ Run / event drain / Close
→ Reply Outbox
→ Binding Revision + target resolve/decrypt
→ Feishu / WeCom text send + transient retry
```
