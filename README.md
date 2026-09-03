# tRPC Agent Service

这是一个基于 tRPC-Agent-Go 的最小多租户 Agent 服务实现，覆盖：

- tenant/app/config version 隔离和不可变配置；
- Channel Binding、身份映射、Gateway Admission 和 Inbox 幂等；
- PostgreSQL Dispatch Outbox → Redis Stream → 多节点 Worker；
- Execution Lease、Redis Session Lock、PostgreSQL / Redis Session Provider；
- Memory Provider、Qdrant Knowledge scoped retrieval；
- ArtifactRef 媒体引用链和 object storage metadata 授权；
- SecretRef、Feishu/WeCom 验签解密、Binding Revision 和普通文本 Reply Outbox；
- verified Recall、基础限流、transient retry、Runner event drain 和 Context cancellation。

## 核心执行链

```text
Tenant / App / immutable Config Version
→ Binding / Identity Mapping
→ Gateway Admission
→ Inbox
→ Dispatch Outbox
→ Relay / Redis Stream
→ Worker / Execution Lease
→ Session Lock
→ Runtime.BuildRunner(exec)
→ fresh Runner / event drain / Close
→ Reply Outbox
→ Feishu / WeCom
```

## 目录

```text
cmd/trpc-service       服务入口和 composition root
trpcservice/gateway   可信身份、准入和配置版本固定
trpcservice/postgres  平台协调记录、Inbox/Outbox、metadata
trpcservice/worker    Execution、Lease、Runner event drain、Reply sender
trpcservice/runtime   concrete model/provider/Tool 组装
trpcservice/session   PostgreSQL/Redis Session Router
trpcservice/knowledge Qdrant Provider 和 scoped retrieval
trpcservice/artifact  object storage、metadata 和 ArtifactRef
trpcservice/channels  Feishu/WeCom adapter、outbound、media
trpcservice/secret    scoped SecretProvider
```

## 文档

- [系统架构](docs/deliverables/system-architecture.md)
- [架构设计](docs/deliverables/architecture-design.md)
- [核心时序](docs/deliverables/sequence-flow.md)
- [数据模型](docs/deliverables/data-model.md)
- [多后端适配](docs/deliverables/backend-adapter.md)
- [幂等与同步](docs/deliverables/data-sync-idempotency.md)
- [风险清单](docs/deliverables/risk-list.md)
- [需求收缩式清理报告](docs/overdesign-cleanup-report.md)

## 本地验证

```bash
go build ./...
go test ./...
go vet ./...
git diff --check
```
