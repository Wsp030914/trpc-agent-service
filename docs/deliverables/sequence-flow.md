# 核心时序

## IM 入站到回复

```text
Feishu official WebSocket / WeCom AI Bot WebSocket event
→ authenticate the binding-scoped client and validate event identity
→ Binding + identity mapping
→ Gateway Admission transaction
   ├─ check tenant/app/config and migration gate
   ├─ Inbox idempotency check
   ├─ allocate Session turn_seq
   ├─ write Execution
   └─ write Dispatch Outbox
→ finish provider event handling after commit
→ Relay publishes outbox to Redis Stream
→ Worker claims Execution and Lease
→ Redis Session Lock
→ Runtime.BuildRunner(exec)
→ Runner.Run(ctx, session principal, session, message)
→ drain every Runner event until channel closes
→ Runner.Close
→ Reply event becomes durable text Reply Outbox row
→ recheck Binding Revision and resolve target
→ Feishu / WeCom send; transient failure retries
```

当前 IM 出站只承诺普通异步文本。stream/card 属于扩展能力，不改变当前 Reply
Outbox、Binding Revision 和 transient retry 主链。

Gateway 是入站线性化点。相同 Binding、外部消息 ID 和 payload hash 的重复事件只
返回原 request；hash 冲突拒绝。队列只携带 tenant/app/request/config scope、
W3C traceparent/tracestate 和 ArtifactRef，不携带媒体 bytes 或 Secret 原文。

Gateway span 的 W3C context 持久化在 Execution，Worker 从 Execution 恢复后创建
子 span；Reply Outbox 读取同一 context，因此 IM event → Gateway → Worker →
Runner/Tool → Reply 属于同一条 trace。Metrics 只使用 tenant/app/channel/provider/
operation/result/error_type 等固定低基数标签。

## HTTP/RPC 排队执行

```text
authenticated HTTP/RPC request
→ trusted tenant/app/user or service principal
→ QueuedRunner
→ Gateway Admission
→ Inbox/Execution/Dispatch Outbox
→ execution event journal / reply projection
→ Worker execution path above
```

协议入口不直接创建真实 Runner。连接取消通过 Context 传播；Execution Lease 丢失
也会取消本地运行并排空已经产生的 event channel。

## Recall

```text
verified provider recall event
→ lock Binding and locate original Inbox/Execution
→ SQL status = CANCELED
   ├─ PENDING: Worker Prepare rejects it
   └─ RUNNING: clear run lease; local lease renewal cancels context
→ duplicate recall returns the stored result
```

Recall 不删除 Inbox、Session、Memory、Artifact 或已经生成的 Reply。

## 配置与后端切换

```text
publish immutable AppConfig version
→ new Admission pins new version
→ existing Execution keeps old version

backend migration gate
→ stop new admission
→ drain accepted executions
→ copy and verify selected provider data
→ activate new config version
```
