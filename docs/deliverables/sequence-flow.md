# 核心时序

## IM 入站到回复

```text
Feishu / WeCom callback
→ verify signature and decrypt
→ Binding + identity mapping
→ Gateway Admission transaction
   ├─ check tenant/app/config and migration gate
   ├─ Inbox idempotency check
   ├─ allocate Session turn_seq
   ├─ write Execution
   └─ write Dispatch Outbox
→ ACK callback after commit
→ Relay publishes outbox to Redis Stream
→ Worker claims Execution and Lease
→ Redis Session Lock
→ Runtime.BuildRunner(exec)
→ Runner.Run(ctx, session principal, session, message)
→ drain every Runner event until channel closes
→ Runner.Close
→ Reply event becomes durable text Reply Outbox row
→ recheck Binding Revision and resolve/decrypt target
→ Feishu / WeCom send; transient failure retries
```

Gateway 是入站线性化点。相同 Binding、外部消息 ID 和 payload hash 的重复回调只
返回原 request；hash 冲突拒绝。队列只携带 tenant/app/request/config scope 和
ArtifactRef，不携带媒体 bytes 或 Secret 原文。

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
verified recall callback
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
