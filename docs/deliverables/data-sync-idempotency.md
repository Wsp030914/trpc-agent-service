# 数据同步、并发与幂等

## Session 并发

```text
tenant_id + app_id + session_principal_id + session_id
```

PostgreSQL `turn_seq` 保证同一 Session 的入站顺序；Execution 的
`lease_owner/run_token/lease_until` 保护运行状态所有权；Redis Session Lock 覆盖
Runner 生命周期。不同 Session 可由不同 Worker 并行执行。

Lease 丢失或普通 Context cancellation 时，Worker 取消本地 Runner，但继续排空已
产生的 event channel。失效 Worker 的 Execution 终态更新会被 run token 条件拒绝。

## 入站幂等

IM 使用：

```text
tenant_id + app_id + binding_id + external_message_id
```

Gateway 在事务内校验 payload hash；相同 payload 返回原 request，冲突拒绝。Inbox、
Execution 和 Dispatch Outbox 同事务提交，成功提交前不确认外部 callback。

HTTP/RPC 使用可信身份和 client idempotency key；请求一旦固定 config version，重试不
创建新的 Session turn。

## 出站幂等

Runner event 由 journal/projection 按 event sequence 推进为普通文本 Reply。Reply
Outbox 以 tenant/app/binding/request/source event/revision 去重，并由发送租约保护
多节点发送。发送前重新解析当前 Binding Revision 和目标；transient provider error
进入有限重试，永久错误保留失败状态。

## Recall

```text
verified recall
→ lock Binding and locate original request
→ status = CANCELED
→ Prepare stops not-yet-running execution
→ running node stops best-effort through Lease loss / Context / Runner cancel
```

Recall 是幂等的，不删除 Inbox、Session、Memory、Artifact 或已发送回复。

## 后端切换

Session、Memory、Knowledge、Artifact 的选择来自 immutable BackendConfig。data
migration gate 在切换前停止新准入、排空已接受执行、复制并校验目标数据；切换只通过
新的 config version 生效，旧 Execution 继续使用其 pinned version。

Knowledge 运行时只走 Qdrant Provider + SQL-scoped retrieval；Artifact 运行时只走
object + metadata + ArtifactRef。两者没有额外的导入任务或 cleanup recovery loop。
