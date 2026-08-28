# 数据同步、并发、幂等与迁移

## 1. 同一 Session 并发控制

同一 Session 的分区键为：

```text
tenant_id + app_id + session_principal_id + session_id
```

协调层以三种职责不同的机制协调多 Worker 执行：

- `turn_seq`：按分区分配，保证消息执行顺序。
- `lease_owner`、`run_token`、`lease_until`：保护 Execution claim、续租、完成和重投；过期 Worker 不能更新 Execution 状态。
- Redis Session Lease：覆盖 Runner 生命周期，避免正常情况下两个 Worker 同时执行同一 Session；不使用长事务。

本方案不要求 Session Provider 原子拒绝失效 Worker 的 Event、State 或 Summary 迟到写入，也不引入统一写入令牌。Lease 丢失后会取消旧 Runner 并继续排空其 Event Channel，`run_token` 仍保护 Execution、Execution Event Journal 和 Audit 的状态所有权；旧 Runner 的最后一次 Session 写入属于明确接受的残余风险。目标 Provider 若未来提供原子条件写入，可以作为可选增强接入，但不是自动故障接管的前置条件。

`run_token` 只保护 Execution 状态、Execution Event Journal 和 Audit 的条件更新；Dispatch Outbox 只保护 PostgreSQL 到 Redis 的可靠投递。审批 Tool Executor 作为治理扩展接入时使用 Execution 条件状态和 `approval_id` 幂等控制，不引入 Session 写入栅栏。

## 2. Session、Event、State、Summary 顺序

一个 turn 的持久化顺序为：

```text
user event -> model/tool event -> assistant event -> session state -> summary
```

Gateway 为消息生成或确认 `request_id` 并分配 `turn_seq`。Worker 重试沿用二者，不创建新的会话轮次。Summary 可以异步生成，但只能基于已提交 Event，并记录 `up_to_event_seq`。

Event 与 State 在同一 SQL 后端时优先同事务提交；跨后端时 State 必须带 `event_seq`，支持幂等重放。本方案不对 Session Provider 的跨后端迟到写入作额外原子拒绝承诺。

## 3. 入站、执行和出站幂等

IM 入站唯一键：

```text
tenant_id + app_id + binding_id + external_message_id
```

HTTP/RPC 入站唯一键：

```text
tenant_id + app_id + authenticated_principal_id + client_idempotency_key
```

执行和回复分别使用：

```text
tenant_id + app_id + request_id
tenant_id + app_id + request_id + part_no
```

Channel Adapter 验签、解密和标准化后，Gateway 在一个事务中写入 Inbox、Execution 和 Dispatch Outbox；提交成功后才 ACK 外部 IM。重复回调命中已有 Inbox 时不创建 Execution，直接 ACK。Runner 完成后，Worker 以当前 `lease_owner`、未过期 `lease_until` 和 `run_token` 条件更新 Execution 终态并写必要 Audit；条件失败则不覆盖新运行者的状态。IM Reply Outbox 属于实际 IM Adapter 接入时的后续实现。

## 4. 后端迁移

后端自身的 Schema、keyspace 和索引准备由各适配器负责；跨后端数据迁移由独立编排层在维护窗口内执行。模型、Tool Policy 和审计策略可以普通发布；Session、Memory、Knowledge、Artifact 的权威 `backend_config` 变更只能作为迁移目标，不能直接激活。迁移记录固定源和目标 `config_version`，过程为：

```text
ACTIVE -> MIGRATING -> ACTIVE
                  \-> FAILED -> ACTIVE（保留旧配置）
```

1. App 进入 `MIGRATING`：Gateway 拒绝该 App 的新请求；HTTP/RPC 返回带 `Retry-After` 的可重试错误。IM Adapter 完成验签后 ACK，但不创建 Execution，并按通道能力发送维护提示；平台不延后执行该输入，也不依赖外部 IM 长期重投。
2. Worker 继续执行已接受的 Job，直到队列排空且没有 Session 写入者。
3. 编排层从旧后端执行全量复制，校验记录数、稳定 ID、事件顺序及必要的校验和。
4. 校验成功后，原子切换 active 后端配置并恢复 `ACTIVE`；失败时保持旧配置并恢复 `ACTIVE`。

迁移期间只有旧后端是权威写入端，不做在线增量同步、长期双写或延后输入队列。向量索引始终由权威 Memory/Knowledge 原始记录重建，不作为迁移唯一来源。

## 5. Memory 与派生索引

生产 Worker 不以本地内存保存权威 Memory。普通 Memory 写入由共享权威后端确认；向量检索只在需要对大量非结构化记忆进行语义召回时启用。向量索引可以异步更新，因此不承诺刚写入内容立刻被语义检索到；检索可补查最近的权威记录。

Knowledge 原文和 Artifact 保存在对象存储，SQL metadata 是权限入口；向量库只保存可重建的 Knowledge 或可选 Memory 索引。所有 SQL 查询、Redis key、对象路径和向量过滤均包含 `tenant_id + app_id`，个人 Memory 额外包含 `user_id`。
