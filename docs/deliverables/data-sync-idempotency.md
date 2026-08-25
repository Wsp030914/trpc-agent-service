# 数据同步、并发、幂等与迁移

## 1. 同一 Session 并发控制

多 Worker 场景下，必须防止两个 Worker 同时修改同一个 Session。

```text
消息 M1 -> Worker A
消息 M2 -> Worker B
```

如果两个 Worker 同时读取旧状态并覆盖写入，会造成 Event 乱序、State 丢失或 Summary 覆盖。

平台只选择一个权威并发机制：

- Queue 模式：按 `tenant_id/app_id/session_principal_id/session_id` 作为 partition key，同一 Session 顺序消费。
- SQL 模式：Worker 通过事务 claim `session_turn` 或 job 行，保证同一 session 同一时间只有一个执行者。

不建议同时叠加多套分布式锁。并发顺序应由队列分区或 SQL 事务保证。

## 2. Session、Event、State、Summary 顺序

Session 使用 append-only Event 模型。

```text
event_seq = 101 user_message
event_seq = 102 tool_call
event_seq = 103 tool_result
event_seq = 104 assistant_message
```

写入顺序：

1. Gateway 为消息生成或确认 `request_id`。
2. Worker claim 同一 session 的下一个 turn。
3. Runner 追加 user event、tool/model event、assistant event。
4. Event 持久化成功后更新 Session State 和 `last_event_seq`。
5. Summary 基于已提交的 event 异步生成，并记录 `up_to_event_seq`。
6. Worker 消费 Event Channel 到关闭，更新 execution 状态并写 Reply Outbox。

如果 Event 和 State 在同一 SQL 后端，优先放在一个事务中提交。如果跨后端写入，State 更新必须带 `event_seq`，支持幂等重放。

## 3. Memory 可见性

Memory 写入后先进入权威 Memory Store，再异步更新向量索引。

检索时可以同时查：

- 最近写入的权威 Memory 记录。
- 已完成索引的向量库记录。

这样即使向量索引延迟，也不会完全丢失刚写入的记忆。向量索引写入使用 `memory_id` 或 `source_event_id` 作为幂等键。

## 4. 入站、执行和出站幂等

IM 入站幂等：

```text
tenant_id + app_id + binding_id + external_message_id
```

HTTP/RPC 入站幂等：

```text
tenant_id + app_id + credential_id + client_idempotency_key
```

执行幂等：

```text
tenant_id + app_id + request_id
```

出站回复幂等：

```text
tenant_id + app_id + request_id + part_no
```

重复消息内容一致时返回原 `request_id` 或原执行状态；内容冲突时拒绝并写审计。

## 5. Redis 到 SQL 迁移

Redis Session 迁移到 SQL 时，SQL 应作为新权威后端。

流程：

1. 创建新 `backend_config`，指向 SQL Session 后端。
2. 快照复制 Redis 中的 Session/Event/State。
3. 记录迁移期间的增量写入。
4. 校验 session 数量、event 数量、`last_event_seq` 和 checksum。
5. 短暂停止该租户新 turn，回放增量写入。
6. 切换 active config version。
7. 保留 Redis 旧数据一段回滚窗口。

切换时不能把新请求同时写到新旧两个权威后端，避免冲突。需要双写时必须有明确 outbox 和校验机制。

## 6. 本地向量库到远端向量库迁移

向量库不是权威数据源。迁移时从 SQL/Object 中的 Knowledge metadata、文档原文和 Memory 记录重建索引。

流程：

1. 创建新向量库索引或 collection。
2. 从 SQL/Object 读取权威数据。
3. 使用相同 embedding 模型或记录新的 embedding version。
4. 批量构建新索引。
5. 校验文档数量、chunk 数量、索引 generation 和抽样召回。
6. 切换检索 alias 到新向量库。
7. 保留旧索引用于短期回滚。

检索请求必须带 `tenant_id`、Knowledge scope、ACL 和 index generation 过滤条件。
