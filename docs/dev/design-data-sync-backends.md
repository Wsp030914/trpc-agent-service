# 数据同步与多后端支持详细设计

> 开发用设计草稿，不作为题目交付物。正式交付物以 `docs/deliverables/` 为准。

## 1. 设计范围

本专题只解决租户级后端路由、各类数据的存储边界、跨节点可见性、Session 顺序、数据迁移和 IM 幂等。

平台不实现一个覆盖 SQL、Redis、向量库和对象存储的通用 CRUD 接口，也不承诺跨后端全局事务。继续复用 tRPC-Agent-Go 对 Session、Memory、Knowledge 和 Artifact 的类型化接口，平台只负责作用域、配置解析和编排。

## 2. 后端选择与路由

后端配置属于版本化的 Agent App 配置。一个租户可以有多个 Agent App，各 App 可以选择不同的后端组合：

```text
backend_config
  session
  memory
  knowledge
  artifact
```

Gateway 根据可信的 `tenant_id + app_id` 读取当前配置版本；Job 固定记录 `config_version`；Worker 根据 Job 装配对应的 tRPC-Agent-Go Provider。连接信息和访问凭据仅以 `secret_ref` 保存，不写入普通配置或日志。

普通配置发布可以变更模型、工具和审计策略。改变某类数据的 `backend_config` 属于受控数据迁移，不能在活跃数据上通过一次普通配置发布直接切换。

## 3. 数据存储边界

| 数据 | 权威存储或职责 |
| --- | --- |
| Tenant、AppConfig、Channel Binding、Job、Inbox、Execution | 平台 PostgreSQL |
| Session、Event、State、Summary | App 选择的共享 tRPC-Agent-Go Session 后端，如 PostgreSQL、MySQL 或 Redis |
| Memory | App 选择的共享 Memory 后端或外部 Memory 服务 |
| Knowledge 原文、附件、Artifact | 对象存储；metadata 存 SQL |
| Knowledge / 可选语义 Memory 检索 | 向量库 |
| Audit Log | 平台 SQL 权威存储 |

向量库不是 Memory 的默认或必需后端。结构化偏好、权限和固定事实适合 SQL、Redis 或外部 Memory 服务；只有需要从大量非结构化个人记忆中按语义召回时，才启用可选的语义 Memory 检索。Knowledge 文档检索是向量库的主要使用场景。

所有 SQL 查询、Redis key、对象路径和向量过滤都包含 `tenant_id + app_id`；个人 Memory 额外包含 `user_id`。

## 4. Session 顺序与跨节点执行

Session 分区键为：

```text
tenant_id + app_id + session_principal_id + session_id
```

Gateway 为同一分区分配递增 `turn_seq`。队列保证同一分区同一时间只有一个 Job 执行，不同 Session 可并行，因此 Worker 不需要 sticky session。

一个 turn 内的持久化顺序为：

```text
用户消息 Event -> 模型/Tool Event -> 助手消息 Event -> Session State -> Summary
```

Summary 可以异步生成，但只能基于已提交 Event，并保存已处理到的 `event_seq`，不能覆盖更晚的内容。Job 重试沿用同一个 `request_id` 与 `turn_seq`，不生成新的会话轮次。

本设计不把整个 Runner 生命周期放入长事务。自动故障接管以 Redis Session Lease 取消旧 Runner，并以 PostgreSQL `run_token` 保护 Execution、Execution Event Journal 和 Audit 的状态更新；不要求权威 Session Provider 原子拒绝旧执行者对 Event、State 和 Summary 的迟到写入，也不引入递增写入令牌。该窗口属于明确接受的残余风险。审批 Tool Executor 通过 Execution 条件状态和 `approval_id` 幂等控制执行，不依赖额外的 Session 写入令牌。支持原子条件写的 Provider 可以后续作为可选增强接入，但不是生产自动接管的前置条件。

## 5. Memory 跨节点可见性

生产 Worker 不以本地内存保存权威 Memory。Memory 写入以共享后端确认持久化为成功条件；后续任意 Worker 读取同一 Provider 都可以看到该记录，无需节点间广播。

若启用向量检索，Memory 原始记录写入共享后端后可读取；向量索引作为检索加速层可以异步更新，因此不承诺新记录立刻被语义召回。

## 6. 后端迁移

迁移由权威 `data_migration` 记录固定源和目标 `config_version`，不修改迁移中的版本。迁移使用 App 维护窗口：

```text
ACTIVE -> MIGRATING -> ACTIVE
                  \-> FAILED -> ACTIVE（保留旧配置）
```

1. App 进入 `MIGRATING`：Gateway 暂停该 App 新请求；HTTP/RPC 返回带 `Retry-After` 的可重试错误。IM Adapter 在验签后 ACK，但不创建 Execution，并按通道能力发送维护提示；平台不延后执行迁移窗口内的输入，也不依赖通道长期重投。
2. Worker 继续排空已接受的 Job，直到没有 Session 写入者。
3. 编排层全量复制旧后端数据，校验记录数、稳定 ID、Event 顺序和必要的校验和。
4. 校验成功后，原子切换后端配置并恢复 `ACTIVE`；失败时保持旧配置并恢复 `ACTIVE`。

迁移期间只有旧后端是权威写入端，不做增量同步、长期双写或 `DEFERRED` 输入队列。Session 和 Memory 保留原有 ID、租户/App 作用域、Event 顺序和更新时间；向量库从 Knowledge 或 Memory 原始记录重建索引；Artifact 与 Knowledge 原文复制后校验大小或校验和。

## 7. IM 幂等与一致性取舍

入站 IM 消息使用以下唯一键去重：

```text
tenant_id + app_id + binding_id + external_message_id
```

Channel Adapter 完成验签后，Gateway 在同一准入事务中创建 Inbox/请求记录和 Job。重复回调返回已有 `request_id` 或 ACK，不再创建新 Job。Runner 完成后，Worker 在平台 PostgreSQL 的同一条件事务中写 Job/Execution 终态、IM Reply Outbox 和必要 Audit；Outbox 发送本身仍是至少一次。

| 数据或链路 | 一致性语义 |
| --- | --- |
| 配置、Inbox、Job、Audit | PostgreSQL 事务，强一致 |
| 同一 Session 消息 | 队列按 `turn_seq` 串行 |
| Event、State、Summary | Event 先于 State/Summary 的有序持久化 |
| 普通 Memory | 共享后端确认写入后跨节点可读 |
| 向量和 Knowledge 索引 | 最终一致，允许索引延迟 |
| IM 回复 | Outbox 至少一次发送尝试，失败重试 |

IM 回复不能对不支持幂等键的外部平台承诺恰好一次。外部平台已接收回复、但平台在标记 Outbox 已发送前崩溃时，重试可能造成重复；不重试则可能丢失。支持幂等键的平台使用 `request_id + part_no` 作为发送键。
