# 数据模型

所有平台协调记录都带 `tenant_id`、`app_id` 和必要的 Binding、Session 或
Config Version 作用域。配置版本不可变；Execution 在准入时固定版本，Worker
不得改用 active 的其他版本。

## 租户、应用与绑定

```text
tenant (tenant_id)
  └─ agent_app (tenant_id, app_id, active_config_version)
       └─ app_config_version (tenant_id, app_id, version)
       └─ channel_binding (tenant_id, app_id, binding_id, revision, status)
       └─ im_identity / channel_conversation
```

`Channel Binding` 保存通道凭据的 `SecretRef` 和当前 revision。长连接认证与 event
校验后，身份映射只在该 Binding 的 tenant/app 范围内产生 `user_id`、
`session_principal_id` 和 `session_id`。入站 payload 不能覆盖这些可信字段。

配置持久化字段：

| 记录 | 关键字段 | 说明 |
| --- | --- | --- |
| `tenant` | `tenant_id`、`name`、`status`、`audit_policy` | `audit_policy` 包含 `enabled`、`retention_days`、`redact_pii` |
| `app_config_version` | `tenant_id`、`app_id`、`version`、`model_config`、`tool_policy`、`backend_config`、`audit_policy` | immutable 配置版本；`audit_policy` JSON 同时保存 `im_access`、`budget`，治理策略随版本固定 |

## 准入、执行与可靠投递

| 记录 | 关键约束 |
| --- | --- |
| `channel_inbox` | unique(`tenant_id`, `app_id`, `binding_id`, `external_message_id`)，保存 payload hash 和 request id |
| `execution` | unique(`tenant_id`, `app_id`, `request_id`)，保存 immutable `config_version`、`turn_seq`、状态、attempt、lease owner/token/until |
| `dispatch_outbox` | 与 Execution 在同一准入事务创建；Relay 发布到 Redis Stream 后再推进状态 |
| `execution_event` | unique(`tenant_id`, `app_id`, `request_id`, `event_seq`)，供事件排空和 HTTP/RPC 结果读取 |
| `reply_outbox` | unique(`tenant_id`, `app_id`, `binding_id`, `request_id`, `source_event_id`, `revision`)；保存普通文本、目标引用和发送状态 |
| `channel_recall_inbox` | 按 Binding 和外部撤回事件去重，保存 verified recall 的结果 |

`execution` 的运行租约由 PostgreSQL 条件更新保护；失去租约的 Worker 不能
覆盖新运行者的状态。`turn_seq` 和 Redis Session Lock 保证同一 Session 的
Runner 不并发执行。

## Session、Memory 与 Knowledge

Session 的逻辑键为：

```text
tenant_id + app_id + session_principal_id + session_id
```

Session Provider 管理以下逻辑记录：

| 逻辑记录 | 核心字段 | 说明 |
| --- | --- | --- |
| `session` | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`state`、`last_event_seq`、`updated_at` | 会话主记录和 State |
| `session_event` | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`event_seq`、`request_id`、`user_id`、`event_type`、`payload`、`created_at` | append-only 会话事件 |
| `summary` | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`up_to_event_seq`、`content`、`model_version`、`updated_at` | 会话摘要；只能覆盖已提交 Event 范围 |

`summary.up_to_event_seq` 不得超过同一 Session 的 `last_event_seq`；三类记录都保持
相同 tenant/app/session 作用域。

Session Router 只路由实际存在的 PostgreSQL / Redis Provider，并由各自 owner
管理 service 生命周期。Memory Provider 通过 immutable BackendConfig 选择，
运行时使用同一 tenant/app/session scope。

Knowledge 只保留运行时检索所需记录：

| 记录 | 作用 |
| --- | --- |
| `knowledge_base` | tenant/app 下的知识库状态 |
| `knowledge_document` | 文档版本和可用状态 |
| `knowledge_chunk` | chunk 标识和 `index_generation` |

Qdrant 只做派生索引。SQL Catalog 在返回 chunk 前校验 tenant、app、配置版本、
knowledge base、document version 和可用状态；Runtime 再把同一 scoped Knowledge
接入 Runner。当前没有平台级 source importer 或 durable indexing job。

## Artifact

Artifact metadata 至少包含 tenant/app/session scope、object key、version、MIME、
size、checksum 和 status。对象路径不是授权依据：读取前先查 metadata，并校验
完整 scope 与版本。IM 媒体在 object storage 中落地，队列、Session Event 和日志
只传 `ArtifactRef`；Worker 在 Runtime 调用模型前按引用恢复 bytes。

对象写入成功但 metadata 写入失败时，调用方立即 best-effort 删除精确对象；删除
失败只记录错误，不引入 durable cleanup 状态机。

## Audit Log

Audit Log 是 tenant/app 作用域的逻辑追加记录。`AuditPolicy` 控制是否记录执行和
Tool 决策；写入失败只产生安全日志和指标，不破坏执行或回复，也不引入独立审计
Worker。OTEL trace 与 Audit Log 各自承担观测和合规记录职责。

| 逻辑记录 | 核心字段 | 说明 |
| --- | --- | --- |
| `audit_event` | `tenant_id`、`app_id`、`request_id`、`config_version`、`event_type`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`input_tokens`、`output_tokens`、`total_tokens`、`cost`、`trace_id`、`created_at` | metadata-only 审计事件；按 tenant/app 查询，禁止保存消息、Prompt、Tool arguments、Secret 和 Artifact bytes |

## Tool 与 Recall

Tool policy 是 immutable AppConfig 的一部分。Runtime 只暴露 catalog 中且被
`VisibleTools` 允许的工具；Worker 在实际调用前按 `ExecutableTools` 再做一次
allow/deny。该检查以执行时的 tenant/app/config scope 为准。

verified recall 在同一 tenant/app/binding 范围内定位原请求：未开始执行的
Execution 直接写 `CANCELED`；运行中的 Execution 写入 durable `CANCELED` 并清除
运行租约。Worker Prepare 读取该状态并停止；已经运行的节点依靠 context、Runner
cancel 和 Lease lost 做 best-effort 停止。
