# 数据模型设计

核心模型必须表达 Tenant、Agent、Channel Binding、Session、Event、Memory、Summary 和 Audit Log 的关系。实际实现可以拆表或合表，但主键、唯一键和查询条件必须包含租户作用域。

## 1. 租户与应用

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| tenant | `tenant_id`、`name`、`status`、`audit_policy`、`created_at` | 最高隔离边界 |
| agent_app | `tenant_id`、`app_id`、`name`、`active_config_version`、`status` | 租户下的 Agent 应用 |
| app_config_version | `tenant_id`、`app_id`、`version`、`model_config`、`tool_policy`、`backend_config`、`audit_policy`、`status` | 不可变配置版本 |

关系：

```text
tenant 1 -> N agent_app
agent_app 1 -> N app_config_version
agent_app.active_config_version -> app_config_version.version
```

## 2. IM 绑定与身份

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| channel_binding | `tenant_id`、`app_id`、`binding_id`、`channel`、`external_account`、`webhook_url`、`token_ref`、`signing_secret_ref`、`secret_ref`、`status` | IM 外部账号和租户应用的绑定 |
| im_identity | `tenant_id`、`app_id`、`binding_id`、`channel`、`external_user_key_hash`、`user_id`、`display_name_enc`、`status`、`key_version` | 外部用户到平台真实发言人的映射 |
| im_conversation | `tenant_id`、`app_id`、`binding_id`、`channel`、`external_chat_key_hash`、`thread_key_hash`、`conversation_id`、`session_principal_id`、`scope` | 外部群、频道、thread 到会话主体的映射 |
| im_membership | `tenant_id`、`app_id`、`conversation_id`、`user_id`、`role`、`member_status`、`last_seen_at` | 群成员关系和权限辅助信息 |

`external_user_id` 和 `external_chat_id` 不直接明文用于查询，建议保存 hash 或密文。

## 3. Session 与执行

Session 的平台逻辑键是：

```text
tenant_id + app_id + session_principal_id + session_id
```

`session_id` 只需要在 `session_principal_id` 内唯一，`user_id` 表示当前真实发言人，不属于 Session 键。映射到 tRPC-Agent-Go PostgreSQL Session 时，规范化的 `tenant_id/app_id` 对应 `app_name`，`session_principal_id` 对应框架的 `user_id`，`session_id` 保持不变。

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| session | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`session_scope`、`last_event_seq`、`updated_at` | 会话主记录 |
| session_event | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`event_seq`、`request_id`、`user_id`、`event_type`、`payload`、`created_at` | append-only 事件；`user_id` 是该事件的真实发言人 |
| execution | `tenant_id`、`app_id`、`request_id`、`session_principal_id`、`session_id`、`turn_seq`、`status`、`error_type`、`trace_id`、`started_at`、`finished_at` | 一次 Runner 执行 |
| summary | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`up_to_event_seq`、`content`、`model_version`、`updated_at` | 会话摘要 |

关系：

```text
session (tenant_id, app_id, session_principal_id, session_id) 1 -> N session_event
session (tenant_id, app_id, session_principal_id, session_id) 1 -> N execution
execution 1 -> N session_event
summary (tenant_id, app_id, session_principal_id, session_id) -> session
summary.up_to_event_seq <= session.last_event_seq
```

## 4. Memory、Knowledge 与 Artifact

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| memory | `tenant_id`、`app_id`、`memory_id`、`subject_id`、`source_event_id`、`content`、`indexed_at` | 长期记忆权威记录 |
| knowledge_base | `tenant_id`、`knowledge_base_id`、`name`、`owner_scope`、`backend_config`、`status` | 租户知识库 |
| knowledge_binding | `tenant_id`、`app_id`、`config_version`、`knowledge_base_id`、`binding_scope`、`channel_binding_id`、`conversation_id`、`user_id`、`retrieval_policy`、`priority`、`status` | 知识库与 Agent 配置的绑定关系 |
| knowledge_document | `tenant_id`、`knowledge_base_id`、`document_id`、`version`、`object_key`、`parser`、`acl_policy`、`status`、`index_generation` | 知识库文档 metadata |
| artifact | `tenant_id`、`app_id`、`artifact_id`、`object_key`、`owner_scope`、`owner_id`、`session_principal_id`、`session_id`、`mime`、`size`、`checksum`、`status`、`scan_status`、`access_policy` | 上传或生成的文件 metadata；关联 Session 时两个 Session 字段同时存在 |

Knowledge 和 Artifact 的原文通常放对象存储，SQL metadata 是权限入口。向量库只保存派生索引，不能作为唯一权威数据源。

## 5. 幂等与审计

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| message_inbox | `tenant_id`、`app_id`、`binding_id`、`external_message_id`、`request_id`、`payload_hash`、`status`、`created_at` | 入站消息幂等 |
| message_outbox | `tenant_id`、`app_id`、`request_id`、`binding_id`、`part_no`、`payload`、`status`、`retry_count`、`next_retry_at` | 出站回复重试 |
| audit_log | `tenant_id`、`app_id`、`channel`、`user_id`、`session_principal_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost`、`trace_id`、`created_at` | 审计日志 |

推荐唯一约束：

```text
channel_binding: unique(tenant_id, app_id, binding_id)
session: unique(tenant_id, app_id, session_principal_id, session_id)
session_event: unique(tenant_id, app_id, session_principal_id, session_id, event_seq)
summary: unique(tenant_id, app_id, session_principal_id, session_id)
execution(request): unique(tenant_id, app_id, request_id)
execution(turn): unique(tenant_id, app_id, session_principal_id, session_id, turn_seq)
message_inbox: unique(tenant_id, app_id, binding_id, external_message_id)
message_outbox: unique(tenant_id, app_id, request_id, part_no)
```
