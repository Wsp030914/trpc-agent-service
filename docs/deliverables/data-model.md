# 数据模型设计

核心模型必须表达 Tenant、Agent、Channel Binding、Session、Event、Memory、Summary 和 Audit Log 的关系。实际实现可以拆表或合表，但主键、唯一键和查询条件必须包含租户作用域。

## 1. 租户与应用

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| tenant | `tenant_id`、`name`、`status`、`audit_policy`、`created_at` | 最高隔离边界 |
| agent_app | `tenant_id`、`app_id`、`name`、`active_config_version`、`status` | 租户下的 Agent 应用；维护门禁不写入此表，由未完成的 `data_migration` 权威记录表达 |
| app_config_version | `tenant_id`、`app_id`、`version`、`model_config`、`tool_policy`、`backend_config`、`audit_policy`、`status` | 不可变运行配置版本；不包含 Channel Binding 启停状态 |
| api_credential | `tenant_id`、`app_id`、`credential_id`、`key_digest`、`key_prefix`、`status`、`expires_at`、`created_at`、`last_used_at` | 外部业务系统调用平台的入站凭据 |

关系：

```text
tenant 1 -> N agent_app
agent_app 1 -> N app_config_version
agent_app.active_config_version -> app_config_version.version
agent_app 1 -> N api_credential
```

API Key 由平台使用至少 32 个随机字节生成，原文只展示一次。`api_credential` 只保存单向 `key_digest` 和用于识别的非敏感 `key_prefix`，不能恢复原文。模型 API Key、IM token、数据库密码和 Tool/MCP 凭据是需要运行时取回的 Secret，只在配置中保存带作用域的 `secret_ref`，不放入 `api_credential`。

Credential 状态包含 `ACTIVE`、`SUSPENDED` 和不可逆的 `REVOKED`；`expires_at` 非空时表示凭据从该时刻起失效。

## 2. IM 绑定与身份

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| channel_binding | `tenant_id`、`app_id`、`binding_id`、`channel`、`external_account`、`webhook_url`、`token_ref`、`signing_secret_ref`、`secret_ref`、`status` | IM 外部账号和租户应用的独立入口资源；`ACTIVE` 是唯一 IM 准入状态 |
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

### 平台 PostgreSQL 协调记录

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| session_lane | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`next_turn_seq`、`updated_at` | 只分配 `turn_seq`，不保存 Session 内容或写入令牌 |
| execution | `execution_id`、`tenant_id`、`app_id`、`request_id`、`session_principal_id`、`session_id`、`user_id`、`turn_seq`、`config_version`、`tenant_source`、`source_id`、`idempotency_key`、`command`、`payload_hash`、`status`、`attempt`、`next_attempt_at`、`lease_owner`、`run_token`、`lease_until`、`last_error`、`error_type`、`trace_id`、`started_at`、`finished_at` | 一次用户输入从准入到终态的唯一可恢复记录；重试、审批或重新启动 Runner 均复用同一记录 |
| dispatch_outbox | `outbox_id`、`tenant_id`、`app_id`、`request_id`、`status`、`attempt`、`next_attempt_at`、`lease_owner`、`lease_until`、`last_error` | PostgreSQL 事务内创建的可靠分发记录，由 Relay 发布到 Redis Streams；`SENT` 的旧记录与 Redis Pending 均可恢复 |
| execution_event | `tenant_id`、`app_id`、`request_id`、`event_seq`、`event_type`、`payload`、`created_at` | 协议流式响应和断线恢复使用的持久化事件日志 |

### Provider 管理的逻辑 Session 记录

下列记录是题目要求的数据模型和作用域契约，不是平台固定 PostgreSQL 表。它们由 Agent App 选定的 tRPC-Agent-Go Session Provider 保存；默认 PostgreSQL Provider、Redis、MySQL 等后端各自决定物理表或 key 结构。

| 逻辑记录 | 核心字段 | 说明 |
| --- | --- | --- |
| session | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`state`、`last_event_seq`、`updated_at` | 会话主记录和 State |
| session_event | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`event_seq`、`request_id`、`user_id`、`event_type`、`payload`、`created_at` | append-only 事件；`user_id` 是真实发言人 |
| summary | `tenant_id`、`app_id`、`session_principal_id`、`session_id`、`up_to_event_seq`、`content`、`model_version`、`updated_at` | 会话摘要；只能覆盖已提交 Event 范围 |

逻辑关系：

```text
session (tenant_id, app_id, session_principal_id, session_id) 1 -> N session_event
session_lane (tenant_id, app_id, session_principal_id, session_id) 1 -> N execution
execution (tenant_id, app_id, request_id) 1 -> N dispatch_outbox
execution 1 -> N execution_event
execution.request_id -> session_event.request_id
summary (tenant_id, app_id, session_principal_id, session_id) -> session
summary.up_to_event_seq <= session.last_event_seq
```

`run_token` 只保护 `execution` 的运行状态、Event Journal 和 Audit 条件写入；Redis Session Lease 负责实际 Runner 串行化。本方案不向 Session Provider 传递写入栅栏，Lease 丢失后的旧 Runner 可能完成最后一次 Session 写入；这不影响新旧 Worker 对 `execution` 终态的覆盖保护，但不构成严格的 Session 写入接管。

## 4. 控制面状态

以下记录只保存跨请求、跨节点必须恢复的控制状态，不引入通用工作流或状态机框架。

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| tool_approval | `approval_id`、`tenant_id`、`app_id`、`request_id`、`turn_seq`、`user_id`、`tool_name`、`tool_call_id`、`arguments_enc`、`arguments_digest`、`status`、`expires_at`、`decided_by`、`execution_owner`、`executed_at`、`resolved_by`、`resolved_at` | 一次确定参数的危险 Tool 审批；`approval_id` 是 Tool 结果写入的幂等键，状态为待定、批准、拒绝、过期、取消、执行中、已执行、结果未知或人工已处置 |
| role_binding | `tenant_id`、`app_id`、`user_id`、`role`、`status`、`created_at`、`updated_at` | 平台用户角色映射；外部群角色不能直接替代它 |
| usage_entry | `tenant_id`、`app_id`、`request_id`、`user_id`、`metric`、`reserved_amount`、`actual_amount`、`settled_at`、`released_at` | 幂等用量账本；只有需要并发下严格不超额时才使用预留与释放 |
| data_migration | `migration_id`、`tenant_id`、`app_id`、`source_config_version`、`target_config_version`、`status`、`lease_owner`、`lease_until`、`run_token`、`drain_deadline`、`progress`、`validation_result`、`failure_reason`、`created_at`、`updated_at` | 维护窗口内唯一权威迁移记录；状态为 `PENDING`、`DRAINING`、`COPYING`、`VERIFYING`、`SUCCEEDED`、`FAILED`。前三个非终态直接构成准入门禁 |
| rollout_rule | `tenant_id`、`app_id`、`stable_config_version`、`candidate_config_version`、`percentage`、`status`、`updated_at` | 灰度规则；同一规则和比例下由稳定 Session 分区键计算版本，不保存每个 Session 的分配记录 |

审批批准后，Worker 用 `WHERE status = APPROVED` 的条件更新 Execution 原子领取为 `EXECUTING`；只有领取成功者执行 `arguments_enc` 对应且摘要匹配的一次 Tool 调用，不重新请求模型生成参数。Runner 请求审批时，先持久化原始 `tool_call_id` 对应的 Tool 调用，再将同一 `execution` 置为 `WAITING_APPROVAL`，并停止内存中的 Runner。审批状态机属于后续治理能力，接入时须继续使用 Execution 的条件更新和 Dispatch Outbox，不引入第二个 Job 表。迁移进行时，源和目标配置版本不得被原地修改。

## 5. Memory、Knowledge 与 Artifact

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| memory | `tenant_id`、`app_id`、`memory_id`、`subject_id`、`source_kind`、`source_session_principal_id`、`source_session_id`、`source_event_from_seq`、`source_event_to_seq`、`content`、`indexed_at` | 长期记忆权威记录；会话提取记录来源 Event 范围，人工或外部导入使用不同 `source_kind` |
| knowledge_base | `tenant_id`、`knowledge_base_id`、`name`、`owner_scope`、`backend_config`、`status` | 租户知识库 |
| knowledge_binding | `tenant_id`、`app_id`、`config_version`、`knowledge_base_id`、`binding_scope`、`channel_binding_id`、`conversation_id`、`user_id`、`retrieval_policy`、`priority`、`status` | 知识库与 Agent 配置的绑定关系 |
| knowledge_document | `tenant_id`、`knowledge_base_id`、`document_id`、`version`、`object_key`、`parser`、`acl_policy`、`status`、`index_generation` | 知识库文档 metadata |
| artifact | `tenant_id`、`app_id`、`artifact_id`、`object_key`、`owner_scope`、`owner_id`、`session_principal_id`、`session_id`、`mime`、`size`、`checksum`、`status`、`scan_status`、`access_policy` | 上传或生成的文件 metadata；关联 Session 时两个 Session 字段同时存在 |

会话提取的 Memory 通过 `source_session_principal_id + source_session_id + source_event_from_seq..source_event_to_seq` 关联来源；单条 Event 时起止序号相同。人工创建或外部导入的 Memory 不伪造 Session Event 来源。Knowledge 和 Artifact 的原文通常放对象存储，SQL metadata 是权限入口。向量库只保存派生索引，不能作为唯一权威数据源。

## 6. 幂等与审计

| 实体 | 核心字段 | 说明 |
| --- | --- | --- |
| message_inbox | `tenant_id`、`app_id`、`binding_id`、`external_message_id`、`request_id`、`payload_hash`、`status`、`created_at` | 入站消息幂等；已验证的撤回事件将其标记为撤回并定位关联 Job |
| message_outbox | `tenant_id`、`app_id`、`request_id`、`binding_id`、`part_no`、`payload`、`status`、`retry_count`、`next_retry_at` | 出站回复重试；IM 最终回复与 Job/Execution 终态在平台协调库同一事务创建 |
| audit_log | `tenant_id`、`app_id`、`channel`、`user_id`、`session_principal_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost`、`trace_id`、`created_at` | 平台 SQL 中的追加型权威审计记录；租户策略只控制保留、脱敏和查询权限 |

平台 PostgreSQL 推荐唯一约束：

```text
channel_binding: unique(tenant_id, app_id, binding_id)
api_credential(id): unique(tenant_id, app_id, credential_id)
api_credential(digest): unique(key_digest)
session_lane: unique(tenant_id, app_id, session_principal_id, session_id)
execution(request): unique(tenant_id, app_id, request_id)
execution(turn): unique(tenant_id, app_id, session_principal_id, session_id, turn_seq)
execution(idempotency): unique(tenant_id, app_id, tenant_source, source_id, idempotency_key)
execution_event: unique(tenant_id, app_id, request_id, event_seq)
message_inbox: unique(tenant_id, app_id, binding_id, external_message_id)
message_outbox: unique(tenant_id, app_id, request_id, part_no)
tool_approval: unique(tenant_id, app_id, approval_id)
role_binding: unique(tenant_id, app_id, user_id, role)
usage_entry: unique(tenant_id, app_id, request_id, metric)
data_migration: unique(tenant_id, app_id, migration_id)
```

`execution_id` 是 `execution` 的主键。选定的 Session Provider 必须在自己的物理模型中保持以下逻辑唯一性：

```text
session: unique(tenant_id, app_id, session_principal_id, session_id)
session_event: unique(tenant_id, app_id, session_principal_id, session_id, event_seq)
summary: unique(tenant_id, app_id, session_principal_id, session_id)
```
