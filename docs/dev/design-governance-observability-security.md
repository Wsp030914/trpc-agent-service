# 治理、监控和安全详细设计

> 开发用设计草稿，不作为题目交付物。正式交付物以 `docs/deliverables/` 为准。

## 1. 工具治理与二次确认

模型不可信，Tool 可见不等于 Tool 可执行。每个 App 的不可变配置为工具声明三种决策：

```text
ALLOW     直接执行
DENY      禁止执行
APPROVAL  需要二次确认
```

运行时先按“可见”规则过滤模型可调用的 Tool；模型请求 Tool 后，在真实调用前按可信的 `tenant_id`、`app_id`、`user_id`、`channel`、工具风险和审批状态重新决策。`DENY` 和 `APPROVAL` 都不得调用 Tool。

需审批 Tool 创建持久化审批记录，至少包含：

```text
approval_id, tenant_id, app_id, request_id, turn_seq, user_id,
tool_name, tool_call_id, arguments_enc, arguments_digest, expires_at, status,
execution_owner, executed_at, resolved_by, resolved_at
```

审批只授权一次、这一组确定参数。原发起用户或有平台权限的管理员可以确认；参数变化、拒绝或超时后审批失效。确认后执行保存的精确 Tool 调用，不让模型重新生成参数。审批最小生命周期为：`PENDING -> APPROVED/REJECTED/EXPIRED`，Worker 用 `WHERE status = APPROVED` 原子领取为 `EXECUTING`，然后进入 `EXECUTED` 或 `UNKNOWN`；`UNKNOWN` 只能由人工记录处置结果后变为 `RESOLVED`。

```text
Runner 请求危险 Tool
-> 持久化原始 tool_call_id、approval，同一 execution 进入 WAITING_APPROVAL
-> 停止内存中的 Runner，不保留内存等待
-> 确认时校验未过期、未使用且 arguments_digest 一致
-> Worker 条件更新 APPROVED -> EXECUTING，成功者独占执行
-> Tool Executor 取得 Redis Session Lease 后执行保存参数一次
-> 以 approval_id 幂等写 tool_result Event 和 Audit
-> 写入成功后将 execution 置回 PENDING，由新启动的 Runner 继续该轮
```

等待审批期间，同一 Session 的后续 turn 按顺序等待。Tool Executor 不运行 Runner，但必须以条件更新独占 Execution，并取得 Redis Session Lease；不引入新的 Session 写入令牌，普通 Runner 的迟到写入残余风险同样适用于审批恢复。拒绝、过期或可确认失败不能执行 Tool，但会写入对应的结构化结果后恢复该轮。若结果已写入、但 Job 尚未恢复，恢复器可以按 `approval_id` 重复投递结果，再条件更新 Job，不产生第二个结果。执行进程崩溃且 Tool 结果不能确认时标记 `UNKNOWN`；非幂等 Tool 不自动重试，Job 保持等待，直到人工处置后写入结果。

平台 Tool 执行包装是唯一实际拦截点。Guardrail 用于检查输入、输出和工具参数中的敏感或危险内容；Callback 记录 `allow`、`deny`、`pending`、`approved`、`executed` 等决策；Plugin 承载可插拔的租户策略实现，但不替代执行前校验。当前不设计通用规则脚本或参数 DSL。

## 2. IM 用户权限、预算和限流

Channel Adapter 映射出可信 `user_id` 后，平台按 `tenant_id + app_id + user_id` 查询平台角色。外部群成员、群主或管理员信息可以辅助映射，但不自动授予平台管理员、高危 Tool 或审批权限；未知用户使用最低权限。

控制点分两层：

```text
Gateway 准入前：用户/Binding 请求频率、App 并发、Tenant/App 配额
Tool 执行前：用户角色、通道、工具风险、审批状态
```

预算至少包括单次最大模型输出或 token、单次最大 Tool 调用数、Tenant/App 周期内 token 或成本上限。用量以 `request_id` 幂等账本结算；只有要求并发下严格不超额的额度才在 Gateway 原子预留，并由 Worker 结算或释放。预算不足不创建 Job。限流键为：

```text
tenant_id + app_id + binding_id + user_id
```

超过限流或预算时，Adapter 返回通道可表达的忙碌或稍后重试提示，不执行 Agent。

## 3. 权威 Audit Log

Audit 是合规和追责记录，不是普通日志或 Telemetry。它写入平台 SQL 的追加型记录，不采样，也不依赖异步 Trace 导出；`audit_policy` 只控制保留、脱敏和查询权限，不选择 Audit 后端。

每条 Audit Event 至少包含：

```text
audit_id, occurred_at, tenant_id, app_id,
channel, user_id, session_id, request_id, trace_id,
agent_name, event_type, tool_name, decision,
latency, error_type, cost
```

配置或凭据变更、认证失败、Agent 执行起止、Tool 决策和执行结果、IM 回复结果均应记录。审计不保存 token、DSN、完整消息、原始 Tool 参数；需要关联时只保存摘要、分类或参数摘要。

危险 Tool 在调用前写入审批或执行意图，结束后补写结果。崩溃时保留“已请求但结果未知”的状态，不误记为成功。写入时根据租户审计策略计算 `expire_at`，由后台任务到期清理；查询仅开放给本租户授权管理员。

## 4. Trace、指标和告警

Channel Adapter 在回调入口创建或提取 Trace Context。Gateway 将 Trace Context 随 Job 持久化，Worker 恢复关联后继续覆盖：

```text
IM callback -> Gateway admission -> Job / Worker -> Runner
-> Model / Tool -> Session / Memory -> Reply Outbox / IM send
```

Span 可以携带 `tenant_id`、`app_id`、`channel`、`request_id` 和 `trace_id` 等检索属性，但不得携带消息正文、凭据、完整 Tool 参数或 PII。

指标至少覆盖请求量、错误率、p95/p99 延迟、队列积压、Runner/Model/Tool/Session/Memory 耗时、IM 入站与回复成功率、重试、token、成本、限流拒绝、预算拒绝和审批等待量。

聚合指标不能将 `user_id`、`session_id`、`request_id`、`trace_id` 作为标签。精确的 Tenant/App 用量和成本从 SQL Audit/Usage 记录聚合，不作为通用指标标签。Telemetry 导出失败不阻塞 Agent 执行；Audit 可靠性独立于 Telemetry。

## 5. 密钥与脱敏

模型 API Key、IM Token、数据库 DSN、Tool/MCP 凭据仅以 `secret_ref` 存在配置、Binding 和数据库中。Worker 按 `tenant_id + app_id + secret_ref` 从 Secret Manager/KMS 按需获取，不能把密钥写入 Job、Audit、普通配置、日志或错误信息。

入站 API Key 由平台生成高熵原始值，只展示一次；数据库只保存单向 digest，不能通过 `secret_ref` 找回。普通轮换通过新 Secret 版本和新 AppConfig 生效；紧急撤销立即停用关联 Binding、Credential 或 App。

日志和 Trace 使用白名单：

```text
允许：tenant_id、app_id、config_version、channel、binding_id、
session_id、request_id、trace_id、错误类别、耗时、成本

禁止：Authorization、Cookie、API Key、Token、DSN、完整 PII、
消息正文、附件内容、原始 Tool 参数、模型或后端原始报错
```

脱敏在 Channel Adapter、Gateway、Worker、Tool 和 Storage Adapter 产生日志或 Span 前完成，不能仅依赖 Collector。Tool 所需密钥由受作用域约束的 Provider 注入，模型和普通业务日志都不能读取该值。
