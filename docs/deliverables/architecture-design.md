# 多租户节点化 Agent 平台架构

## 设计边界

服务支持多个 tenant/app 在多节点上执行固定版本的 Agent 配置，按配置选择共享
Session、Memory、Knowledge、Artifact 后端，并完成飞书/企业微信的安全收发。
AuditPolicy、Audit Log 和租户治理策略接入执行链；服务使用标准 OTEL runtime，
无 OTLP endpoint 时不导出，Collector 仍是可选外部组件，不加入独立审计 Worker、
健康检查或知识导入调度系统。

## 主链

```text
IM / HTTP
→ trusted identity + Channel Binding
→ Gateway Admission
→ PostgreSQL Inbox + Execution + Dispatch Outbox
→ Relay / Redis Stream
→ Worker claim + Execution Lease
→ Redis Session Lock
→ Runtime.BuildRunner(exec)
→ fresh Runner.Run
→ event drain + Runner.Close
→ Reply Outbox
→ Binding Revision recheck
→ Feishu / WeCom send
```

Provider event、Gateway、Worker、Runner、Tool、Session/Memory 和 Reply 边界使用安全
metadata span；W3C trace context 随 Execution 持久化并跨 Redis Stream 传播。审计
事件只保存身份、决策、耗时、错误类型、token/cost 和关联 ID，不保存消息、Prompt、
Tool arguments、Provider target 或 Secret。

Gateway 在一个事务中复核 tenant/app/binding、读取 active config version、处理
Inbox 幂等、分配 Session `turn_seq`，并同时写 Execution 与 Dispatch Outbox。
重复 provider event 命中 Inbox 时返回原 request。

Worker 从 Execution 中读取并固定的 `config_version` 装配运行时。每次执行创建
一个 fresh Runner；长期 Session service、Memory、Qdrant、Artifact 和客户端缓存
由各自 Provider owner 管理，Worker 在 event channel 关闭后关闭 Runner。

## 隔离与配置

- tenant 是最高隔离边界；所有 SQL 查询、Redis key、向量过滤、对象 metadata 都
  带 tenant/app scope。
- 长连接认证和 event 校验后才能导出 tenant/app 和外部身份；payload 不可信。
- AppConfig version immutable；Execution 固定版本，配置发布只影响后续准入。
- 模型、IM 和存储凭据只以 SecretRef 进入配置，运行时由 scoped SecretProvider
  解析；secret 原文不进入队列、Session 或日志。
- Session key 为 `tenant + app + session_principal + session`。turn_seq、Execution
  Lease 和 Redis Session Lock 分别保护顺序、运行所有权和实际串行执行。

## Provider 组装

```text
AppConfig.BackendConfig
├─ Session   → PostgreSQL / Redis Session Provider
├─ Memory    → configured Memory Provider
├─ Knowledge → Qdrant Provider → SQL-scoped Knowledge
└─ Artifact  → object storage + SQL metadata
```

Knowledge 只提供 Qdrant 检索：SQL Catalog 先校验 tenant/app/config/knowledge-base
范围和可用版本，再返回 scoped result。Artifact 只在 SQL metadata 授权后按
ArtifactRef/version 读取；媒体 bytes 在模型调用边界恢复。

## IM

Feishu Adapter 使用官方 Go SDK 长连接；WeCom Adapter 使用官方 AI Bot WebSocket
协议。二者负责连接认证、身份映射和入站标准化。出站 Reply 是普通文本：Runner event → Reply Outbox → 当前 Binding Revision 的 target
resolve/decrypt → provider send。当前选择普通异步文本；stream/card 只作为后续
扩展能力，不进入当前主链。provider transient failure 按 Reply Outbox 的重试策略
处理，其他 channel 细节不进入 Gateway 或 Worker。

## 多后端切换

AppConfig 的后端引用仍按 immutable version 发布。需要复制和切换数据时使用现有
data migration admission gate；它负责排空旧执行、复制校验并切换 active version。
知识 importer、索引任务租约和 Artifact cleanup recovery 不属于运行时主链。
