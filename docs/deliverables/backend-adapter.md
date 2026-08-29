# 多后端适配方案

## 1. Storage Adapter 职责

Storage Adapter 是租户数据后端路由层，根据租户配置选择并装配 Session、Memory、Artifact、Knowledge 等后端服务。Audit 固定写入平台 SQL Audit Store，不属于租户可切换的后端。

它不负责业务路由，也不绕过租户权限访问数据。业务链路只能使用带 `tenant_id/app_id` 作用域的后端能力，不能直接拼接物理 DSN。

## 2. 后端配置

每个 Agent App 的配置版本可以绑定一组后端配置：

```text
backend_config
├── Session
├── Memory
├── Knowledge
└── Artifact
```

Session 后端是无状态 Worker 的基础能力，生产环境必须使用共享后端。本方案以 Redis Session Lease 取消失效 Runner，不要求 Session Provider 实现写入令牌；因此不承诺严格拒绝旧 Worker 的迟到写入。原子条件写入可作为未来可选增强接入，不是当前自动故障接管的前置条件。Memory、Knowledge、Artifact 可以按租户需求启用；Audit 的保留和脱敏由 `audit_policy` 控制，权威记录始终在平台 SQL。

模型密钥、IM token、数据库密码和对象存储凭据等需要运行时取回的 Secret 只保存 `secret_ref`，运行时从 Secret Manager/KMS 获取。用于外部系统调用平台的 API Credential 属于入站认证凭据，数据库保存单向 `key_digest`，不通过 `secret_ref` 恢复原文。

## 3. SQL

SQL 适合保存强一致、可审计、需要事务的数据：

- Tenant、Agent App、配置版本。
- 默认 PostgreSQL Session Provider 的 Session、Event、State、Summary；其 Schema 和表由 Provider 在选定的 DSN 中准备，不属于平台协调 migration。
- Inbox、Job Outbox、Reply Outbox。
- Audit Log。
- Knowledge 和 Artifact metadata。

SQL 优点是事务清晰、查询方便、审计和备份成熟。缺点是高峰写入需要索引、分区和连接池治理。

## 4. Redis

Redis 适合低延迟和高吞吐场景：

- 队列或 stream。
- 限流计数。
- 短期缓存。
- 可选 Session 热数据。

Redis 单 key 或 Lua 内一致性较好，但跨 key 事务和长期审计不如 SQL。生产使用 Redis 时仍建议把 execution、audit、metadata 等权威记录写入 SQL。

## 5. 向量库

向量库适合 Memory 和 Knowledge 的语义检索索引：

- Memory embedding。
- Knowledge chunk embedding。
- 检索召回和相似度排序。

向量库通常是最终一致，索引可能滞后。权威数据应保存在 SQL/Object 中，向量库只保存派生索引。查询必须附带 `tenant_id`、knowledge scope、ACL 和 index generation 过滤。

## 6. 对象存储

对象存储适合大文件和原文：

- 用户上传文件。
- 图片、音频、视频。
- Agent 生成物。
- Knowledge 原文和解析产物。

对象路径不能作为权限依据。读取对象前必须先查 SQL metadata，校验租户、应用、会话、用户、文件状态、MIME、大小、安全扫描结果和访问策略。

## 7. InMemory

InMemory 只适合本地开发和测试：

- 单进程内验证链路。
- 单元测试。
- Fake backend。

它不支持多节点共享状态，不具备恢复能力，生产 Worker 禁止使用 InMemory 承载 Session 或 Memory。

## 8. 后端选择建议

| 数据类型 | 推荐后端 | 说明 |
| --- | --- | --- |
| 配置、版本、审计 | SQL | 审计是平台固定权威记录；需要事务、查询和回滚 |
| Session/Event/State/Summary | App 选择的共享 Session Provider | 必须共享；Summary 与同一 Provider 的 Event/State 对齐；本方案以 Lease 取消旧 Runner，不承诺严格拒绝迟到写入 |
| Memory 权威记录 | SQL / Memory Store | 向量库只做检索索引 |
| Knowledge metadata | SQL | 记录 ACL、版本、绑定关系 |
| Knowledge 原文 | Object Storage | 保存文档和解析产物 |
| Knowledge 索引 | Vector DB | 支持语义检索 |
| Artifact | Object Storage + SQL metadata | metadata 控权，object 保存内容 |
| 队列、限流、缓存 | Redis | 降低 SQL 压力 |

## 9. 迁移边界

后端自身的 Schema 或索引准备由具体适配器负责：例如 PostgreSQL 负责平台表 DDL，
Redis 负责 keyspace/version 准备，向量库负责 collection 和 index generation。跨后端
数据迁移不放在 `postgres`、`redis` 或其他单一适配器中，而由包外迁移编排层在 App 的
`data_migration` 的维护窗口内统一处理排空旧 Job、全量复制、校验、切换和失败恢复。权威
`backend_config` 变更只能通过该编排层生效，不能经普通配置发布直接激活。编排层依赖
能力型接口，不要求所有后端实现相同的底层迁移语义。
