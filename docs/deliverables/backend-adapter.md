# 多后端适配方案

## 1. Storage Adapter 职责

Storage Adapter 是租户数据后端路由层，根据租户配置选择并装配 Session、Memory、Artifact、Knowledge、Audit 等后端服务。

它不负责业务路由，也不绕过租户权限访问数据。业务链路只能使用带 `tenant_id/app_id` 作用域的后端能力，不能直接拼接物理 DSN。

## 2. 后端配置

每个 Agent App 的配置版本可以绑定一组后端配置：

```text
backend_config
├── Session
├── Memory
├── Knowledge
├── Artifact
└── Audit
```

Session 后端是无状态 Worker 的基础能力，生产环境必须使用共享后端。Memory、Knowledge、Artifact、Audit 可以按租户需求启用。

密钥、token、数据库密码和对象存储凭据只保存 `secret_ref`，运行时从 Secret Manager/KMS 获取。

## 3. SQL

SQL 适合保存强一致、可审计、需要事务的数据：

- Tenant、Agent App、配置版本。
- Session、Event、State、Summary。
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

它不支持多节点共享状态，不具备恢复能力，生产 Worker 禁止使用 InMemory 承载 Session、Memory 或 Audit。

## 8. 后端选择建议

| 数据类型 | 推荐后端 | 说明 |
| --- | --- | --- |
| 配置、版本、审计 | SQL | 需要事务、查询和回滚 |
| Session/Event/State | SQL 或 Redis-backed Session | 必须是共享后端 |
| Summary | SQL | 和 event_seq 对齐 |
| Memory 权威记录 | SQL / Memory Store | 向量库只做检索索引 |
| Knowledge metadata | SQL | 记录 ACL、版本、绑定关系 |
| Knowledge 原文 | Object Storage | 保存文档和解析产物 |
| Knowledge 索引 | Vector DB | 支持语义检索 |
| Artifact | Object Storage + SQL metadata | metadata 控权，object 保存内容 |
| 队列、限流、缓存 | Redis | 降低 SQL 压力 |
