# 多后端适配边界

Storage Provider 只负责按 immutable BackendConfig 创建并复用真实后端资源；业务
层不能绕过 scope 直接拼接 DSN、object key 或 vector filter。

## 配置到 Provider

```text
BackendConfig
├─ Session   → PostgreSQL Session Provider 或 Redis Session Provider
├─ Memory    → Memory Provider
├─ Knowledge → Qdrant Provider + SQL Catalog
└─ Artifact  → object storage + SQL metadata
```

Session Provider 必须是共享资源，才能支持无 sticky session 的 Worker。Session
Router 保留实际存在的 PostgreSQL/Redis 两条路，并在服务生命周期内复用 service。
Memory、Qdrant、COS client 也由各自 owner 关闭；这些缓存不保存请求级 Runner。

## 一致性、延迟、成本与运维取舍

| 后端 | 一致性 | 延迟 | 成本 | 运维取舍 |
| --- | --- | --- | --- | --- |
| PostgreSQL / SQL | 事务和条件更新提供强一致 | 中等 | 中等 | 备份、索引、连接池和容量规划成熟；承载权威协调记录 |
| Redis | 单 key/Lua 内原子；跨 key 非强一致 | 低 | 中等至高 | 适合 Stream、Lock、限流和热数据；需管理持久化、故障转移和内存淘汰 |
| Qdrant / Vector DB | 派生索引最终一致 | 低至中等 | 中等 | 需维护 collection、索引和重建；不能替代 SQL 权威数据 |
| Object Storage | bytes 持久；业务 metadata 由 SQL 保证一致 | 中等 | 存储低，读写/出网另计 | 适合媒体和原文；需管理生命周期、权限和 orphan 补偿 |
| InMemory | 单进程内一致 | 最低 | 基础设施低 | 只用于测试和本地开发；无跨节点共享和恢复能力 |

## Knowledge

Qdrant 是运行时检索 Provider。每次执行按 `tenant_id`、`app_id`、固定
`config_version` 和 `knowledge_base_id` 选择索引；SQL Catalog 在返回结果前检查
knowledge base、document version、index generation 和 available 状态。向量库只
保存可重建的索引，不提供跨租户查询。

运行时路径只有：

```text
AppConfig Knowledge BackendRef
→ Qdrant Provider
→ SQL-scoped chunk retrieval
→ Runner Knowledge
```

不在平台内实现 source importer、解析流水线或 durable indexing worker。

## Artifact

对象存储保存媒体 bytes，SQL 保存带 tenant/app/session scope 的 metadata。对象 key
由平台生成；读取必须先通过 metadata authorization 和 version 校验。IM 回调中的
媒体先写 object，再写 metadata，后续只传 ArtifactRef；Worker 在模型调用前恢复
内容。

若 metadata 写入失败，立即 best-effort 删除刚写入的精确对象；删除失败记录错误，
不创建 durable cleanup record。该失败只可能留下 orphan object，不改变 metadata
授权、版本读取和执行主链。

## Secret

模型、IM、数据库和对象存储凭据只作为 SecretRef 保存在配置中。Scoped
SecretProvider 是安全边界；Provider 不接收调用方自带的 secret bytes，也不把 secret
写入 Queue、Session Event、Reply 或日志。

## 切换

后端切换由 immutable AppConfig version 和现有 data migration admission gate 控制：
停止新准入、排空已接受执行、复制并校验数据后再切换 active version。Provider 本身
不维护第二套迁移状态机。
