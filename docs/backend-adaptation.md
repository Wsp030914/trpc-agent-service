# 后端适配方案

本文只写当前代码真实解析或实际使用的 Backend。`tenant.BackendKind` 的 `inmemory/sql/redis/vector/object/external` 用于表达租户级 Backend 选择，实际 Provider 由 ConfigVersion 和对应 resolver 装配。

## 当前真实后端

| Domain | Provider / 位置 | 保存内容 | authority | 一致性/延迟 | 成本与运维 | 故障与迁移 |
| --- | --- | --- | --- | --- | --- | --- |
| 平台控制面 | PostgreSQL `platform.*` / `postgres.Store` | tenant/app/config/credential/binding、execution、event、outbox、approval、migration、artifact/knowledge catalog、audit、heartbeat | 平台所有权威状态 | 事务、行锁、unique/FK；Admission 强一致；每次执行会有多次 SQL 往返和 event 写 | 需要 PostgreSQL 备份、连接池、migration/锁；成本高于纯内存但可恢复 | readiness/admission/claim 失败并退避；schema migration checksum fail closed；无 SQL 不应切换 Redis 为 authority |
| Session | framework PostgreSQL Session (`session/postgres`) | framework events/state/tracks/summary | 该 config 的 Session Backend；平台 `session_lane` 只管顺序 | 持久、跨 Worker 可见；读写延迟取决于同库/网络 | SQL schema/连接和清理由 framework/运维承担；可按 `schema` 隔离 | 当前支持 Redis→Postgres 迁移，drain/copy/verify 后切换 |
| Session | framework Redis Session (`session/redis`) | framework Session 内容 | 该 config 的 Session Backend，不是平台 execution authority | 低延迟、跨节点可见；依赖 Redis 数据持久性/可用性 | Redis 运维简单、吞吐高；需要内存、TTL/AOF/备份策略 | resolver 能列 legacy ZSet 和 current HashIdx inventory；迁移目标只能是 PostgreSQL |
| Session | framework InMemory (`session/inmemory`) | 单进程 Session | 仅本地/test；不能作为生产共享 authority | 最低延迟，节点间不可见 | 无外部成本；进程退出即丢 | `session.Router` 可选注入，生产运行时没有该 route；不可做跨节点迁移 |
| Memory | TencentDB Agent Memory (`memory/tencentdb`) | framework Session capture 的长期 Memory | TencentDB；平台只控制 scoped key/配置 | 外部网络调用，最终可见性由 Provider；私聊按 tenant/app/user/session 隔离 | 外部服务成本和凭据/网关运维；endpoint 由 operator logical map 提供 | resolver 失败阻止相应 runtime；共享群 Session 当前跳过 ingestor；已完成真实外部连通性与跨节点可见性实测 |
| Knowledge | Qdrant (`knowledge/qdrant`) | 向量、检索 point | Qdrant 保存向量；PostgreSQL catalog 是 tenant/config/KB authorization authority | 向量 upsert/query 是 Provider 语义；SQL 额外做 scope 过滤，延迟含两次 backend 访问 | 需要 collection、embedding dimension、index generation、容量和备份运维 | 当前 Knowledge migration 仅 Qdrant→Qdrant；point 写入 wait/verify，失败可 checkpoint 恢复 |
| Artifact | Tencent COS (`artifact/cos`) | Session artifact 和 IM 入站对象 | PostgreSQL artifact/inbound metadata + COS object；权限由 metadata/object key 双重约束 | 对象读写非 SQL 原子；大文件适合外部化，读取会增加网络延迟 | 对象存储成本低、容量大；需 endpoint/secret/生命周期/cleanup | staging 失败由 compensator，删除由 durable cleanup retry；对象和 metadata 可能短暂不一致 |
| Queue/coordination | Redis Stream + Consumer Group | dispatch transport、pending delivery；不存权威 execution | PostgreSQL execution authority | Stream at-least-once；XACK 在 durable transition 后 | 低延迟、运维独立于 SQL；需要持久化/AOF、内存和监控 | XAUTOCLAIM、DLQ、outbox relay；Redis 断开时 backoff，execution 不丢 |
| Lock/limiter | Redis SET NX/Lua | session lease、execution/relay/reply coordination、每 binding reply rate limit | lease token 是当前 owner 的协调 authority；业务状态仍 SQL | 低延迟；TTL/renew interval 决定故障检测窗口 | 与队列共用 Redis，成本小但需避免容量互相影响 | token 校验 release/renew；丢 lease 取消 Runner；limiter 不可用则不发 Provider |
| Model | OpenAI-compatible (`model/openai`) | 模型请求/响应，不是平台数据存储 | Provider model endpoint；config/secret 由平台 pin | timeout 默认 1m；延迟和 token 由 Provider | API 成本由 operator pricing catalog 可选估算；需 HTTPS/egress allowlist | retry 只在明确 safe 边界；超时取消 context；usage 缺失时 budget fail closed |

## 适配层如何选择

Worker 先从 execution 精确加载 immutable `AppConfig`，再由 resolver 按 `BackendRef.Provider/Kind/Name/SecretRef/Options` 生成或缓存 framework service。缓存 key 含 tenant/app、ConfigVersion、provider/name、schema 或 endpoint，避免不同租户/版本复用同一 service。endpoint map（COS、Qdrant、TencentDB）由 operator 环境变量按逻辑 name 提供，tenant config 不能直接注入任意 URL；模型 base URL 经过 HTTPS 和 egress policy。

Session 的访问契约最完整：`session.Router` 只接受 `postgres`、`redis` 和可选 `inmemory`。Memory、Knowledge、Artifact 没有一个“大而全”的通用远端接口去掩盖一致性差异：各 resolver 在进入 framework 前补 scope、SQL authorization、对象 metadata、secret 和 tracing。这样 Session 迁移可以复制完整 framework session，Knowledge 迁移可以按 SQL catalog 的 chunk identity 复制向量，Artifact 只依赖元数据驱动 cleanup。

## 一致性取舍

PostgreSQL 适合“是否接收、谁拥有执行、哪个 ConfigVersion、下一 turn、是否已发送”这类需要事务和 fencing 的状态；Redis 适合“尽快分发、租约和限流”这类可通过 TTL/重试恢复的协调状态；它不应成为 execution authority。Qdrant 的相似度检索不是严格事务查询，所以 SQL catalog 先授权、向量结果再按 scope 过滤；COS 的对象操作不能与 SQL 两阶段提交，因此用 PENDING/ATTACHED/DELETED 和 cleanup retry 把暂时不一致收敛；TencentDB Memory 是外部最终可见服务，不参与平台事务。

成本上，SQL 写入较贵但提供审计、恢复和强约束；Redis 降低排队/锁延迟但要支付内存/AOF 运维成本；Qdrant 和 COS 将高体积数据移出 SQL，代价是跨服务网络和独立备份；TencentDB 降低自建 Memory 的工程量，代价是 Provider 依赖和独立运维。真实连通性、可见性和故障行为已由外部实测覆盖。

## 迁移能力

| Domain | Source → Target | 实现机制 | 验证 |
| --- | --- | --- | --- |
| Session | Redis → PostgreSQL | DRAINING 后复制完整 events/state/tracks/summary，VERIFYING 通过后事务切换 active ConfigVersion | migration tests、migration E2E Workflow、外部迁移实测 |
| Knowledge | Qdrant → Qdrant | 按 SQL catalog 的 chunk inventory 复制，校验 embedding dimension、index generation 和 deterministic point identity | Qdrant migration tests、migration E2E Workflow、外部迁移实测 |

两条迁移都由 PostgreSQL `data_migration` 记录状态、lease、run token 和 checkpoint；成功切换前保持 source ConfigVersion active。这样迁移能力与 Session/Knowledge 各自的数据 authority 保持一致。
