# 第二阶段多后端数据同步 — 对抗性审查报告

审查日期：2026-08-29
审查范围：`trpcservice/migration`、`trpcservice/session/{redis,postgres}`、`trpcservice/artifact/cos`、`trpcservice/knowledge/{qdrant,cos,importer}`、`trpcservice/memory/tencentdb`、`trpcservice/postgres/data_migration.go`、`trpcservice/runtime`、`trpcservice/worker`、`cmd/trpc-service/main.go`，以及 tRPC-Agent-Go v1.11.2 对应框架源码。
审查方法：逐包读源码 + 框架实现回溯 + 外部行为验证。仅收录可复现缺陷，风格意见一律不收。

---

## 缺陷 A（严重 · 数据一致性）：Redis→PG 迁移静默截断超过 1000 条事件的会话

**位置**
- `trpcservice/migration/session.go:69`（`copySession` 读源）、`:84`（读目标）、`:102`（逐条 Append）
- `trpcservice/session/redis/resolver.go:77`（`redisprovider.NewService(WithRedisClientURL(url))`，未传任何事件选项）
- 框架 `session/redis@v1.10.0/options.go:20`：`defaultSessionEventLimit = 1000`
- 框架 `internal/hashidx/session.go`：`ApplyEventFiltering(WithEventNum(limit))` — 只保留最后 1000 条事件（并保证至少一条用户消息）
- `trpcservice/migration/session.go:123` `VerifySession`：两侧都用同样的无选项 `GetSession`，比较的是"都被截断到 1000 条"的结果

**复现路径**
1. 构造一个 Redis 会话，追加 1500 条事件（长对话真实可达）。
2. 触发 Redis→PG 迁移（`runDataMigration` → `Executor.Run` → `copySession`）。
3. `GetSession` 无选项 → 框架默认事件上限 1000 → 只读回最后 1000 条。
4. 目标 PG 只写入这 1000 条；`VerifySession` 两侧同样截断后比较 → **校验通过**。
5. 迁移状态推进 SUCCEEDED，`active_config_version` 原子切换到 PG。

**影响**：早期事件（含用户消息、工具调用记录、轨道事件——track 同样受限）永久丢失，且校验机制设计上无法发现。这直接违反迁移需求"完整搬迁会话数据"的验收目标，属于最严重级别。

**修复方向**：平台 redis resolver / 迁移专用读路径显式传 `WithEventNum` 足够大的值（或框架提供"全量"模式）；同时 `VerifySession` 必须用与拷贝相同（且未截断）的事件数比对，两侧若都被截断则比较结果无意义。

---

## 缺陷 B（严重 · 功能正确性）：任一绑定的知识库无命中即导致整个检索失败

**位置**
- `trpcservice/knowledge/knowledge.go:332-335`：`ScopedKnowledge.Search` 逐 base 调 `k.inner.Search`，任一 base 返回错误即 `return nil, fmt.Errorf("search knowledge base %q: %w", ...)`，已收集的其他 base 结果全部丢弃。
- 框架 `trpc-agent-go@v1.11.2/knowledge/default.go:1105-1107`：`BuiltinKnowledge.Search` 在 `len(result.Documents) == 0` 时返回错误 `fmt.Errorf("no relevant documents found")`——**空结果被框架建模为错误**。
- `trpcservice/knowledge/qdrant/resolver.go:159-163`：`frameworkknowledge.New(WithVectorStore, WithEmbedder)` 即默认 BuiltinKnowledge，被 `NewScopedKnowledge` 包装为平台检索入口。

**复现路径**
1. 应用绑定两个知识库（`exec.Config.KnowledgeBaseIDs` 含 2 个 base，多库绑定是平台的明确能力）。
2. 用户查询在 base #1 命中若干文档、在 base #2（内容不相关或刚建好还没索引文档）无命中。
3. base #2 触发框架 `"no relevant documents found"` 错误 → `ScopedKnowledge.Search` 整体失败 → Runner 检索环节报错，base #1 的有效结果被丢弃。

**影响**：多知识库场景下检索可用性退化到"最弱一环"——任何一个空库或低相关库都会让整个 RAG 检索失败。这是需求验收缺口（多库检索功能形同虚设）。

**修复方向**：`ScopedKnowledge.Search` 对每个 base 的错误分类处理：`errors.Is(err, ErrNoRelevantDocuments)`（或字符串匹配框架该错误）应跳过该 base 继续聚合；真正的检索基础设施错误才向上返回。若聚合后所有 base 均空，再按平台自己的语义决定返回空结果还是错误。

---

## 缺陷 C（高概率 · 需环境验证）：共享 collection 上每个租户重复 CreateFieldIndex

**位置**
- `trpcservice/knowledge/qdrant/resolver.go:305`：每个新 scope 的 `resolveStore` 都调 `ensureScopedPayloadIndexes`
- `trpcservice/knowledge/qdrant/resolver.go:324-347`：对 `scopedPayloadFields` 逐字段 `client.CreateFieldIndex(..., Wait: true)`，**不检查索引是否已存在、不容忍 "already exists" 错误**
- collection 名只由 `embedding_profile + index_generation` 决定，跨租户共享同一 collection（多租户隔离靠 payload filter `metadata.tenant_id` 等，该设计本身正确）
- 框架 `storage/qdrant@v1.11.0/client.go:25`：接口直接透传官方 `github.com/qdrant/go-client` v1.19.0 的 gRPC 桩，无任何幂等处理

**复现路径**
1. 租户 A 首次解析 Qdrant store → 建索引成功。
2. 租户 B（同 embedding profile / 同 index generation，即共享 collection）首次解析 store → `resolveStore` 再次执行 `CreateFieldIndex`（同 collection、同字段、同参数）。
3. Qdrant 服务端对同一字段重复建 payload index 的行为依版本而异：社区证据（llama_index PR #14001、odoo-llm、theneuralbase 文档）普遍需要显式捕获 "already exists" 错误才能容错，表明主流版本会返回错误（部分版本 REST 层幂等）。
4. 若部署的 Qdrant 版本返回错误 → 租户 B 的 `ResolveKnowledge` 初始化失败 → 该租户所有知识检索请求失败。

**影响**：轻则每个租户初始化时重复等待冗余索引操作（`Wait: true` 串行阻塞）；重则第二个及之后租户的 knowledge resolver 完全不可用。仓库内集成测试仅单租户路径，未覆盖此场景。

**验证方法**：起本地 Qdrant（compose 环境已有），对同一 collection 同一字段连续调两次 `CreateFieldIndex`（gRPC 6334），观察第二次返回。若报错即为可复现缺陷。

**修复方向**：建索引前先 `GetCollectionInfo` 查 payload schema 判断字段是否已建索引，或捕获 "already exists" 类错误视为成功。

---

## 缺陷 E（严重 · 数据一致性）：迁移执行中途 Redis 故障仍被静默当作"会话不存在"

上一轮审查发现的"Copier 构造时 Redis 读故障被吞"已通过 `CheckSessionBackend` 预检修复（`trpcservice/migration/redispostgres/copier.go:67`，用 Ping 区分"Redis 不可达"与"会话不存在"）。但预检只在 `NewCopier` 构造时刻执行一次，`Executor.Run` 随后要遍历全部会话键（可能数分钟到数小时），中途故障窗口未覆盖：

**位置**
- 框架 `session/redis@v1.10.0/service.go:373-377`：GetSession final hook 中 `checkSessionExists` 失败仅 `log.WarnfContext`，继续以 `zsetExists=false, hashidxExists=false` 调 `getSessionInternal` → 返回 `(nil, nil)`，**Redis 不可达被表达为"会话不存在"**。
- `trpcservice/migration/session.go:81-83`：`if sourceSession == nil { return false, nil }` —— 静默跳过。
- `trpcservice/migration/session.go:150-154`：verify 侧 source nil 且 target nil（因从未拷贝）→ 通过。

**复现路径**
1. 迁移开始，前 100 个会话拷贝成功。
2. Redis 发生 failover / 网络分区（迁移窗口内完全现实）。
3. 后续会话 `GetSession` 全部返回 (nil, nil) → 全部静默跳过，verify 全部通过。
4. Redis 恢复也无济于事——迁移已推进 SUCCEEDED 并原子切换 `active_config_version` 到 PG。
5. 未迁移会话滞留在旧 Redis 后端（已非权威），新会话写入 PG：**会话数据按时间线劈成两半**。

注意与"已拷贝会话"的区别：故障前已拷贝的会话在 verify 时会报 "target session exists without source session" 而安全失败；危险的是**尚未拷贝**的会话——静默丢失。

**修复方向**：每个会话拷贝/校验前做轻量存活检查（复用 `CheckSessionBackend` 或在循环内周期性 Ping）；或对 `GetSession` 返回 nil 的会话追加一次显式存在性复核（如直接查 zset 键）。

---

## 缺陷 D（边界 · 数据一致性）：零事件会话的 Summaries 静默丢失，且守卫成为死代码

**位置**
- 框架 `session/redis@v1.10.0`（`internal/hashidx/session.go`）与 `session/postgres@v1.11.0/service_helper.go`：GetSession 均仅在 `len(sess.Events) > 0` 时加载 summaries。
- `trpcservice/migration/session.go:80-81`：`if len(sourceSession.Summaries) > 0 && summaries == nil { return false, ErrSummaryImportRequired }` —— 由于零事件会话读不出 summaries，此守卫对这类会话永远不触发（死代码）。
- `trpcservice/migration/session.go:113`：`if len(sourceSession.Summaries) > 0` 才导入 —— 同样不触发。

**复现路径**：构造一个有 summaries 但事件已被清空/归档到 0 条的 Redis 会话（长期会话经框架事件过滤后理论上至少留 1 条用户消息，但若被显式清理或仅做总结压缩，则可能出现 0 事件 + 有 summaries 的状态）；迁移后目标侧 summaries 为空，无任何报错。

**影响**：低概率但静默的数据丢失；同时 `ErrSummaryImportRequired` 的防御承诺（"有摘要必须配 SummaryImporter"）在这类会话上失效。

**修复方向**：与缺陷 A 一并处理——迁移读路径需要能区分"确实没有 summaries"与"因事件为 0 而未加载"。

---

## 降级/排除的旧审查项（今日早些时候第一轮审查的复核结论）

- 旧 P0「Copier 构造时 Redis 读故障被吞」：已由 `CheckSessionBackend` 预检修复——但只覆盖构造时刻，执行中途的残余窗口升级为本报告缺陷 E。
- 旧 P1「Search filter 不含 index_generation，旧 generation chunk 持续命中」：当前代码 collection 名含 generation（`knowledge-{profile}-{generation}`，resolver.go:477-479），generation 隔离由 collection 层实现，正确性成立。剩余问题仅为旧 collection 永不清理（资源泄漏，不构成正确性缺陷）。
- 旧 P2「verify 事件顺序 / summary updated_at 客户端时钟」：PG 事件 `ORDER BY created_at ASC, id ASC`，迁移期写入方已停（DRAINING 阻断准入），id 为插入序 → 与 Redis 追加序一致；summary 时间戳仍取客户端 `time.Now()`（summary_import.go:126），时钟漂移只会导致 verify 失败（fail-safe 方向，不丢数据），不计入缺陷。

---

## 已排除的怀疑（对抗性审查中的否定结论）

以下攻击面经过完整证据链检查，**未发现可复现缺陷**，列出以避免重复审查：

| 攻击面 | 结论 |
|---|---|
| COS 对象键跨租户碰撞 | 键 `artifacts/{app_name}/...`，AppName = `tenant:{t}:app:{a}:runner`，含租户信息，无碰撞。知识源对象键另用 base64 段 + 内容 SHA256，内容寻址不可伪造。 |
| Qdrant payload filter 字段映射 | `metadata.tenant_id` 前缀与框架 `toPoint`/`metadataToCondition` 布局一致，且 SQL catalog（`AvailableKnowledgeChunk`）二次鉴权兜底。 |
| 迁移 lease/claim/token 机制 | `FOR UPDATE SKIP LOCKED` 抢占 + token 轮换 + 所有状态更新带 owner/token/lease_until 守卫，数据错误与基础设施错误分流正确（fail 持久化 FAILED，ctx 取消原样返回供接管）。 |
| 迁移目录顺序稳定性 | `session_lane` 行只插入不删除 + `ORDER BY session_principal_id, session_id` 确定性排序，断点续迁安全；DRAINING/COPYING/VERIFYING 阻断新准入。 |
| CopySession 重试幂等 | 先 `DeleteSession` 再 `CreateSession` 再逐条 Append，重入安全。 |
| SUCCEEDED 原子激活 | `AdvanceDataMigration(SUCCEEDED)` 同一事务内 `lockAgentApp` + 切换 `active_config_version`，无中间态。 |
| TencentDB 共享群组泄露 | `ResolveSessionIngestor` 对 principal != user 返回 nil，跨成员隔离正确。 |
| Worker 资源释放 | runner defer 释放、ephemeral runner 策略、事件通道排空至关闭、`cancelManagedRunnerOnContextDone` 均符合规范。 |
| Knowledge importer 补偿 | COS Put 失败→SQL 失败→`context.WithoutCancel` 补偿删除，补偿失败时 `errors.Join` 上报不吞错。 |
| 迁移 Copier 资源释放 | 专用 SummaryImporter 连接池 `Close()` 释放，重复 Close 安全。 |

---

## 优先级建议

1. **A 优先修**：数据静默丢失 + 校验机制结构性失明，影响所有长会话迁移。
2. **E 与 A 同根同修**：都源于"迁移读路径依赖框架的默认/降级行为"，读路径需要平台显式接管存在性与全量语义。
3. **B 其次**：多库检索在真实数据分布下几乎必然触发。
4. **C 尽快环境验证**：验证成本一个本地 Qdrant + 两次 gRPC 调用；若成立则多租户知识检索在第二租户上线即坏。
5. **D 与 A 合并处理**：同一个"迁移读路径需绕过框架加载限制"的根因。
