# 核心链路风险清单

| 风险 | 当前约束 |
| --- | --- |
| 跨 tenant/app 数据读取 | SQL、Redis key、对象 metadata 和 Qdrant filter 都带可信 tenant/app scope |
| 伪造 IM callback | Feishu/WeCom 在 Adapter 内验签、解密并复核 Binding 状态和 revision |
| 重复入站执行 | Inbox unique key + payload hash；重复请求复用原 request |
| 配置漂移 | Admission 固定 immutable config version；Worker 只解析该版本 |
| 同一 Session 并发覆盖 | PostgreSQL turn_seq + Execution Lease + Redis Session Lock |
| 节点崩溃或 Lease 丢失 | Redis Pending 重领；PostgreSQL 条件更新拒绝失效 owner；本地 Runner 取消并排空 events |
| 媒体越权或 bytes 泄漏 | metadata authorization + ArtifactRef/version；Queue、Session、Log 只保存引用 |
| Secret 泄漏 | 只保存 scoped SecretRef；运行时由 SecretProvider 解析，错误信息不含 secret |
| 知识库越权 | Qdrant 检索前后均固定 tenant/app/config/knowledge-base scope，SQL Catalog 校验可用版本 |
| metadata 写入失败 | 立即 best-effort 删除精确 object；删除失败只留下 orphan，不影响主链授权 |
| IM 回复重复或丢失 | Reply Outbox 唯一键、发送租约、Binding Revision 复核和 transient retry |
| Reply target 过期 | 发送前重新解析并校验 Binding revision、目标范围和解密结果 |
| Recall 竞态 | verified recall 锁定 Binding/Execution；写 durable CANCELED；Worker Prepare 和 Lease loss 双重兜底 |
| 工具越权 | Runtime visibility check + Worker execution allow/deny；策略来自固定 tenant/app/config |
| Runner goroutine 泄漏 | Worker 持续 drain event channel 到关闭，再 Close Runner；Context 取消仍保留排空 |
| 后端切换混写 | data migration admission gate 排空旧执行、复制校验后切换 immutable version |
