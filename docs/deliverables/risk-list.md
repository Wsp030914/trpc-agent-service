# 生产风险清单

| 风险 | 缓解措施 |
| --- | --- |
| 跨租户数据泄漏 | 所有主键、缓存 key、对象路径、向量过滤都包含租户作用域；增加隔离测试和审计查询 |
| IM 回调伪造 | 校验签名、时间窗、nonce、corp_id/agent_id 或 bot token 归属；只信已启用 Channel Binding |
| 重复消息重复执行 | 使用 Inbox 唯一键和 payload hash；重复且内容一致返回原 request_id，冲突则拒绝 |
| 同一 Session 并发覆盖 | PostgreSQL 按 Session 分配 `turn_seq`，Redis Session Lease 串行化 Runner，PostgreSQL `run_token` 条件更新保护 Execution 状态；本方案不保证 Lease 丢失后旧 Runner 的最后一次 Session 写入被拒绝 |
| 旧 Worker 在失去 Lease 后继续写 Session | 取消旧 Runner 并保护 Execution 状态；Session Provider 的严格旧写入拒绝不属于本方案，作为显式残余风险接受 |
| Session/Event 顺序错乱 | 使用 PostgreSQL `turn_seq`、Redis Session Lease、append-only event 和递增 event_seq；Execution 状态更新由 `run_token` 条件保护 |
| 认证、配置切换和入队发生竞态 | 单个 Admission 事务复核 Credential/Tenant/App、锁定 active config、处理幂等、分配 turn 并创建 Execution/Dispatch Outbox |
| 协议入口绕过队列直接执行 | tRPC-Agent-Go `server/*` 只注入 QueuedRunner；真实 Runner 仅由 Worker 持有；增加入口契约测试 |
| Memory / Vector 索引延迟 | 先写权威 Memory Store，再异步写向量索引；检索时补查最近权威记录 |
| Tool 重复副作用、审批参数漂移或结果未知 | 非幂等 Tool 必须带业务幂等键；审批保存精确参数与摘要、仅执行一次；Tool Executor 以 `approval_id` 幂等写结果；结果未知不自动重试，由人工处置后恢复关联 Session |
| Artifact 越权访问 | 对象读取前先查 SQL metadata；使用短期 URL；对象路径不可作为权限依据 |
| 密钥或 PII 泄漏 | 入站 API Key 只保存 digest 且原文只展示一次；可取回 Secret 只保存 `secret_ref`；日志、trace、错误报告脱敏 |
| 成本失控 | 按租户、应用、用户设置 RPM、TPM、日预算、Tool 次数和成本告警 |
| IM 平台限流 | Reply Outbox 退避重试；遵守 Retry-After；长回复分片；失败进入 DLQ |
| 后端迁移丢数据或新旧写入混杂 | `MIGRATING` 暂停新请求、排空旧 Job、全量复制校验后切换；HTTP/RPC 返回可重试错误，IM ACK 并提示维护，不延后执行迁移窗口输入；失败保持旧配置恢复服务 |
| Worker 崩溃 | Redis Pending 由 Consumer Group 重领，过期 Execution 租约由 PostgreSQL 恢复并重新发布；Execution 状态可恢复 |
| Worker 取消泄漏 | `context.Context` 取消后仍排空 Runner Event Channel；统一管理 goroutine 和 shutdown |
| 配置发布错误或后端直接切换 | 配置版本不可变；模型、工具和策略可灰度发布；权威后端变更必须经 `MIGRATING` 排空、复制校验后切换；回滚只切对应安全版本 |
| Job 已完成但 IM 回复未入队 | Job/Execution 终态、Reply Outbox 和必要 Audit 在平台协调库按当前 lease 条件同事务提交 |
| 观测数据不完整 | trace_id 贯穿 IM callback、Gateway、Worker、Runner、Tool、Storage 和 Reply；采集失败不阻塞主链路 |
| 审计事件随遥测丢失 | Audit Event 写入权威 Audit Store，跨后端使用事务 Outbox；Telemetry Collector 不作为唯一副本 |
