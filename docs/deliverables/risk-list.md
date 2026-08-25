# 生产风险清单

| 风险 | 缓解措施 |
| --- | --- |
| 跨租户数据泄漏 | 所有主键、缓存 key、对象路径、向量过滤都包含租户作用域；增加隔离测试和审计查询 |
| IM 回调伪造 | 校验签名、时间窗、nonce、corp_id/agent_id 或 bot token 归属；只信已启用 Channel Binding |
| 重复消息重复执行 | 使用 Inbox 唯一键和 payload hash；重复且内容一致返回原 request_id，冲突则拒绝 |
| 同一 Session 并发覆盖 | Queue 按 session 分区，或 SQL Job Outbox 事务 claim；不同时使用多套锁 |
| Session/Event 顺序错乱 | 使用 append-only event 和递增 event_seq；State 更新带 event_seq 幂等 |
| Memory / Vector 索引延迟 | 先写权威 Memory Store，再异步写向量索引；检索时补查最近权威记录 |
| Tool 重复副作用 | 非幂等 Tool 必须带业务幂等键；执行前检查权限、预算和危险等级 |
| Artifact 越权访问 | 对象读取前先查 SQL metadata；使用短期 URL；对象路径不可作为权限依据 |
| 密钥或 PII 泄漏 | 只保存 secret_ref；日志、trace、错误报告脱敏；禁止记录 token、DSN、完整 PII 和原始 Tool 参数 |
| 成本失控 | 按租户、应用、用户设置 RPM、TPM、日预算、Tool 次数和成本告警 |
| IM 平台限流 | Reply Outbox 退避重试；遵守 Retry-After；长回复分片；失败进入 DLQ |
| 后端迁移丢数据 | 快照、增量、checksum 校验、短暂停写切换、旧源保留回滚窗口 |
| Worker 崩溃 | Job 未完成由队列或 SQL Outbox 重投；execution 状态可恢复；Reply Outbox 独立重试 |
| Worker 取消泄漏 | `context.Context` 取消后仍排空 Runner Event Channel；统一管理 goroutine 和 shutdown |
| 配置发布错误 | 配置版本不可变；灰度发布；监控错误率、延迟和成本；回滚只切 active version |
| 观测数据不完整 | trace_id 贯穿 IM callback、Gateway、Worker、Runner、Tool、Storage 和 Reply；采集失败不阻塞主链路 |
