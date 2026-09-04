# 生产风险清单

| Risk | Impact | Detection | Mitigation | Recovery |
| --- | --- | --- | --- | --- |
| Worker 执行中崩溃 | 当前 Redis delivery 未 ACK，执行中断；外部副作用存在 at-least-once 重复风险 | execution lease expiry、Redis pending、Worker restart、Trace 中断 | PostgreSQL 条件 Claim + expiring Execution Lease；Redis pending reclaim；Session Lock TTL；Tool 使用稳定 idempotency key（若支持） | 等待 Lease/Lock 过期，由其他 Worker 重新验证状态并接管；已完成 Execution 不再运行 |
| Redis 不可用 | Stream 收发、Session Lock、Reply rate limit 失败，队列暂时不能推进 | Redis ping、命令错误率、queue lag、SafeError | Gateway/Worker 不改用本地队列；Relay/Worker 使用 context-aware 指数退避+jitter | 恢复 Redis 后 relay/consumer 继续；以 pending/Lease 语义恢复，不 busy loop |
| PostgreSQL 不可用 | Admission、Execution 状态、Inbox/Outbox 不可安全提交 | PG ping、连接池/SQL 错误、Admission error rate | Gateway 返回 retryable error，不创建内存中的半条 Execution；后台循环退避 | 上游重投，Inbox 幂等保证只生成一个有效 Execution；数据库恢复后重新 Admission |
| Model timeout 或 provider 卡住 | 执行延迟、资源被长期占用 | model latency p95/p99、timeout/error、Trace deadline | `TRPC_AGENT_SERVICE_MODEL_TIMEOUT` 派生 context；Runner event drain 后 Close；复用 provider 既有重试 | context cancel 后收口 Execution，释放 Session Lock；按既有失败语义结束，检查 provider 配额/网络 |
| Tool 外部副作用重复 | 重复扣费、写入、通知或调用第三方系统 | Audit、Tool error/latency、第三方 idempotency 日志 | 不对已启动 Runner 做盲目自动 retry；Tool 有幂等能力时传稳定 key；Permission/Human Review/Budget 仍生效 | 用第三方 idempotency key 或人工审计补偿；无幂等接口时明确接受 at-least-once 风险 |
| IM 重复投递 | 同一用户消息被重复处理或回复 | Inbox conflict/重复 event 指标 | 继续使用 message identity + Inbox dedupe + 同一有效 Execution，禁止第二套去重 | 重复事件复用已有 Admission/Execution；核查 provider 重连后的重复投递原因 |
| IM provider 限流或发送失败 | Reply 延迟、Reply Outbox backlog | reply failure/retry、HTTP 429、provider `Retry-After` | Reply Outbox 按行 bounded retry、指数退避+jitter、尊重 Retry-After、永久错误标 failed | 限流窗口后继续发送；超过上限的 failed row 进入运维/人工处理，不无限循环 |
| 配置发布错误 | 新 Execution 使用错误模型、工具或治理策略 | config version 维度 Trace/Audit、错误率、canary 观测 | AppConfig Version immutable；先小范围 canary；Execution Admission 时 pin 版本 | 仅切换 `active_config_version` 回历史稳定版本；已 Admission 的执行保持原 pinned version |
| Schema 与旧 Worker 不兼容 | 混合版本期间旧 Worker 读写失败或状态错乱 | rollout error、SQL error、旧版本 Pod 日志 | `expand → deploy → migrate/use → contract later`；上线前验证 Stream/state/outbox 兼容 | 停止扩容新版本、回滚镜像；保留已 expand schema，待兼容恢复后处理 |
| 高峰流量造成 queue backlog | 延迟升高、Lease/retry 增加、IM 超时 | Redis group lag/pending、running executions、CPU/Memory、p95 | HPA 结合 queue lag/running/CPU；容量实测加 30%～50% headroom；热点 Session 观察 | 扩 Worker、限流/降级非关键请求、等待队列清空；不突破 Session Lock 顺序 |
| Telemetry backend 不可用 | 观测缺口，不应阻塞业务 | Collector exporter errors、missing traces/metrics | Collector 与业务关键路径隔离；应用可 no-op telemetry 降级；告警监测 exporter | 恢复 collector/backend 后重新导出新数据；用 Audit/DB/Redis 记录补充事故窗口 |
| Secret 泄漏 | provider、数据库或 IM 凭据被滥用 | secret scanning、访问审计、异常 provider 调用 | 仅保存 SecretRef；Secret/external injector 注入；SafeError/Trace/Audit redaction；最小权限与轮换 | 立即吊销/轮换、审查访问、重部署并排查日志/镜像/ConfigMap；必要时通知受影响方 |
