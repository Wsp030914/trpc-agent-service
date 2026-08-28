# 故障恢复与运维详细设计

> 开发用设计草稿，不作为题目交付物。正式交付物以 `docs/deliverables/` 为准。

## 1. 失败恢复与重试

所有重试沿用原 `request_id` 和 `turn_seq`，不创建新的会话轮次。

| 故障 | 恢复策略 |
| --- | --- |
| Worker 崩溃 | Job 租约到期后由其他 Worker 重领并重试。 |
| Gateway 或 PostgreSQL 短暂不可用 | 不 ACK IM 回调，交由通道重投；HTTP/RPC 返回可重试错误。 |
| Worker 无法续租或持久化 | 取消 Runner，不写成功终态，等待租约到期后重试；`run_token` 阻止旧 Worker 覆盖 Execution 状态，旧 Runner 的最后一次 Session 迟到写入是明确接受的残余风险。 |
| 模型超时、429、5xx | 限次数指数退避重试。 |
| 模型认证或参数错误 | 不重试，标记失败。 |
| 可重试且幂等的 Tool | 限次数重试。 |
| 非幂等 Tool 网络结果未知 | 不自动重试，记录 `UNKNOWN`，保持关联 Job 和该 Session 后续 turn 等待；人工处置并持久化结果后才恢复。 |
| IM 回复发送失败 | 只重试 Reply Outbox，不重新执行 Agent；超过次数进入 DLQ。 |
| 已验证的 IM 消息撤回 | `PENDING` Job 取消；等待审批的 Job 取消并使审批失效；运行中请求取消；已发生的模型、Tool、Session、Memory 或回复副作用不自动回滚，只追加 Audit。 |

Job 与外部 Tool 副作用的总体语义为至少一次。需要恰好一次效果的业务 Tool 必须由目标业务系统使用业务幂等键保证。

## 2. 取消、goroutine 生命周期与优雅停机

每个长期 goroutine 都由服务根 Context 和 WaitGroup 管理：

```text
服务根 Context
-> Gateway / Channel Adapter 接收循环
-> Worker claim 循环
   -> 单个 Job 的 Runner、续租、Event Sink
-> Reply Outbox 投递循环
```

收到终止信号后的顺序为：

1. 进程变为 not-ready，Gateway 和 Worker 停止接收新请求、停止 claim 新 Job。
2. 已运行 Job 在优雅停机窗口内继续执行并续租，避免被其他 Worker 重复执行。
3. 窗口结束仍未完成时，取消 Job Context；支持 `runner.ManagedRunner` 时调用其取消能力。
4. 取消后继续消费 Runner Event Channel 到关闭；每次 Event Sink 调用有独立超时，不能阻塞排空。
5. Event Channel 关闭后释放 Session Lock、停止续租、等待子 goroutine 退出，再结束进程。

用户显式取消的 Job 标记为 `CANCELED`，不重试。节点停机、锁或租约丢失、超时取消不得写成功终态，后续由租约机制恢复。Lease 丢失后的旧 Runner 可能短暂继续运行，因此 Worker 立即取消其 Context 并继续排空 Event Channel；本方案不要求目标 Provider 以原子令牌拒绝最后一次 Session 迟到写入。Runner 在停机截止时间内仍不关闭 Event Channel 时，进程记录异常并退出，让租约失效后由其他节点重试。

## 3. 灰度发布与配置回滚

AppConfig 不可变。发布创建候选版本，Gateway 在准入时选择版本；Job 入队后固定 `config_version`，Worker 不读取执行时的最新配置。

灰度按稳定的 Session 分区键确定性分桶：

```text
hash(tenant_id + app_id + session_principal_id + session_id)
-> 候选版本或稳定版本
```

同一 rollout rule 和比例下，同一 Session 始终选择同一版本，避免在稳定规则内模型或工具策略来回切换；不同 Session 可以按比例进入候选版本。扩大比例、降低比例或回滚属于显式路由变更，后续新 Job 可以重新选择稳定或候选版本；已入队和运行中的 Job 保持其固定版本。根据错误率、延迟、成本和 Tool 拒绝率逐步扩大比例。

回滚只改变新请求的版本选择规则。已入队和运行中的 Job 继续使用固定版本。数据后端不参与灰度，仍通过数据同步专题中的受控迁移切换。发布、比例变更和回滚写入 Audit Log。

## 4. 容量评估与部署

容量由压测实测，不预设固定节点数。初步估算为：

```text
所需 Worker 并发 >= 峰值已接收消息速率 × p95 Agent 执行时长
```

压测后根据余量确定实际配置，并持续观察模型 token 速率、Tool 耗时、Redis Session Lease 等待、Redis Stream Pending/积压、PostgreSQL QPS、对象/向量库吞吐和 IM 回调峰值。Worker 在 Runner 生命周期内持有 Redis Session Lease，PostgreSQL 连接池不再因 Session 锁连接直接限制并发；Redis 连接、Stream Pending 和模型配额仍共同限制容量。

最小可运行部署：

```text
Docker Compose
-> 一个 all 角色服务：Channel Adapter + Gateway + Worker + Admin API
-> 一个 PostgreSQL：配置、Execution、默认 PostgreSQL Session、Inbox、Outbox、Audit
-> 一个 Redis：Dispatch Stream、Consumer Group Pending 重领、Session Lease
-> 可选 OTel Collector
```

生产部署：

```text
Channel Adapter / Gateway：按入站流量横向扩缩
Worker：按队列积压和执行并发扩缩
Admin API：独立部署
PostgreSQL：高可用托管或主备
对象存储、向量库、Secret Manager、OTel Collector：独立服务
Kubernetes：Deployment + HPA + readiness/liveness + 优雅终止窗口
```

Gateway、Worker 和 Admin API 独立扩缩容和故障隔离。密钥运行时注入，不进入镜像。压测至少覆盖高峰 IM 回调、长模型响应、慢 Tool、数据库短暂故障和滚动发布。
