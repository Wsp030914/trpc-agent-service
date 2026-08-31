# trpc-agent-service 项目长期记忆

## 项目定位
腾讯赛题实现：基于 tRPC-Agent-Go v1.11.2 的「多租户、节点化、多后端数据同步、可接 IM」的 Agent 部署平台。
- 模块名 `github.com/liuzengh/trpc-agent-service`，Go 1.24.1，`cmd/trpc-service` 为唯一入口。
- 不是单个 Agent 进程，而是把 tRPC-Agent-Go 能力平台化：租户路由、隔离、协调、运营由平台补；编排/Session/Memory/Tool/Runner 全部复用框架。
- 规模：约 91 个 .go 文件、11.5k 行非测试代码、20 篇文档（docs/deliverables 为交付物，docs/dev 为内部设计）。

## 五个逻辑面（架构核心划分）
控制面（Admin API + 版本化配置 + 凭据）、数据面（Channel Adapter / 协议入口 → Gateway → Outbox → Stream → Worker → Runner）、
状态面（PostgreSQL 协调库 + 租户可选后端）、密钥面（SecretRef + KMS/环境变量）、观测面（OTel + SQL Audit）。

## 主链路（必记）
IM/HTTP 入口（验签或认证）→ Gateway 原子准入（单 PG 事务）→ execution + dispatch_outbox → Relay 发到 Redis Stream
→ Worker Consumer Group 收 → 条件 Claim（run_token + turn 顺序）→ Redis Session Lease 串行 → runtime 装配真实 Runner
→ 排空 Event Channel → PG 条件写终态 + Audit + execution_event → XACK。

## 三条并行的所有权机制（不要混淆）
1. `turn_seq`（PG session_lane）：分配消息顺序，保证后一轮不越过前一轮。
2. `run_token` + `lease_owner` + `lease_until`（PG execution）：保护 Execution 状态、Event Journal、Audit 的条件更新，过期 Worker 不能覆盖。
3. Redis Session Lease：覆盖 Runner 生命周期，实际串行化同一 Session 执行；续租失败 → cancel Runner + 继续排空。
明确接受的残余风险：Lease 丢失后旧 Runner 的最后一次 Session 写入无法被拒绝（未向 Session Provider 传写入栅栏）。

## 关键设计约束（改代码前先看）
- 租户身份只能来自认证 claims / 已验签 Channel Binding，绝不信任 payload。HTTP API Key 请求固定 `session_principal_id = "service:<credential_id>"`。
- 配置版本不可变（DB 有 BEFORE UPDATE/DELETE 触发器拒绝修改）；`data_migration` 才能切换权威后端。
- `internal/execution.Job` 是私有不透明类型，外部无法绕过 Admission 构造任务。
- 协议 Server 只注入 `QueuedRunner`，真实 Runner 只存在于 Worker。
- 平台 `platform` schema 只管协调表；框架 Session 表在选定 DSN 的 `agent` schema，由 Provider 自己准备。
- 日志/trace 不得出现 token、DSN、API key、完整 PII、原始 Tool 参数。

## 依赖方向
`ingress/channels → auth → gateway → postgres → relay → redis → worker`；`worker → runtime → session|storage|secret|tool`；`admin → postgres`。
gateway 不依赖 worker，worker 不依赖协议入口，postgres 不依赖任何具体 Session/Memory/Knowledge/Artifact Provider。

## 工程约定（AGENTS.md）
- 新增/修改公共 API 必须做「二次设计评审」9 项检查。
- 用 `event.Event.IsRunnerCompletion()` 判完成、`runner.ManagedRunner` 做取消，不自建框架事件标记或生命周期 API。
- 内存 Provider 只允许测试和本地开发；生产 Worker 必须共享后端。
- 测试要覆盖公共契约，不是只跑代码。
