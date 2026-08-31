# IM 接入 Implementation Spec

状态：规范正文已冻结；实现进度不属于规范  
阶段：规格定义器已完成  
适用仓库：trpc-agent-service  
基线：以当前工作树源码和 Schema 为准；本阶段不把需求文档中“计划存在”的能力视为已实现。

本文件是企业微信（WeCom）和飞书（Feishu）IM 接入的实现规格。它只定义
实现边界、数据契约、事务语义、验收和落点，不包含代码实现。

原始需求文档 `docs/dev/需求.md` 对 IM 明确要求：支持至少两类 IM，且至少包含微信
或企业微信；设计 Channel Adapter、消息与 Agent Event 的双向转换、账号与租户绑定、
验签、去重、身份映射、群聊/单聊 Session 隔离，并考虑消息长度、频率限制、异步回复、
图片/文件消息、撤回和失败重试。本 Spec 将真实 Provider 固定为企业微信和飞书，
并将两者的单聊、群聊入站纳入本需求验收；企业微信采用能够提供群聊入站的 AI Bot
产品形态。

本次最终一致性修订冻结了三项执行规则：

- IM-01 只负责 public route、Binding trusted scope 和 Admission，不负责任何
  Provider 协议验签、解密或字段提取；
- 串行开发顺序为 IM-01 -> IM-04 -> IM-05 -> IM-02 -> IM-03 -> IM-06 -> IM-07；
- IM-04 使用“数据库密文 target envelope + 内部 target_ref”作为 v1 Provider
  target 持久化方案，复用现有 SecretProvider 取密钥，不引入 KMS/Vault。

本次 Provider 调研进一步冻结四项实现选择：

- 企业微信使用“智能机器人（AI Bot）API 模式”的 HTTP URL 回调协议，不使用普通
  自建应用回调来承担本需求的群聊入站；仓库不引入第三方 Go AI Bot 运行时封装，
  也不引入 Python runtime 或 sidecar，而是在 `channels/wecom` 内实现窄协议适配器。
- 企业微信官方 Node/Python SDK 只作为协议、消息类型和行为参考，不作为 Go 运行时
  依赖；社区项目只作为调研或 fixture 参考，不能成为安全边界。
- 飞书 HTTP 事件接入使用官方 `github.com/larksuite/oapi-sdk-go/v3`，本次规格
  修订参考 v3.11.0；高层 WebSocket Channel SDK 不作为 HTTP webhook 入站 transport。
- Provider SDK 只负责协议 DTO、校验、编解码和一次出站 API 调用；完整 Reply
  Outbox、Claim、Lease、Retry 生命周期统一由 IM-06 负责。

## 1. Executive Summary

当前仓库已经具备多租户 RuntimeContext、可信租户来源枚举、Gateway 原子
Admission、PostgreSQL Execution、Dispatch Outbox、Session Lane、Worker
调用 Runner 和排空 Runner Event Channel 的基础能力。IM 接入缺口集中在：

1. 没有从公网 webhook 路由到 Binding 的安全定位能力；
2. PostgreSQL Store.Admit 明确拒绝 verified channel binding；
3. 没有企业微信、飞书 Adapter；
4. 没有 Identity / Conversation 的持久化映射；
5. 没有 channel_inbox 和 reply_outbox；
6. 当前 Gateway/Worker Runner 边界虽然有 ArtifactRefs 字段，但 Validate 会
   拒绝，尚未形成从 IM 媒体下载、Artifact 持久化到 Runner 输入的完整链路。

本 Spec 的核心决策：

- 公网入口使用不透明、全局唯一的 public_route_id；binding_id 继续作为
  tenant/app 作用域内的内部资源 ID。
- IM-01 的 route_key 只负责定位 Binding，不负责最终授权；Provider 验签、
  解密和 external_account 提取由 IM-02/IM-03 负责。最终可信租户必须来自
  已验证 Binding，并在 PostgreSQL Admission 事务内重新校验 Tenant、App、
  Binding 和 ACTIVE 状态。
- 企业微信和飞书各自拥有协议 Adapter；共享的是平台级 Channel Input、
  Identity/Conversation 映射、Gateway、Inbox、Reply Projection 和 Outbox，
  不做一个包含两个协议全部分支的“大 Adapter”。
- 本 Spec 的最终验收必须覆盖文本、图片、文件、卡片/流式回复、异步回复、分片、
  限流、失败重试和撤回。实现过程可以按任务逐步提交，但不得把文本阶段的临时状态
  变成最终功能边界。
- Provider Adapter 只提取并校验媒体引用，并提供受 Binding 作用域保护的媒体 client；
  IM-05 先冻结预入站 `AttachmentIngestor` 契约，IM-07 最终实现入站媒体下载/解密、
  Artifact 持久化和出站附件处理。完整运行时必须在 IM-05 Admission 前得到
  `ArtifactRef`，在 Reply Projection 后处理出站附件；不能把当前已有 ArtifactRefs
  字段误标为已支持。
- 入站 Inbox、Execution 和 Dispatch Outbox 必须在一个 PostgreSQL 事务中
  提交。提交成功才返回外部 HTTP ACK；丢 ACK 后的 Provider 重试命中同一
  Inbox，不创建第二个 Execution。
- Reply Outbox 独立于 Dispatch Outbox。发送重试只重试发送操作，不重新执行
  Runner。
- IM-02/IM-03 只提供入站协议实现、平台级出站 codec/client contract 和 fake；
  完整 Reply Sender lifecycle、Provider error classification、Claim/Retry
  全部属于 IM-06。

## 2. Verified Repository Facts

### 2.1 工作区基线

在本阶段开始时已执行 git status --short。工作区不是干净状态，存在多处
与 IM 无关的已修改文件和未跟踪文件，包括数据迁移、Artifact、Knowledge、
Session、PostgreSQL 拆分以及 cmd/trpc-service 变更。本阶段未覆盖、回滚、
格式化或修改这些变更；新增本 Spec 文档是唯一计划中的文档产物。

以下结论以当前工作树中的文件为准，而不是只以 HEAD 为准。当前工作树中
trpcservice/postgres/admission.go 是现有 Admit 实现所在文件，且相关
PostgreSQL 拆分仍有未提交变更。

### 2.2 状态定义

- VERIFIED：代码已存在，且对本行所描述的能力行为符合需求。
- PARTIAL：有领域模型或基础能力，但缺少 IM 所需的持久化、协议或边界。
- MISSING：当前仓库没有对应实现或 Schema。
- CONFLICT：当前代码行为与需求契约不一致；文档内部设计冲突另在架构决策中说明，
  不改变 Repo Facts 的状态含义。

### 2.3 Repo Facts 表

| Capability | Status | Current Implementation | Required Change | Target IM Task |
| --- | --- | --- | --- | --- |
| channels.Binding | PARTIAL | trpcservice/channels/channels.go 已有 TenantID、AppID、BindingID、Channel、ExternalAccount、WebhookURL、TokenRef、SigningSecretRef、Secret、Status；Validate 支持 ACTIVE/SUSPENDED | 增加 public_route_id、binding_revision；定义授权属性变更递增规则、路由轮换和 provider secret 的使用边界；保留现有 scoped binding_id | IM-01 |
| channels.Identity | PARTIAL | 已有 TenantID、AppID、BindingID、Channel、ExternalUserKeyHash、UserID、Status、KeyVersion | 增加持久化 Repository、唯一约束、并发首次创建、加密 provider target 引用 | IM-04 |
| channels.Conversation | PARTIAL | 已有 TenantID、AppID、BindingID、Channel、ExternalChatKeyHash、ThreadKeyHash、ConversationID、SessionPrincipalID、Scope | 增加群聊/话题持久化、唯一约束、成员映射和 provider target 引用 | IM-04 |
| gateway.Message | PARTIAL | 只有 Text 和 ArtifactRefs；Validate 要求 Text 非空，并明确拒绝非空 ArtifactRefs | 扩展为“文本或受控 ArtifactRef 至少一种”的平台输入契约，并完成 Worker 到 tRPC-Agent-Go model.Message 的真实转换、权限和失败语义 | IM-05、IM-07 |
| gateway.TenantSourceVerifiedChannelBinding | VERIFIED | gateway/gateway.go 已有可信来源枚举；AdmissionIdentity.Validate 要求 Channel 和 BindingID | 让 PostgreSQL Admission 真正接受并重新验证该来源 | IM-01 |
| Gateway | PARTIAL | Gateway.Handle 要求 AdmissionIdentityResolver，把请求交给 Admitter；不做进程内入队 | 保持入口职责；让 channel input 映射到现有 Request/AdmissionRequest；trusted source/revalidation 由 IM-01 建立，Inbox 原子写入由 IM-05 扩展 | IM-01、IM-05 |
| Worker | PARTIAL | worker.Worker.Prepare/Run 已解析配置、锁 Session、调用 Runner；持续 range 消费 Event Channel，并使用 IsRunnerCompletion；ManagedRunner 可取消 | 增加平台级 Reply Projection/Reply Sink；不加入企业微信或飞书客户端 | IM-06 |
| Runner | VERIFIED | runtime.RuntimeRunnerResolver 解析 tRPC-Agent-Go runner.Runner；Worker 调用 Run；已使用 runner.ManagedRunner 做取消 | 复用现有 Runner API；不要在 Adapter 内直调 Runner | IM-06、IM-07 |
| Dispatch Outbox | VERIFIED | platform.dispatch_outbox 与 Claim/Complete/Retry/Recover 流程存在，用于 Execution 到 Worker 的可靠分发 | 不复用为回复队列；保持其生命周期和字段语义 | IM-05 |
| ResolveBinding | PARTIAL | config.BindingResolver 和 postgres.Store.ResolveBinding 都要求 tenant_id + app_id + binding_id，SQL 按三列精确查找 | 新增按 public_route_id 定位的窄接口；保留现有 exact-scope API | IM-01 |
| postgres.Store.Admit | CONFLICT | 在事务内校验 credential、Tenant、App、Config、幂等 Execution、Session Lane、Execution、Dispatch Outbox；但非 authenticated_claims 直接返回 ErrUnsupportedAdmissionSource | IM-01 增加 verified channel trusted source、BindingRevision/授权快照重校验；IM-05 在该分支内扩展 Inbox 幂等及 Inbox/Identity/Execution/Dispatch 原子写入 | IM-01、IM-05 |
| WeCom Adapter | MISSING | trpcservice/channels/wecom/doc.go 只有包说明，没有 HTTP、验签、解密、标准化或发送实现 | IM-02 实现企业微信 AI Bot URL 回调协议、标准化、provider media reference 提取、Gateway submission、ACK，并提供供 IM-06 使用的一次出站 codec/client contract 和 fake；媒体下载/解密及 Artifact ingest 由 IM-07 编排；完整 Reply Sender lifecycle 在 IM-06 | IM-02 |
| Feishu Adapter | MISSING | trpcservice/channels/feishu/doc.go 只有包说明，没有事件订阅、验证、标准化或发送实现 | IM-03 使用官方 `oapi-sdk-go/v3` 完成 HTTP 事件、challenge、验证、provider media reference 提取、标准化、Gateway submission、ACK，并提供供 IM-06 使用的一次出站 codec/client contract 和 fake；媒体下载及 Artifact ingest 由 IM-07 编排；完整 Reply Sender lifecycle 在 IM-06 | IM-03 |
| channel_inbox | MISSING | migrations/000001_platform.sql 没有表，仓库没有 Inbox Repository | 新增 tenant/app/binding/external_message_id 唯一键、payload hash、request_id、消息级 reply target envelope，以及 ADMITTED/已验签不支持类型或永久不支持媒体的 REJECTED 语义 | IM-05、IM-07 |
| channel_recall_inbox | MISSING | 当前没有撤回事件的持久化幂等记录 | 新增 tenant/app/binding/external_event_id 唯一键，与取消/审计动作同事务提交；不引入通用事件恢复框架 | IM-07 |
| reply_outbox | MISSING | 当前只有 Dispatch Outbox，没有回复投递记录、Claim、Lease 或 Sender | 新增独立 Reply Outbox、source event 幂等键、logical reply/revision、有限 Claim/Lease/Retry | IM-06 |
| reply_projection_state | MISSING | 当前没有 Reply Projection 顺序游标或投影状态表 | 新增按 tenant/app/binding/request scope 的最小 event_seq 游标，防止乱序/重放产生过时回复 | IM-06 |
| execution_event | VERIFIED | platform.execution_event 已存在，按 tenant/app/request/event_seq 持久化事件 | 作为跨节点事件/回复投影输入；只投影用户可见内容 | IM-06 |
| Session Lane | VERIFIED | platform.session_lane 以 tenant/app/session_principal_id/session_id 唯一，并由 Admit 分配 turn_seq | 复用群聊、话题和单聊的顺序保证 | IM-04、IM-05 |
| RuntimeContext | PARTIAL | tenant.RuntimeContext 已包含 TenantID、AppID、ConfigVersion、Channel、BindingID、SessionID、SessionPrincipalID、UserID、TraceID；Validate 要求全部有效 | IM resolver 必须提供可信的 active config version 快照，Admission 再读取最终版本 | IM-01、IM-04 |
| Artifact boundary | CONFLICT | gateway.Message 有 ArtifactRefs 字段，但 Validate 明确返回 artifact refs are not supported by runner boundary；Worker 当前只构造 model.NewUserMessage(Text) | 在 IM-07 内完成 Artifact ingest、Gateway 校验、Execution 持久化和 Worker 到 Runner 的受控输入转换；二进制永不进入 Session event | IM-05、IM-07 |

### 2.4 已确认的 ResolveBinding 路由缺口

当前 ResolveBinding 的契约是：

    ResolveBinding(ctx, tenantID, appID, bindingID)

其数据库主键也是：

    tenant_id + app_id + binding_id

外部 webhook 初始只有 URL 中的 channel 和 public route token，不能可信地
提供 tenant_id/app_id。因此，直接从公网请求调用当前 ResolveBinding 不成立；
不能让 Adapter 从 payload 中补 tenant_id/app_id，也不能依赖一个可能跨租户
重复的 binding_id。

### 2.5 Gap List

1. channel_binding 没有全局 opaque public route，公网 webhook 无法仅凭初始
   URL 安全定位到现有 scoped Binding。
2. Gateway 已能表达 verified channel binding，但 Store.Admit 明确拒绝该来源，
   可信身份链未闭合。
3. Binding、Tenant、App 的事务内重校验目前只覆盖 authenticated claims 路径；
   IM 需要新增 Binding 锁定和 ACTIVE 检查。
4. Identity、Conversation 只有领域结构，没有 PostgreSQL 映射、唯一约束、
   并发首次创建和回复 target 保存策略。
5. 没有企业微信或飞书协议 Adapter、HTTP wiring、验签/解密、标准化和 Sender。
6. 没有 channel_inbox，现有 Execution 幂等键还没有以 Inbox 记录表达
   external_message_id 的独立入口事实。
7. Dispatch Outbox 只覆盖 Execution 到 Worker，缺少独立的 Reply Outbox 和
   回复 Claim/Lease/Retry。
8. Worker 能运行并排空 Runner 事件，但没有面向 IM 的 Reply Projection。
9. gateway.Message 的 ArtifactRefs 当前被 Validate 拒绝，Worker 也没有把
   ArtifactRef 转成 Runner 可消费输入；附件能力不能标为已支持。
10. Provider 的确切事件版本、字段、加密配置、大小限制和错误码不在仓库中，
    必须在 Adapter 实现前按目标账号官方协议固定。

## 3. Architecture Decisions

### 3.1 目标链路

~~~text
Provider webhook
  -> IM-01 public_route_id lookup
  -> Binding Channel/ACTIVE + trusted scope
  -> IM-02/IM-03 provider verification/decryption/extraction
  -> VerifiedProviderEnvelope
  -> IM-07 inbound AttachmentIngestor (pre-admission media materialization)
  -> materialized platform Channel Input
  -> Gateway.Handle
  -> PostgreSQL Store.Admit transaction
       -> channel_inbox
       -> execution
       -> dispatch_outbox
  -> COMMIT
  -> provider HTTP ACK
  -> Relay / Redis Stream
  -> Worker
  -> tRPC-Agent-Go Runner
  -> execution_event / Reply Projection
  -> reply_outbox
  -> channel-specific Reply Sender
  -> Provider
~~~

IM-01 负责 route lookup、Binding 状态和 trusted scope；IM-02/IM-03 负责各自
Provider 协议、凭据验证、外部 ID 提取和协议 ACK，并把已验证的
`VerifiedProviderEnvelope` 交给预入站媒体处理与 Gateway。IM-07 的入站部分负责
把 provider media reference 变成 ArtifactRef，但必须发生在 Store.Admit 之前；这
一项实现虽在串行交付的最后完成，接口由 IM-05 先冻结。Gateway 负责可信身份
边界；Store 负责原子 Admission；Worker 负责执行和事件排空；Reply Projection
负责把用户可见结果变成平台级 Reply；IM-06 的 Reply Sender 负责具体 Provider
HTTP 调用。

### 3.2 不做一个大的 Adapter

共享抽象应是 capability-oriented 的小接口，例如：

- 入站：Provider 验证后返回 `VerifiedProviderEnvelope`，由共享 ingress 边界物化为
  标准 Channel Input；
- 出站：判断文本/卡片/文件能力并发送一个平台级 Reply；
- 绑定：读取已定位的 Binding 和 scoped secret；
- 限流：由每个 Adapter 管理自身 Provider 限制。

企业微信 AI Bot 的加解密 envelope、飞书 challenge/event envelope、各自的
target 字段、发送 API、错误码和消息长度不能进入 Gateway、Worker、Session。
一个“大 Adapter”会把协议分支、凭据生命周期和出站错误分类集中到同一包，
后续新增通道时会扩大修改面，也会让协议字段泄漏到平台层。本 Spec 选择共享
领域契约加两个独立 Adapter。

### 3.3 关键不变量

1. tenant_id 和 app_id 只能由已验证 Binding 导出，不能来自外部 payload。
2. public_route_id 只能定位候选 Binding，不能单独授权。
3. Binding、Tenant、App 的 ACTIVE 状态必须在 Admission 事务内重新确认。
4. Inbox、Execution、Dispatch Outbox 的提交点是同一个事务提交点。
5. Reply 重试不会重新调用 Runner。
6. Worker 不依赖企业微信或飞书客户端。
7. 所有数据库、缓存、对象存储和消息幂等键都带 tenant/app 作用域。
8. v1 provider target 只进入 DB 密文 envelope；原始 secret、完整 PII、原始
   provider payload 和 Tool 参数不进入普通日志。

### 3.4 tRPC-Agent-Go/OpenClaw 参考边界

已按当前 go.mod 锁定的 tRPC-Agent-Go v1.11.2 检查 OpenClaw 的 Channel、
Gateway、Registry、Telegram Channel 和 Outbound 实现。参考源码：

- [`openclaw/channel`](https://github.com/trpc-group/trpc-agent-go/blob/v1.11.2/openclaw/channel/channel.go)：最小 `Channel` 生命周期，以及可选的文本/消息发送能力；
- [`openclaw/gwproto`](https://github.com/trpc-group/trpc-agent-go/blob/v1.11.2/openclaw/gwproto/types.go)：平台无关消息、会话、线程和内容片段的字段组织；
- [`openclaw/registry`](https://github.com/trpc-group/trpc-agent-go/blob/v1.11.2/openclaw/registry/registry.go)：Channel factory、依赖注入和严格配置注册；
- [`openclaw/internal/channel/telegram`](https://github.com/trpc-group/trpc-agent-go/tree/v1.11.2/openclaw/internal/channel/telegram)：协议解析、媒体、会话 lane、流式预览和出站编码的模块拆分；
- [`openclaw/internal/outbound`](https://github.com/trpc-group/trpc-agent-go/tree/v1.11.2/openclaw/internal/outbound)：出站 target 和发送路由的能力化思路；
- [`EXTENDING.md`](https://github.com/trpc-group/trpc-agent-go/blob/v1.11.2/openclaw/EXTENDING.md)：新增 Channel 的扩展边界示例。

这些内容用于校准模块责任，不改变本服务的持久化和多租户边界：

| 上游设计 | 本服务采用 | 本服务不直接复用 |
| --- | --- | --- |
| `Channel` 与可选 Sender 能力接口 | 采用 capability-oriented 的小接口；Webhook 使用请求级 Handler，长轮询才使用后台 `Run` 生命周期 | 不强制企业微信/飞书实现无意义的 `Run` 循环 |
| `gwproto.MessageRequest` 的平台无关字段 | 借鉴字段分层和 ContentParts 思路；本服务仍以 `channels.ChannelInput`、现有 `gateway.Request` 为正式契约 | 不把 `gwproto` 作为第二套 Gateway 入口，不绕过现有 Admission |
| Registry factory | 如需注册 Adapter/codec，只复用 factory/strict decode 的模式 | 不把静态进程配置当作 DB Binding；运行时 Binding 必须由 PostgreSQL 定位 |
| Telegram 的 media/session/streaming/outbound 拆分 | 企业微信、飞书各自按协议、标准化、出站 codec/client 拆分 | 不复制 Telegram 的 provider 语义、内存 session 或文件 session store |
| OpenClaw Gateway 直接调用 Runner 并排空事件 | 继续复用当前 Worker、`runner.Runner`、`ManagedRunner` 和事件排空约束 | 不复用其无 Inbox/Execution/Dispatch 原子事务的运行路径 |
| OpenClaw outbound target router | 借鉴 target 与 channel capability 分离 | 不把 provider target 明文写入平台记录；本服务使用 `target_ref` 加密 envelope |

上游当前 tag 的实际 Channel 实现主要是 Telegram，企业微信只有扩展示例，
没有可直接接入本服务的企业微信或飞书生产 Adapter。因此，代码阶段只能复用
公开且语义匹配的框架能力；`openclaw/internal/...` 既受 Go internal 包规则
限制，也不满足本服务的多租户、Inbox、Outbox 和事务要求，不能作为运行时依赖。

### 3.4.1 Go 编码风格和命名约束

IM 新增代码的 Go 编码风格、包结构、命名和接口设计，遵循本仓库既有规范，并
参考当前锁定版本 tRPC-Agent-Go v1.11.2 的实现方式。具体要求如下：

- 使用小而明确的包和消费者侧接口；接口只表达当前真实能力，不为假设中的未来
  Provider 或通用平台预留抽象；
- 使用自然的 Go MixedCaps 命名，统一常见 initialism，避免把 Provider、部署细节
  或内部实现状态伪装成公共概念；
- 导出类型、函数和方法必须有完整 Godoc；`context.Context` 置于首参数，错误保留
  cause，生命周期、并发、取消和资源所有权必须明确；
- 按 tRPC-Agent-Go 的分层方式拆分协议解析、平台标准化、能力接口和出站编码，
  但不复制其内部实现，不引入第二套 Gateway、Session、Runner 或消息模型；
- 每个 IM 子任务完成时执行 `gofmt`、`goimports`、`go vet` 和项目 lint，并用
  tRPC-Agent-Go v1.11.2 的相邻实现检查命名和边界；风格调整不得扩大任务范围或
  改变既有公共契约。

### 3.5 减少分工的交付规则

本 Spec 默认由一个主编码 Agent 按串行顺序完成，每个任务完成后由同一个验收
流程执行单元测试、必要的 PostgreSQL 测试和 Gherkin 检查。除非后续明确要求
并行开发，不拆出独立的“重新设计契约”“单独做 Schema”“单独做 Provider
Sender”工作流。

实现阶段固定使用“主线程 + 审查 Agent + QA Agent”三方协作，不再增加独立的
清理 Agent、异常增强 Agent 或额外的 Spec Agent。审查 Agent 同时承担原清理
Agent 的检查职责，只针对当前 diff 提出最小清理建议，不把清理职责扩展成新的
架构设计：

1. 主线程负责当前子任务的编码、处理审查意见和修复 QA 失败；同一时间只允许
   主线程修改业务代码、测试和 Spec。
2. 审查 Agent 只读检查当前子任务的 diff 和相关调用方，不修改文件，不进入其他
   IM 子任务，也不重新设计完整 IM。它的反馈必须分成两类：
   - 必须处理的问题：阻塞项、重要问题和会影响安全/事务/验收的问题；
   - 独立清理建议：过度设计、重复抽象、无必要的提前扩展、兼容性收口和可维护性
     改进。
3. 审查 Agent 可以一并指出清理项，但清理项默认不自动变成当前任务的实现范围，
   不得以“建议未实现”为理由阻塞当前交付；主线程在当前任务完成后决定立即处理
   或记录到后续任务。只有实际涉及安全、数据一致性、兼容性破坏或已冻结验收
   条件时，才升级为必须修复项。
4. QA Agent 在主线程修复完成后只读验收，不修改代码或 Spec；若失败，只返回可
   复现的命令、错误和对应验收项，由主线程修复后重新交给同一个 QA 流程复验。

审查 Agent 不设置人为超时；在其返回最终反馈前保持等待。超时、关闭或没有
最终报告都不能视为 PASS。每轮交接只携带当前子任务的 Spec 章节、Repo Facts、
允许修改文件列表、当前 diff 和验收命令，避免全量上下文导致范围漂移。

跨任务只允许通过本 Spec 已冻结的契约交接，不允许下一个任务重新定义上一个
任务的模型或事务语义。每个任务提交时只需交付以下五项：

1. 变更文件清单及每个文件的责任；
2. 新增/扩展的公共契约及调用示例；
3. Schema 变更或“无 Schema 变更”的明确结论；
4. 测试命令、验收结果和未覆盖的明确非目标；
5. 给下一任务的输入、稳定 ID 和失败语义。

任务之间不得共享未冻结的临时类型、测试 fake 或隐式全局状态。Provider 包只能
实现自己的协议；平台层只能消费标准 Input/Reply/Capability，不能反向读取
XML/JSON DTO。

### 3.6 Provider 产品和 SDK 决策（冻结）

本实现固定为企业微信和飞书，并要求两者都覆盖单聊、群聊入站。Implementation Spec
完成后，两个真实 Provider
Adapter 必须覆盖本文件规定的入站和平台能力边界；测试阶段可以使用 fake，但 fake
不能替代真实协议实现。媒体下载/解密和 Artifact ingest 的编排归 IM-07，不归
入站 Adapter 的 Reply Sender lifecycle。

#### 企业微信

最终选用企业微信“智能机器人（AI Bot）API 模式”的 HTTP URL 回调，以满足当前
实现范围中的企微单聊、群聊真实接入。该产品形态提供本 Spec 所需的群聊入站能力。

`channels/wecom` 在 Go 仓库内实现窄协议适配器，不引入第三方 Go AI Bot 封装作为
运行时依赖，也不引入 Python runtime 或 Python sidecar。选型依据是官方性、协议
覆盖、transport、版本兼容和维护可验证性；Star 数只能作为辅助信号，不能作为安全
边界或依赖准入标准。已调研的社区 Go 项目存在低维护、无稳定 release、只支持
WebSocket 或 Go 版本不兼容等问题，因此只可作为阅读和 fixture 参考：

| 调研对象 | 已确认情况 | 本 Spec 处理 |
| --- | --- | --- |
| `xen0n/go-workwx` | 成熟的普通企业微信自建应用 SDK，面向普通应用能力 | 不用于 AI Bot；普通应用不能满足本 Spec 的群聊入站 |
| `wenerme/go-wecom` | 覆盖较多企业微信 API，并包含 Bot WebSocket 客户端；没有作为本服务稳定运行时依赖所需的版本/协议承诺 | 仅作 API 对照，不能替代窄适配器 |
| `go-sphere/wecom-aibot-go-sdk` | 社区 AI Bot Go 封装，主要面向 WebSocket，维护与发布信号不足 | 不引入运行时 |
| `seastart/wecom-aibot-go` | 覆盖 HTTP/WS，但 Go 版本要求和发布成熟度不适合当前模块 | 不引入运行时 |
| 官方 Node/Python AI Bot SDK | 可用于协议和行为对照，但不是 Go 依赖 | 不引入 Node/Python runtime；仅保留外部参考 |

企微 v1 固定使用 HTTP URL 回调，不使用 WebSocket 长连接作为本需求的入站
transport。原因是当前服务的入口、`route_key`、commit-before-ACK 和 PostgreSQL
Admission 都是 HTTP 请求边界；WebSocket 的连接所有权、重连和单 Bot 连接约束不
属于本需求的必要基础。若未来接入 WS，必须另立 transport/lifecycle 规格，不能在
本 Adapter 中隐式加入。

企微 URL 回调的固定实现边界是：

- GET 使用 `msg_signature`、`timestamp`、`nonce`、`echostr` 完成 URL 验证；
- POST 接收加密 JSON envelope，验证签名后解密 `encrypt`，企业内部 AI Bot 的
  `receive_id` 使用空字符串；
- 解密后的消息支持 `msgid`、`aibotid`、`chatid`、`chattype`、`from.userid`、
  `response_url`、text/mixed、非文本媒体/文件和 event 等协议内容；
- Adapter 只提取并校验 provider media reference；IM-07 再按 Binding-scoped
  client 下载、解密并写入现有 Artifact 能力，provider reference 不进入 Gateway、
  Worker、Runner 或 Session；
- 被动回复、`response_url` 主动回复、流式消息、模板卡片、媒体上传/发送都由
  `wecom` codec/client 实现为一次出站调用，Reply Outbox 的调度、Claim、重试和
  lease 仍归 IM-06；
- `response_url` 的有效期、一次性和单次内容上限必须进入 capability/错误分类，
  不能被当成普通可永久重放的 URL。超过 Provider 单次限制时，按本 Spec 的
  分片、流式更新或 Artifact/下载链接降级规则处理。

官方协议参考：[企业微信 AI Bot 回调与回复协议](https://github.com/go-sphere/wecom-bot-api/blob/master/API.md)
和 [URL 回调模式说明](https://cloud.tencent.com/document/product/1759/121473)。
社区协议镜像只用于阅读和 fixture 对照；实施时以企业微信官方控制台配置和官方
协议为准，不把镜像当作授权来源。

#### 飞书

飞书单聊和群聊均通过普通事件订阅与消息 Open API 接入，不需要套用企业微信
AI Bot 产品形态；本 Spec 只要求其遵守相同的平台级 Input、Session、Inbox 和
Reply contract。

运行时固定使用官方 [oapi-sdk-go](https://github.com/larksuite/oapi-sdk-go)
的 `v3` 模块（本 Spec 修订参考 v3.11.0）：HTTP 事件由官方
event dispatcher/typed Open API 处理，Reply Sender 通过同一官方客户端访问消息、
媒体和卡片 API。版本升级必须经过依赖审查，不把文档中的参考版本当作动态下载
规则。
官方 [channel-sdk-go](https://github.com/larksuite/channel-sdk-go) 是偏向
WebSocket 长连接的高层 Channel SDK，不作为本需求的 HTTP webhook 入站 transport；
当前 HTTP 入站使用事件 dispatcher 和 typed service API；高层 Channel SDK 的
标准化和 capability 思路可以参考，但不能替代本服务的 durable Inbox。

飞书 Adapter 只提取消息、thread/topic 和 provider media reference；IM-07 负责
调用官方媒体 API 下载并写 Artifact。两个 Provider 的 SDK 都不能替代本服务的
Binding 校验、Inbox 幂等、事务 ACK 或 Reply Outbox。

两个 Provider SDK/协议包都必须是 Binding-scoped 的：凭据、客户端、限流器、
target 和 capability 不得使用跨租户全局单例；SDK 自带的内存去重、重试或缓存
都不能替代 PostgreSQL Inbox 和 IM-06 Reply Outbox。

## 4. Standard Channel Input

### 4.1 平台级模型

当前仓库没有这个标准模型，以下是新增的领域契约，不是现有代码能力：

~~~text
ChannelInput {
    tenant_id              // only from verified Binding
    app_id                 // only from verified Binding
    channel
    binding_id             // internal scoped binding ID
    binding_revision       // trusted Binding authorization snapshot
    external_message_id   // normalized provider message ID
    sender {
        user_id             // resolved internal ID; empty before admission mapping
        external_key_hash   // scoped HMAC lookup key, not raw provider ID
    }
    conversation {
        kind                // direct, group, topic
        conversation_id     // resolved internal ID; empty before mapping/direct
        session_principal_id // resolved internal principal; empty before mapping
        external_chat_key_hash
    }
    topic {
        thread_key_hash
        topic_principal_id
    }
    message_type            // text, image, file, mixed, card, event, unsupported
    text
    artifact_refs           // tenant-scoped ArtifactRefs; may be non-empty
    provider_timestamp
    received_at
}
~~~

Adapter 先产出一次性 `VerifiedProviderEnvelope`，再由媒体处理和入站持久化边界
转换为进入 Gateway/Admission 的 `ChannelInput`。Envelope 中可以有 Adapter 自己
的 `ProviderMediaRef` 和消息级 `ProviderReplyTarget` 不透明句柄；它们只允许被
`AttachmentIngestor`、target persistence 或对应 Provider outbound codec 消费，不能
进入 Gateway、Worker、Runner 或 Session。`ChannelInput.artifact_refs` 只保存已经
完成 Artifact ingest 的平台引用。`user_id`、`conversation_id` 和
`session_principal_id` 在 IM-05 的同一事务中补齐；它们不是外部 payload 字段，也
不是 Adapter 可任意指定的内部主键。

`ChannelInput` 有两个明确生命周期：Adapter/AttachmentIngestor 产出的
`unresolved` 形态只包含 binding-scoped external key hash，内部 ID 为空；Gateway
把它作为平台级 admission command 传入 Store.Admit。Store.Admit 在同一事务内完成
Identity/Conversation lookup-or-create、Session Lane 分配和最终 RuntimeContext
构造，只有 `materialized` 形态才写入 Execution 并交给 Worker。这样既保留现有
Gateway 入口，也不要求在事务外预建身份。

预入站边界固定为：

~~~text
VerifiedProviderEnvelope {
    input              // provider-neutral fields; no provider DTO
    provider_media_refs // opaque, Adapter-owned handles; never enter Gateway
    provider_reply_target // opaque message-scoped target; may be absent
}

AttachmentIngestor.Prepare(
    ctx, trusted_scope, input, provider_media_refs
) -> ChannelInput{artifact_refs} | IngestError
~~~

`provider_reply_target` 按 6.2.1 的 message target 方案在 Inbox 事务边界内保存；
它不是稳定的用户/群 target，也不作为 `ChannelInput` 字段传递。

字段语义和边界如下：

| 字段 | 来源和语义 | 允许进入的边界 |
| --- | --- | --- |
| tenant_id / app_id | Binding 数据库记录；不是外部消息字段 | Channel Domain、Gateway identity、Execution；不由 payload 覆盖 |
| channel | URL 与 Binding 的匹配结果 | Channel Domain、RuntimeContext；不带 provider-specific 类型 |
| binding_id | 内部 Binding ID | Gateway identity、RuntimeContext、Execution、Reply；不作为外部 target |
| binding_revision | IM-01 从 Binding 读取的单调授权版本；不是外部 payload 字段 | 仅用于 Gateway/Admission transaction revalidation；不进入 Runner、Session 或用户消息 |
| external_message_id | Provider 的稳定消息 ID，经 Adapter 规范化 | Channel Domain；作为 Gateway idempotency key；不作为 Session 主键 |
| sender.user_id | Identity 映射产生的内部 ID | RuntimeContext.UserID、Execution、审计 |
| sender.external_key_hash | Binding 作用域内的 HMAC 查找键 | Identity Repository；不进入 Runner message |
| conversation_id | Conversation 映射产生的内部 ID | RuntimeContext/Session principal；不带 XML/JSON 字段 |
| session_principal_id | direct 为 user_id，group/topic 为对应内部 Conversation/Topic principal | RuntimeContext、Session Lane、Runner session user argument |
| session_id | 本需求默认值为 default | RuntimeContext、Execution、Runner |
| topic/thread | 只保留规范化后的 thread key 和内部 principal | Channel Domain、Session mapping；没有 provider thread 时为空 |
| message_type | 平台无关枚举 | Adapter、Channel Domain；Gateway 接收已转换的文本和 ArtifactRefs |
| text | 规范化 UTF-8 文本 | gateway.Message.Text、model.NewUserMessage |
| artifact_refs | Artifact 服务生成的租户作用域引用，不是 URL 或二进制 | gateway.Message、Execution 输入和 Worker 到 Runner 的受控转换 |
| provider_timestamp | Provider 事件时间，用于顺序/重放窗口判断 | Adapter、Inbox 可选字段；不决定 tenant |
| received_at | 服务接收时间 | Adapter/Inbox；不进入 Runner prompt |
| provider metadata | 原始 envelope、XML/JSON、签名字段、事件版本 | 只在对应 Adapter 内；不得以 map 形式穿过 Gateway |

### 4.2 到现有 Gateway 的映射

文本和附件路径统一复用现有 gateway.Request，不新增第二套执行入口：

~~~text
RequestID      = 服务端生成的内部 UUID/ULID
IdempotencyKey = binding-scoped canonical(ChannelInput.external_message_id)
Tenant         = ChannelBindingIdentityResolver
Message        = gateway.Message{Text: ChannelInput.text, ArtifactRefs: ChannelInput.artifact_refs}
~~~

`IdempotencyKey` 不能作为全局裸 external ID 使用；其有效唯一范围始终是
`tenant_id + app_id + binding_id + external_message_id`，具体字符串/摘要编码按
现有 Execution API 适配。channel_inbox 仍以这四列建立数据库唯一约束。

Gateway 的 admission command 至少包含可信 Binding identity、materialized
`gateway.Message` 和 unresolved ChannelInput 的平台元数据；它不包含 Provider
XML/JSON 或 raw target。Resolver/Store 在事务内映射后返回最终 RuntimeContext，
至少包含：

- tenant_id、app_id；
- active config version 的可信快照；
- channel、binding_id；
- user_id；
- session_principal_id；
- session_id = default；
- trace_id。

当前 RuntimeContext.Validate 要求 ConfigVersion 非空，而 Adapter 本身不能
猜测版本。实现时由已根据 Binding 定位出的 Tenant/App resolver 读取
active_config_version；PostgreSQL Admission 仍然以数据库当前 active version
为最终值。不能把外部 payload 的版本字段传入。

Gateway 不需要接收 WeCom 或 Feishu 的协议 DTO。provider_timestamp、标准
message_type、artifact digest 等需要持久化的内容，只能通过平台级
AdmissionMetadata 或已定义的 ArtifactRef 进入；不得增加 raw provider payload
或 provider-specific DTO。

### 4.3 ArtifactRefs 方案比较

| 方案 | 做法 | 影响 | 结论 |
| --- | --- | --- | --- |
| A. 扩展 gateway.Message | 放开 ArtifactRefs.Validate，并让 Worker 把已授权 ref 转成 tRPC-Agent-Go 可消费的 model.Message | 需要同步补齐输入权限、大小/MIME、安全扫描、对象生命周期和失败语义；只放开 Validate 不算完成 | 必须实施 |
| B. IM 层先持久化 Artifact | IM-07 调用 Adapter 提供的 provider media client，下载/解密图片、文件或混排媒体，写入现有 Artifact/Object 能力，得到租户作用域 ArtifactRef，再由平台消息携带引用 | 二进制不进 Session/Event；可重试、可审计；写入使用外部消息和媒体序号幂等 | 必须实施 |
| C. 不支持附件 | 只把附件标成 unsupported 并写 REJECTED，不创建 Execution | 与本需求的图片/文件输入要求冲突 | 不采用 |

最终采用 A+B。IM-05 先冻结 `AttachmentIngestor.Prepare` 的窄接口，并在
`Store.Admit` 前调用它；IM-07 负责该接口的实际媒体下载/解密、Artifact ingest，
以及出站附件降级。Admission command 接受文本、ArtifactRefs 或二者组合。只有
Provider 明确不支持的出站类型才允许按 capability 降级为文本或下载链接，不能
用“首版只支持文本”作为平台默认边界。

媒体处理失败分两类：可重试的下载、Provider 临时错误或对象存储错误不写
Inbox/Execution，不返回成功 ACK，等待 Provider 重投；已经验证且确定不可支持的
媒体写入 `channel_inbox(REJECTED, ATTACHMENT_REJECTED)` 并在事务提交后 ACK，不能
静默丢弃。数据库/Admission 失败仍按 7.2 的 no-row 语义处理。

## 5. Binding Routing and Admission

### 5.1 公网路由

正式入口：

    /im/{channel}/{route_key}

其中 route_key 对应 Binding 的 public_route_id。它必须：

- 使用 CSPRNG 生成，具备不可预测性；
- 至少有足够的随机空间，推荐不少于 128 bit；
- 全局唯一；
- 不编码 tenant_id、app_id、binding_id 或外部用户信息；
- 不从外部 payload 读取；
- 不写入普通日志，日志只记录脱敏后的 route fingerprint；
- 作为外部 URL bearer-like locator，泄露后依靠签名/解密和 Binding 状态阻止
  未授权请求。

请求初始处理：

~~~text
/im/{channel}/{route_key}
  -> IM-01 按 route_key 查候选 Binding
  -> IM-01 验证 Binding.Channel == URL channel
  -> IM-01 验证 Binding.Status == ACTIVE
  -> IM-01 导出 tenant_id + app_id + binding_id + public_route_id + binding_revision
     trusted scope snapshot
  -> IM-02/IM-03 使用该 Binding 做 Provider verification/decryption/extraction
  -> IM-02/IM-03 标准化 VerifiedProviderEnvelope
  -> IM-07 AttachmentIngestor 预入站物化为 ChannelInput
  -> Gateway
~~~

route_key 查询是路由定位，不是最终授权。最终授权由以下组合完成：

1. route_key 定位出的 Binding；
2. URL channel 与 Binding.Channel 一致；
3. IM-02/IM-03 的 Provider 签名/解密成功；
4. IM-02/IM-03 从已验证内容提取的 external_account 与 Binding.ExternalAccount
   一致；
5. Gateway 使用 verified_channel_binding；
6. PostgreSQL 事务重新读取并锁定正确 scope 的 Binding、Tenant、App。

IM-01 本身不读取或解释 Provider signature、XML/JSON encryption envelope、
Verification Token 或 external_account。IM-01 的单元测试和 PostgreSQL 测试
接收 fake trusted-scope channel binding input；真实 Provider 协议测试分别归入
IM-02 和 IM-03。

### 5.2 方案比较

| 方案 | 数据迁移 | API 兼容性 | Tenant 隔离 | 唯一性和安全性 | 扩展成本 |
| --- | --- | --- | --- | --- | --- |
| public_route_id | channel_binding 增加一列并为存量行回填；binding_id 不变 | 保留现有 ResolveBinding(ctx, tenant, app, binding)；新增独立 route resolver，不破坏现有调用方 | route 本身不泄露 tenant/app；最终仍按 Binding scope 校验 | 可做全局 UNIQUE；可轮换；不会把可猜的业务 ID 暴露为入口 | WeCom、Feishu 和后续通道共用 |
| binding_id 全局唯一 | 需要检查存量冲突、改主键/约束和创建接口 | 会改变现有 tenant/app scoped 语义；外部 URL 与内部资源 ID 耦合 | ID 唯一不能代替签名和 ACTIVE 校验；迁移期间容易出现歧义 | 业务 ID 往往可猜；轮换需要改变资源主键 | 后续外部路由和内部引用耦合 |

最终选择 public_route_id。现有 binding_id 继续代表 tenant/app 作用域内的
资源；不新增“把 binding_id 变全局唯一”的要求。

### 5.3 Schema 和存量兼容

目标字段：

    platform.channel_binding.public_route_id TEXT NOT NULL
    platform.channel_binding.binding_revision BIGINT NOT NULL

约束：

    UNIQUE (public_route_id)
    CHECK (public_route_id <> '')

`binding_revision` 从 1 开始单调递增。任何会影响入站授权或出站目标的 Binding
变更都必须递增它，至少包括 Channel、ExternalAccount、TokenRef、
SigningSecretRef、Secret、Status 和 public_route_id；Tenant/App/Binding 主体
标识不可变。

迁移分两步，避免在回填期间阻塞存量数据：

1. 先增加可空列，给所有存量 Binding 生成随机 route key，并将
   binding_revision 回填为 1，检查空值和冲突；
2. 创建全局唯一索引，确认回填完成后将 public_route_id 和 binding_revision 改为
   NOT NULL。

旧 Binding 的兼容规则：

- binding_id、tenant/app 主键和现有 ResolveBinding 不变；
- 当前仓库没有已经实现的公网 IM Adapter，因此没有已存在的旧 HTTP 路由
  行为需要隐式保留；
- 新 Adapter 只接受 public_route_id；
- 若外部环境已经使用设计草稿中的 /im/{channel}/{binding_id}，必须在上线
  前为该 Binding 回填 route key 并更新 Provider callback URL；
- 不做“按 binding_id 在全局扫描并猜租户”的兼容分支；若确实要保留旧 URL，
  只能在迁移工具中验证该 binding_id 在 channel 作用域内唯一，并把它视为
  有明确截止时间的临时兼容，不得成为默认路由。

route key 轮换：

- v1 在 Store/内部控制面支持显式生成新 route key 并替换当前值；IM-01
  只交付该操作所需的 scoped storage primitive 和 stale-route 语义，不新增
  IM-01 专属的管理 HTTP endpoint；
- 替换 route key 时必须递增 binding_revision；
- 替换后旧 route 立即失效，不支持默认双活；
- 轮换不改变 binding_id、tenant/app 或 Identity/Conversation；
- 不立即复用旧 route key；
- 若部署需要无缝切换，另增 route alias 表并带 expire_at；当前 Spec 不启用
  双活旧 route。

Binding 生命周期：

- “disabled” 在当前代码中对应 Status=SUSPENDED；
- SUSPENDED Binding 可以被 route query 找到，但在验证/Admission 前拒绝；
- 删除 Binding 后 route query 不命中，不能返回成功 ACK；
- 停用或删除不会删除既有 Session、Execution、Artifact、Audit 或 Reply
  记录；
- Reply Sender 在发送前重新检查 Binding 状态；停用时保留待发送记录，按
  策略暂停或标记不可发送，重新启用后才可继续，不静默丢弃。

### 5.4 Admission 分层检查

| 阶段 | 必须做的检查 | 不允许做的事 |
| --- | --- | --- |
| IM-01 route/admission boundary | route key 格式/大小、Binding 存在、URL channel 与 Binding.Channel、Binding ACTIVE、trusted tenant/app/binding scope、binding_revision/public_route snapshot、fake trusted-scope input contract | 注册或处理 Provider HTTP method/path、读取或验证 WeCom/Feishu signature、XML/JSON decrypt、Verification Token、external_account；从 payload 取 tenant/app；调用 Runner；先返回成功 ACK；将 fake trusted-scope input 暴露为生产入口 |
| IM-02/IM-03 Provider Adapter | body 上限、Content-Type、secret_ref 解析、Provider 验签/解密、external_account、external_message_id、Provider 时间窗口、协议 DTO 到 `VerifiedProviderEnvelope` | 修改 tenant/app trusted scope；把 Provider DTO 传给 Gateway；调用 Runner；建立 Reply Outbox retry loop |
| Gateway | 构造 verified channel binding identity；校验 Source、SourceID、BindingID、Channel、消息/Artifact 边界和 idempotency key；把 unresolved platform metadata 交给 Store.Admit，生成/传递 request_id | 从 payload 覆盖 tenant/app；在身份映射前强行构造内部主键；绕过 Admitter 直接入队；依赖 Provider-specific fields |
| PostgreSQL transaction | 锁 Tenant 并验证 ACTIVE；锁 App 并验证 ACTIVE；锁 Binding 并验证 tenant/app/binding/channel/public route snapshot、binding_revision、ACTIVE 及授权属性版本；读取并验证 active config；检查 Inbox/Execution 幂等；分配 Session turn；写 Inbox、Execution、Dispatch Outbox | 只相信 Adapter 的 Binding 快照；先 ACK 后持久化；只按 request_id 不带 scope 查询 |

`fake trusted-scope channel binding input` 只能通过测试依赖注入或 `_test.go` 内部
构造器进入 IM-01/IM-05；不得提供生产 HTTP 路由、公共 API、可配置的生产 DI
分支或可由客户端反序列化的 trusted identity。生产部署中，唯一能构造
`ChannelBindingIdentityResolver` 的路径是 IM-01 的 Binding lookup 加 IM-02/IM-03 的
Provider verification/extraction；测试 fake 不得进入可部署 wiring。

`BindingSnapshot` 至少包含 tenant_id、app_id、binding_id、channel、
public_route_id、binding_revision、ExternalAccount、TokenRef、SigningSecretRef
和其他用于 Provider verification 的 SecretRef；只携带 SecretRef，不携带 secret
明文。Adapter 使用该快照完成 Provider 验证，Admission transaction 以
binding_revision 和授权属性的原子更新规则判断快照是否过期。

现有 Gateway.AdmissionIdentity 的 SourceID 建议使用稳定的内部 binding_id，
而不是 route_key。这样 route 轮换不会改变同一 Binding 的 Execution 幂等范围。
为了让轮换或授权配置变更在进行中的请求上也可被检测，ChannelBindingIdentityResolver
的内部快照必须携带 public_route_id 和 binding_revision；AdmissionIdentity 继续
保留这些字段用于 transaction revalidation，但 verified channel 分支还必须带有
gateway 包内不可由外部 struct literal 设置的 provenance marker。直接构造同样
字段的 verified AdmissionIdentity 必须在 Validate 阶段拒绝；PostgreSQL 只把
route/revision 作为 transaction revalidation 条件，不能把它们单独当成授权凭据。

IM-01 不负责注册可部署的 Provider HTTP handler。`/im/{channel}/{route_key}` 是
跨 Adapter 的 URL contract；IM-02/IM-03 各自注册对应 Provider 的 HTTP method、
Content-Type、body limit 和 callback handler，并在进入本任务的 route resolver
后执行协议验证。这样 IM-01 的 fake trusted-scope 测试不会形成可配置的生产旁路。

IM-01 的 revision 规则覆盖 ExternalAccount、验证 SecretRef 和 Status：若
Provider 验证使用的 Binding 快照 revision 与事务锁定行不一致，即使当前行又是
ACTIVE，也必须回滚，不得提交旧快照验证出的消息。

### 5.5 Binding/Tenant/App TOCTOU

Provider 验证/解密和标准化完成、请求进入 Gateway 后到事务提交前，如果 Binding
被改为 SUSPENDED、ExternalAccount/SecretRef 被修改，或 public route 被轮换：

1. Admission transaction 锁定 Binding；
2. 读取到 SUSPENDED，或发现 binding_revision/public_route/授权属性不一致；
3. 回滚本次 Inbox/Execution/Dispatch 写入；
4. 返回非成功结果，不返回外部成功 ACK；
5. Provider 可以重试；只有重新启用并再次通过全部校验才会入站。

如果 Binding 在事务提交后才被停用：

- 已经 ADMITTED 的 Execution 不回滚、不删除；
- 新 webhook 被拒绝；
- 尚未发送的 Reply 不应绕过 Binding 状态；可以暂停等待重新启用；
- 运行中的 Runner 不因普通 Binding 停用而假设外部副作用已撤销，取消需要
  走 IM-07 的显式撤回/取消语义。

## 6. Identity / Conversation / Session

### 6.1 映射规则

不新建第二套 User/Conversation Domain，优先扩展
channels.Identity、channels.Conversation 和已有 Membership。

| 外部场景 | Identity | Conversation | RuntimeContext |
| --- | --- | --- | --- |
| Direct Message | external sender -> user_id | 可不创建 Conversation | UserID=user_id；SessionPrincipalID=user_id；SessionID=default |
| Group Message | external sender -> user_id | external chat -> conversation_id | UserID=user_id；SessionPrincipalID=conversation_id；SessionID=default |
| Topic/Thread | speaker -> user_id | external chat + thread -> topic/conversation principal | UserID=user_id；SessionPrincipalID=topic/thread 对应 principal；SessionID=default |

群聊和话题的 Session 保留群/话题上下文；user_id 始终是真实发言人，用于
权限、审计和个人记忆。Runner 使用 SessionPrincipalID 作为 session user 参数，
沿用当前 Worker 的群聊共享 Session 行为。

### 6.2 唯一键和 ID 存储

Identity 的逻辑唯一键：

    tenant_id + app_id + binding_id + normalized_external_sender

Conversation 的逻辑唯一键：

    tenant_id + app_id + binding_id + normalized_external_chat + normalized_thread

其中 direct 场景可以只依赖 Identity；Conversation 记录只用于 group/topic。
group 没有 thread 时，`normalized_thread` 不得为 NULL，必须使用独立命名空间的
固定 typed sentinel `thread:none` 再计算 HMAC；topic 使用 chat + thread 的
组合，避免不同群的相同 thread ID 冲突。

因此 `external_chat_key_hash` 和 `thread_key_hash` 在 Conversation 表中均为
NOT NULL；group 的无 thread sentinel 与真实 Provider thread 使用不同的 typed
输入，不能因空值或字符串碰撞产生重复/混淆。

规范化规则：

- Provider ID 必须是非空、合法 UTF-8、去掉协议允许的首尾空白；
- 默认保持大小写，不擅自 lower-case；是否大小写不敏感由 Provider Adapter
  的正式协议定义；
- 规范化时区分 ID 类型，不能把 user ID 和 chat ID 使用同一个命名空间；
- 使用规范化后的字节计算 HMAC-SHA-256，写入 ExternalUserKeyHash、
  ExternalChatKeyHash、ThreadKeyHash，并记录 KeyVersion；
- HMAC key 由平台密钥服务管理，不进入仓库、日志或普通配置；
- user_id、conversation_id 使用平台生成的不可猜测内部 ID，不直接把外部 ID
  当作主键。

用于内部映射的 ID 和用于回复的 Provider target 必须分开：

| 用途 | 存储 |
| --- | --- |
| 查找同一外部用户/群/话题 | tenant/app/binding 作用域 + HMAC hash |
| Session、Execution、权限、审计 | 内部 user_id/conversation_id |
| 向 Provider 发送消息 | 稳定用户/群/话题使用 Identity/Conversation 的 DB 密文 target envelope；消息级临时目标使用 channel_inbox 的密文 target envelope；Reply Outbox 只保存指向内部主体的 target_ref |

当前 Identity/Conversation 结构只有 hash 字段，没有 provider target 字段。仓库
核对结果是：trpcservice/secret 只提供带 tenant/app scope 的
SecretProvider.ResolveSecret；cmd/trpc-service 当前实现从带作用域的环境变量
读取 Secret；代码中没有可复用的可逆加密、KMS/Vault client 或 PostgreSQL
密文模式。api_credential.key_digest 是单向 digest，不能用于回复 target。
docs/deliverables 中的 display_name_enc 只是设计字段，当前 migration 没有对应
实现。

因此冻结 v1 方案：数据库保存 provider target 的密文 envelope，Reply Outbox
只保存内部 `target_ref`；它不保存明文 provider target，也不把 target 放进日志、
Session event 或普通 Execution command。Reply Outbox 允许保存发送成功后返回的
`provider_message_id` 作为同一条消息 UPDATE 所需的 scoped delivery receipt，但它
不是 target，不能被当作任意发送目标或写入日志。稳定目标从 Identity/Conversation 读取；
像 WeCom `response_url` 这类消息级临时目标从 channel_inbox 读取。Adapter 发送
时通过 scoped mapping 解密并确认 target 属于同一 tenant/app/binding，且未超过
有效期。

### 6.2.1 IM-04 precondition：Provider target 持久化方案

这项核对已在进入 IM-04 编码前完成；它是 IM-04 的前置条件，不是 IM-01 的
blocker。

**复用的现有能力**

- trpcservice/secret.SecretProvider；
- tenant.Scope 和 tenant.SecretRef；
- 当前环境 SecretProvider 的 tenant/app/ref 作用域解析；
- log.RoutingFields 的 allowlist 思路。

**不复用的能力**

- 没有现成 reversible encryption；
- 没有 KMS/Vault API；
- 没有可逆 provider target 的 Object/Artifact controlled reference；
- 不能把 API credential digest 反解为 target。

**v1 窄接口**

在 channels 领域定义最小 TargetProtector contract，职责只有 target envelope
的 Seal/Open，不负责 Secret 管理：

~~~text
TargetProtector.Seal(ctx, scope, purpose, plaintext) -> TargetEnvelope
TargetProtector.Open(ctx, scope, purpose, envelope) -> plaintext
TargetEnvelope {
    algorithm
    key_version
    nonce
    ciphertext
}
~~~

`purpose` 不是自由字符串，v1 只允许：

    im.identity.user_target
    im.conversation.chat_target
    im.conversation.topic_target
    im.reply.message_target

target 明文在加密前使用固定字段顺序、无空白的 UTF-8 JSON 编码：

~~~json
{"version":1,"channel":"...","target_kind":"...","external_user_id":"...","external_chat_id":"...","external_thread_id":"...","provider_target":"..."}
~~~

七个字段都必须存在；不适用的 ID 或 provider_target 使用空字符串，不能使用
null 或任意 map。消息级 target 的 `provider_target` 可以是 WeCom
`response_url` 或其他 Provider 的消息级句柄，但只存在于加密明文的受控生命周期。
`target_kind` 只允许 `user`、`conversation`、`topic`、`message`，Provider 名称通过
`channel` 表达。`message` 只用于 channel_inbox 的消息级回复目标。AAD 使用同样
固定编码规则的字段序列：

    tenant_id, app_id, binding_id, channel, entity_type,
    internal_entity_id, purpose, key_version

所有字段按上述顺序编码为 UTF-8 长度前缀字符串；不得直接拼接未转义字符串。

`entity_type` 固定为 `channel_identity`、`channel_conversation` 或
`channel_inbox`；`internal_entity_id` 分别为 user_id、conversation_id 或
request_id。IM-04 必须提供固定 key、固定 nonce 和固定 target 的 round-trip
test vector，验证 canonical plaintext、AAD、Seal/Open 和错误 scope 拒绝；消息
级 target 的 vector 由 IM-05/IM-02 或 IM-03 补充。生产环境仍只能使用随机 nonce，
测试固定 nonce 只能通过测试专用注入点提供。

实现由现有 SecretProvider 提供密钥材料，再使用标准库 AEAD；v1 固定使用
AES-256-GCM
完成 envelope 加解密；不新增 KMS、Vault、通用 Secret Manager 或可写 Secret
服务。TargetProtector 不记录明文和密钥。

**密钥归属和版本**

- key ownership：平台部署/运维拥有保护密钥；业务 tenant 不能从 payload 或
  Provider 自行指定密钥；
- key lookup：按 tenant.Scope 和 key_version 通过现有 SecretProvider 取得；
  v1 固定使用 `SecretRef{Name="im-provider-target-key", Version=key_version}`
  映射，不把 key material 写入 PostgreSQL；
- key format：SecretProvider 返回值必须是无 padding 的 base64url，解码后严格
  为 32 bytes；不做隐式 KDF、截断或补零；
- key_version：每条 envelope 必须记录；新写入使用当前 active version；
- active version：由部署/运维配置注入 TargetProtector，Seal 不接受调用方
  指定版本；Open 只使用 envelope 中的版本，并通过 SecretProvider 读取对应
  旧 key；
- nonce：每次 Seal 使用 crypto/rand.Reader 生成 AES-256-GCM nonce（v1 为 12
  bytes），同一 key version 不得复用；随机源失败则 Seal 失败；
- Seal/Open 对未知 purpose、algorithm、key_version、target_kind、字段缺失、
  base64url 非法、密钥长度错误或 AAD 不匹配一律 fail closed；Open 不允许
  fallback 到其他租户、Binding 或 key version；
- rotation boundary：切换 active version 只影响新写入；旧 version 在所有
  envelope 完成重加密前必须保持可读；重加密按 tenant/app/binding scope
  运行，成功后才可撤销旧 version；
- purpose/AAD：把 tenant_id、app_id、binding_id、channel、实体类型和内部
  主体 ID 作为 AEAD associated data，防止密文跨租户、跨 Binding 或跨实体
  搬运。

**密文 schema**

v1 固定使用 Identity/Conversation 上的 `provider_target_envelope JSONB`，以及
channel_inbox 上的 `provider_reply_target_envelope JSONB`（消息级临时目标）字段，
不保存明文 Provider target。两者字段和编码固定为：

~~~json
{
  "algorithm": "AES-256-GCM",
  "key_version": "<provider target key version>",
  "nonce_b64": "<base64url nonce>",
  "ciphertext_b64": "<base64url ciphertext and authentication tag>"
}
~~~

`purpose` 和 tenant/app/binding/channel/entity/internal ID 作为 AAD，不重复
写入 envelope；明文 target 只在 `TargetProtector.Open` 和 Provider outbound
codec 的受控调用期间存在。

`reply_outbox.target_ref JSONB` 只保存内部引用：

~~~json
{"target_kind":"identity|conversation|inbound_message","internal_target_id":"..."}
~~~

tenant_id、app_id、binding_id 使用 reply_outbox 的独立作用域列保存，并在
所有读取、解密和发送前校验。`inbound_message` 的 `internal_target_id` 是
request_id；Sender 必须在同一 scope 下回读 channel_inbox 的
`provider_reply_target_envelope`，校验 `reply_target_expires_at` 后再解密。

Sender 通过内部 target_ref 回读相应的密文 envelope。这样 Reply Outbox 不重复
保存 provider target，也不需要一个新的外部 Vault/Object reference；消息级目标失效
时只能按 Provider 能力分类为可重试或永久失败，不能回退到另一个用户/群目标。

### 6.3 并发首次映射

Identity、Conversation 和 Membership 的首次创建由数据库唯一约束决定：

1. 根据 HMAC key 查找；
2. 尝试 INSERT；
3. 发生唯一冲突时回读已存在行；
4. 返回同一个内部 user_id/conversation_id；
5. 不因并发 webhook 创建两个内部主体。

不要用进程内 map 或单节点锁作为正确性条件。缓存可以优化读取，但必须带
tenant/app/binding 作用域，并以 PostgreSQL 为权威来源。

## 7. Inbox and Transaction Model

### 7.1 channel_inbox 数据模型

channel_inbox 的最小字段：

| 字段 | 语义 |
| --- | --- |
| tenant_id | 从已验证 Binding 得到 |
| app_id | 从已验证 Binding 得到 |
| binding_id | 内部 Binding ID |
| external_message_id | Provider 稳定消息 ID，已规范化 |
| payload_hash | 规范化 Channel Input/Admission command 的 SHA-256，32 bytes |
| request_id | 首次 Admission 生成的内部 request ID；任何持久化行都必须有值 |
| status | `ADMITTED` 或已验签但不支持类型的 `REJECTED` |
| message_type | 已标准化的平台消息类型；所有 Inbox 行必填 |
| reject_reason | 可空；仅在 `status=REJECTED` 时必填 `UNSUPPORTED_MESSAGE_TYPE` 或 `ATTACHMENT_REJECTED` |
| provider_reply_target_envelope | 可空的 AES-256-GCM 消息级回复目标密文；只供 Reply Sender 使用，不进入 ChannelInput/Session |
| reply_target_expires_at | 消息级回复目标的 Provider 有效期；无临时目标时为空 |
| created_at | Inbox 持久化时间 |
| updated_at | 状态/诊断字段更新时间 |

可选但不保存 raw body 的字段：

- provider_timestamp：用于顺序、重放窗口和排障；
- provider metadata fingerprint：仅用于排障，不能还原原始 payload。

唯一键：

    UNIQUE (tenant_id, app_id, binding_id, external_message_id)

先对已完成 provider verification、身份字段规范化和消息类型判定的 Channel Input
计算 payload_hash，再执行 Inbox lookup。payload_hash 必须针对规范化且不含
received_at、签名字段、随机 request_id、
public_route_id 或 binding_revision 的语义内容计算。对于已规范化的消息语义，它至少
覆盖 tenant/app、channel、sender.external_key_hash、conversation/thread hash、
message type 和 text；必要时覆盖已规范化的 artifact reference。它不依赖尚未
生成的 user_id、conversation_id 或 session_principal_id，应与 Execution 幂等
比较使用同一 canonical 表示，避免 XML 空白差异造成错误冲突。
binding_revision/public_route 是授权快照，不是消息语义，不得因 route 轮换使同一
external_message_id 变成不同 payload hash。

### 7.2 状态是否全部需要落库

不机械采用 RECEIVED/ADMITTED/ACKED/REJECTED 全套状态：

- RECEIVED：不落库。原始 HTTP 请求尚未通过可信验证；本模型不做“先存后验”
  的两阶段接收。
- ADMITTED：落库。它表示 Inbox、Execution 和 Dispatch Outbox 已在同一
  事务中提交，服务可以返回成功 ACK。
- ACKED：不落库。HTTP ACK 是 PostgreSQL commit 之后的独立网络行为，服务不
  能可靠知道 Provider 是否收到或处理了响应；数据库写 ACKED 会制造虚假
  语义。ACK 尝试只进入指标/日志。
- REJECTED：只用于已经通过 IM-01 路由/Binding 校验、通过 IM-02/03 Provider
  验证、有稳定 message ID，但 message_type 或已验证媒体在当前 capability 中
  明确不支持。它必须与 reject_reason、request_id 在同一事务中写入，并写一条
  脱敏审计事件；不创建 Identity/Conversation、Session Lane、Execution 或
  Dispatch Outbox，commit 后返回 Provider 成功 ACK。签名错误、未知 route、停用
  Binding、格式错误或缺少稳定 ID 仍不写 Inbox，也不返回成功 ACK。

本需求的有效状态机只有：

    no row -> ADMITTED       (supported input)
           -> REJECTED       (verified unsupported type or permanent attachment rejection)

失败事务保持 no row，不能留下半条 RECEIVED 记录。已有 ADMITTED/REJECTED 行
不因 HTTP ACK 丢失变成另一个状态。

预入站媒体处理发生在 Inbox transaction 之前：可重试的下载、解密、Provider
临时错误或对象存储错误保持 no row，不返回成功 ACK，等待 Provider 重投；已经验证
且确定不可支持的媒体才写 `REJECTED/ATTACHMENT_REJECTED` 并在 commit 后 ACK。
这不新增第三个 Inbox 状态，也不把 transient failure 伪装成 ACKED。

### 7.3 幂等行为

首次消息：

    valid verify
    -> begin PostgreSQL transaction
    -> Inbox lookup
    -> supported: identity/conversation mapping
                -> Inbox/Execution/Dispatch transaction
       unsupported: Inbox(REJECTED) + audit, no Execution/Dispatch
    -> commit
    -> HTTP ACK

相同 key、相同 hash：

- 返回原 request_id；
- 对 ADMITTED 行不创建第二个 Execution 或 Dispatch Outbox；
- 对 REJECTED 行不重复创建拒绝审计；
- 可以返回 Replayed=true；
- 外部协议层返回成功 ACK。

相同 key、不同 hash：

- 返回 idempotency conflict；
- 不覆盖原 Inbox；
- 不创建第二个 Execution；
- 建议以不可重试的 4xx/协议等价错误响应，避免 Provider 无限重试。

并发 duplicate：

- 数据库唯一约束是正确性保障；
- Inbox 唯一键防止同一 Binding 的重复消息；
- 现有 Execution unique
  (tenant_id, app_id, tenant_source, source_id, idempotency_key) 作为第二道保护；
- 竞争失败的一方在看到唯一冲突后重新读取同 scope 的 Inbox/Execution，返回
  已提交行的 request_id。

当前 Store.Admit 对已有 FAILED Execution 存在“同一幂等请求 re-arm”的现有行为。
IM-05 的 webhook admission 路径不得用该行为处理 Provider 重复投递：只要 Inbox
已存在，就返回原 request_id/status，不重新激活 Execution、不增加 Runner 调用。
业务重试必须由独立、显式的 retry command/API 发起，保留审计，并复用原 request_id；
它不属于 webhook Inbox 幂等路径。

### 7.4 入站事务图

~~~text
verified ProviderEnvelope
  -> pre-admission AttachmentIngestor.Prepare
       -> recoverable error: no Inbox/Execution, no success ACK
       -> permanent unsupported media: Inbox(REJECTED, ATTACHMENT_REJECTED) + audit
       -> materialized ChannelInput
  -> begin PostgreSQL transaction
  -> lock and validate Tenant
  -> lock and validate App + active config
  -> lock and validate Binding + ACTIVE + binding_revision + authorization snapshot
  -> lookup channel_inbox unique key
       -> existing same hash: return original request_id
       -> existing different hash: conflict
  -> supported input:
       -> insert channel_inbox(status=ADMITTED, reply target envelope if present)
       -> lookup-or-create Identity / Conversation in this transaction
       -> allocate Session Lane turn
       -> insert Execution
       -> insert Dispatch Outbox
  -> verified unsupported type:
       -> insert channel_inbox(status=REJECTED, reject_reason=UNSUPPORTED_MESSAGE_TYPE)
       -> insert redacted audit event
  -> COMMIT
  -> HTTP ACK
~~~

IM-01 负责 trusted channel source、binding_revision/authorization snapshot 的
transaction revalidation；IM-05 负责在同一 `Store.Admit` transaction 中加入
Inbox 幂等分支、Identity/Conversation lookup-or-create、Execution 和 Dispatch
写入。两者不是两个 Admission 实现：IM-05 只扩展 IM-01 已建立的 verified
channel branch，不重新定义 trusted source。

对 supported text，Identity/Conversation 首次创建、Inbox、Execution、Dispatch
和 Session turn allocation 属于同一事务；事务回滚时新建的映射和密文 target
一并回滚。对重复消息，先命中 Inbox 后直接返回原结果，不重复创建映射或执行。
Binding 和 Tenant/App 的再校验也属于同一事务。

消息级 `ProviderReplyTarget` 只在该事务内通过 `TargetProtector.Seal` 写入
`channel_inbox.provider_reply_target_envelope`，AAD 的 entity type 为
`channel_inbox`、internal ID 为 request_id；Seal 失败则事务回滚且不成功 ACK。
重复消息命中既有 Inbox 后不得用重试请求中的 target 覆盖原 envelope。

数据库不可用、事务开始失败、任一校验失败或 commit 失败：

- 不返回成功 ACK；
- 不把请求放入内存队列；
- 事务回滚后的 Provider 重试可以重新尝试。

Commit 成功但 HTTP ACK 丢失：

- Provider 重试；
- 新请求重新命中 channel_inbox；
- 返回原 request_id；
- Execution count 仍为 1，Runner 不因这次重试而新增一次调用。

## 8. Reply Domain and Reply Outbox

### 8.1 Reply Domain

Reply Domain 是平台级对象，不包含企业微信 XML、飞书 event JSON 或 Provider
错误码。其最小类型契约在 IM-05 阶段冻结，供 IM-02/03 编译和实现 outbound
codec/client；IM-06 才实现 Reply Projection、Reply Outbox、Claim/Lease、
Sender lifecycle、错误分类和 retry。IM-05 不创建 Reply Outbox 表或发送循环：

~~~text
Reply {
    tenant_id
    app_id
    request_id
    source_event_id       // canonical execution_event key: request_id:event_seq
    channel
    binding_id
    reply_id
    logical_reply_id
    part_no
    revision
    operation              // SEND, UPDATE or FINALIZE
    reply_kind             // text, card, artifact, fallback_text
    target                 // platform target reference
    text
    card                   // platform-neutral card model or opaque approved card
    artifact_ref
}
~~~

Worker/Runner 只产生用户可见的最终文本、允许的卡片或 ArtifactRef。内部
模型事件、Tool 参数、原始错误、secret 和调试 payload 不直接进入 Reply。

### 8.2 Projection 位置

现有 Worker.EventSink 可以继续接收 Runner 事件，现有 execution_event 可作为
跨节点事件日志。新增 Reply Projection 的职责是：

1. 使用 event.Event.IsRunnerCompletion 判断 Runner 完成；
2. 只提取用户可见的助手输出；
3. 对支持流式更新的通道按 logical_reply_id 合并/节流；
4. 对不支持流式的通道只保留最终结果；
5. 分片并生成稳定的 part_no；
6. 使用 `(request_id, event_seq)` 生成 `source_event_id`；同一个源事件重放时
   必须复用已有 Reply Outbox 行和 reply_id，不得再次生成发送操作；
7. 在 `reply_projection_state` 行上按 `event_seq` 顺序处理；旧事件直接视为已处理，
   有缺口的后续事件等待前序事件，不能先投影较新的内容；
8. 对流式中间事件，在投影游标事务中创建对应 Reply Outbox；对最终事件，在
   Execution 终态确认和必要 Audit 写入的协调事务中创建最终 Reply Outbox。

支持流式的 Provider 可以在 Runner 完成前产生 `SEND`、`UPDATE` 和
`FINALIZE` 三类平台操作；如果 Provider 要求先建立一条可更新消息，IM-06
负责先创建/发送 `SEND`，再按 capability 生成更新和结束操作。Provider-specific
stream context 只能通过受控 target/context 引用传给一次出站调用，不能进入
Gateway、Session 或普通事件字段。

Worker 必须继续消费 Runner Event Channel 直到 channel 关闭，即使 context
已经取消。Reply Projection 不能通过提前停止消费来实现“快速返回”。

### 8.3 Reply Outbox persistence record

推荐字段：

| 字段 | 语义 |
| --- | --- |
| reply_id | 一次发送/更新操作的稳定内部 ID |
| logical_reply_id | 同一用户可见消息的稳定逻辑 ID |
| tenant_id / app_id | 租户和应用作用域 |
| binding_id | 出站 Binding |
| request_id | 来源 Execution |
| source_event_id | 来源 `execution_event` 的规范化键 `request_id:event_seq`；重放幂等依据 |
| part_no | 分片序号，从 1 开始 |
| revision | 同一 logical part 的内容版本，从 1 开始 |
| operation | SEND、UPDATE 或 FINALIZE |
| reply_kind | text/card/artifact/fallback_text |
| target_ref | 内部 JSON 引用（target_kind + internal_target_id）；不保存 Provider ID |
| payload | 平台级序列化内容，不放 Provider envelope |
| artifact_ref | 可选租户作用域 Artifact 引用 |
| status | PENDING、SENDING、SENT、PERMANENTLY_FAILED |
| attempt | 发送尝试次数 |
| next_attempt_at | 下次可发送时间 |
| lease_owner / lease_until | 有限 Claim/Lease |
| provider_message_id | 首次发送成功后保存的 scoped delivery receipt，用于 UPDATE；不是 target，不进日志 |
| last_error_type | 脱敏后的稳定错误类别 |
| last_error | 可选截断诊断信息，不放 secret/完整 body |
| created_at / updated_at | 生命周期时间 |

建议的数据库唯一约束：

    UNIQUE (
        tenant_id, app_id, binding_id,
        request_id, source_event_id, logical_reply_id,
        part_no, revision, operation
    )

reply_id 也应有全局唯一约束。Projection 必须先按上述唯一键查找；并发或重放
冲突时回读已有 reply_id，而不是重新生成一行。所有查询都必须带 tenant/app，
按 reply_id 更新时仍需带 scope 条件。

### 8.4 为什么 request_id + part_no 不够

request_id + part_no 不能区分：

- 同一 request 的状态消息和最终消息；
- 同一 part 的 SEND、UPDATE 与 FINALIZE；
- 同一 logical part 的多个流式 revision；
- 不同 execution_event 产生的多个用户可见变更；
- 运维明确要求的 resend 操作。

因此使用 `source_event_id` 绑定投影源事件，reply_id 代表一次稳定发送操作，
logical_reply_id + part_no 代表用户可见位置，revision 代表该位置的内容版本。
正常重试复用同一 reply_id；Projection 重放通过唯一键回读原行；有意 resend
必须生成新的 resend source/event 标识并留下审计记录。

Provider 如支持幂等键，Sender 使用 reply_id 作为 Provider 幂等键。Provider
不支持幂等时，进程在“Provider 已接受但本地未更新 SENT”窗口内可能重复发送；
本 Spec 不虚假承诺 exactly-once，只保证不重新执行 Runner，并尽量通过
provider_message_id/幂等键降低重复。

### 8.5 Reply Outbox 状态机

~~~text
PENDING
  -> SENDING      (claim + lease + attempt increment)
  -> SENT         (provider success and durable update)

SENDING --lease expired--> PENDING
SENDING --retryable error--> PENDING
SENDING --permanent error--> PERMANENTLY_FAILED
~~~

retryable error 回到 PENDING，并设置 next_attempt_at 和 last_error_type；
不额外引入 RETRYABLE_FAILED 状态。Lease 超时只允许重新投递发送操作，不会
重新生成 Execution。

Claim/Lease 可以复用现有 Dispatch Outbox 的 PostgreSQL 模式：

- SELECT ... FOR UPDATE SKIP LOCKED；
- 设置 lease_owner、lease_until、attempt；
- 发送 HTTP 在数据库事务外完成；
- 发送结果按 owner、lease 和 scope 条件回写；
- stale SENDING 恢复为 PENDING。

这是 IM Reply Outbox 的局部可靠性能力，不扩展成通用 Worker 故障接管框架。

### 8.6 回复规则

- 支持流式更新：先发送一个 logical message，后续在 provider_message_id 或
  消息级 target context 可用时生成 UPDATE，结束时生成 FINALIZE；按 Adapter
  capability 节流。每个操作都由一个 Reply Outbox 行驱动，发送端不维护第二套
  隐式队列。同一 `logical_reply_id + part_no` 内必须按 SEND -> UPDATE（revision
  递增）-> FINALIZE 顺序发送；前一操作未 SENT 时，后一操作不能 claim。
- 不支持流式更新：只投影最终事件。
- 超长文本：按 Provider 限制分片，part_no 严格顺序发送；只有 part N 达到
  `SENT` 后才能 claim/send part N+1。part N 处于 PENDING、SENDING 或可重试
  失败时，后续分片必须保持等待；part N 永久失败时，后续分片保持 PENDING，
  以 `last_error_type=BLOCKED_BY_PREVIOUS_PART` 表示阻塞，不得发送。
- 不支持的卡片/文件：降级为文本或租户作用域的下载链接；不能把内部错误
  或 Tool 参数作为用户回复。
- Reply 发送失败：只推进 Reply Outbox 状态和 attempt，不重新调用 Runner。
- Binding 停用：新 Reply 不发送；已存在的 PENDING 记录保留并按策略暂停，
  重新启用后可继续。

## 9. WeCom Adapter Spec

### 9.1 入站职责和出站接口边界

实现位置建议为 trpcservice/channels/wecom。公网入口先由 IM-01 完成
public_route 定位、Channel/ACTIVE 校验和 trusted scope；本 Adapter 只接收该
前置结果。AI Bot URL 回调 DTO、JSON 加解密 helper 和 Provider field extractor
保持在该包内；共享的 Channel Input、Reply Domain 和 route/admission contract
放在平台层。

IM-01 前置（不由 IM-02 实现）：

1. 读取 URL 中的 channel 和 route_key；
2. 通过独立 route resolver 定位 Binding；
3. 校验 Binding.Channel=wecom 且状态 ACTIVE，并导出 trusted scope。

IM-02 入站：

4. 通过 TokenRef/SigningSecretRef/SecretRef 取得临时 secret；
5. 处理 AI Bot URL verification：GET 验证 `msg_signature`、`timestamp`、`nonce`、
   `echostr`，解密后在 Provider 要求的时间内返回明文；
6. 对 POST 加密 JSON callback 验签、解密并校验 `aibotid` 与 Binding 的
   `ExternalAccount` 一致；企业内部 AI Bot 解密使用空 `receive_id`；
7. 提取 `msgid`、sender、chat、`chattype`、`response_url`、message type、
   text、媒体引用、event 和 provider timestamp；
8. 提取并校验 image/file 等非文本附件的 provider media reference；混排消息拆成
   平台级文本和 Adapter/IM-07 间的受控媒体引用，不在本任务写 Artifact；
9. 规范化为 `VerifiedProviderEnvelope`，其平台字段原样携带 IM-01 提供的
   public_route_id/binding_revision 快照；Adapter 不得从 payload 覆盖它们；
   `response_url` 只作为消息级不透明 target 交给受控持久化边界；
10. 通过共享 ingress orchestrator 完成 IM-07 预入站媒体处理和 ChannelInput
   构造，再调用 Gateway；不调用 Runner；
11. Gateway/Store commit 成功后快速返回协议规定的 ACK；
12. 为 IM-06 提供 WeCom outbound client/codec contract 和 fake；不在 IM-02
    建立 Reply Outbox、Claim/Lease、Retry loop 或完整 Reply Sender lifecycle。

协议 DTO 只能在 WeCom 包内出现。Gateway 不接受 `encrypt`、`msg_signature`、
`aibotid`、`response_url` 等 Provider 字段。IM-02 的出站接口只接受平台级 Reply
和 capability；Provider payload 编码、媒体上传/下载、HTTP client 和 fake 可以
在本任务定义，但发送调度、错误分类、重试和 lease 由 IM-06 统一实现。

### 9.2 支持范围和拒绝

- text、image、mixed、file 及其他可归入附件的非文本媒体：验证成功且有稳定
  `msgid` 时进入标准化流程；媒体由 IM-07 完成下载/解密/Artifact ingest 后才能
  进入 Runner；
- direct/group：按 Identity/Conversation 规则映射，AI Bot 的 `single/group`
  不能被降级为普通自建应用的 direct/group 假设；
- `event`：保留事件类型和受控 action metadata；进入 Session 前不得携带原始
  Provider JSON；模板卡片点击事件按 IM-07 的更新/审计边界处理；
- Provider 明确不支持或当前平台 Reply 无法表达的类型，验证成功且有稳定消息
  ID 时写入 `channel_inbox(REJECTED, UNSUPPORTED_MESSAGE_TYPE)` 和脱敏审计，
  不创建 Execution/Dispatch，commit 后返回协议成功 ACK；不能以此拒绝本需求
  已明确要求支持的图片、文件、卡片或流式能力；
- 缺少稳定 message ID：拒绝，不进入 Gateway；
- body 超过配置上限：在解析前拒绝，不写 Inbox；
- 验签/解密失败：拒绝，不写 Inbox，不返回成功 ACK。

企业微信的 URL verification、加密 JSON envelope、流式消息、模板卡片、媒体
上传/发送和 `response_url` 语义按 AI Bot 官方协议实现；`response_url` 若作为
回复目标，必须使用 `im.reply.message_target` 密文 envelope 和
`reply_target_expires_at`，不能写入普通 ChannelInput。目标账号的具体权限、大小
限制和错误码进入 capability/config，不得改变平台级契约。普通自建应用协议不作为
本任务的兼容实现。

### 9.3 WeCom 测试

- AI Bot URL verification 合法和非法；
- `msg_signature` 缺失、错误、时间窗口过期；
- 加密 JSON 解密失败、空 `receive_id` 规则和 `aibotid` 不匹配；
- `msgid` 缺失；
- direct/group 的 text、image、mixed、file 和非文本附件引用；
- provider media reference 提取、卡片/流式/事件边界；媒体下载/解密和 Artifact
  ingest 在 IM-07 验收；
- body 为空、非法 JSON、超过上限；
- route 不存在、Binding SUSPENDED；
- ACK 前 DB rollback；
- outbound codec/client 能把平台级 Reply 编码为 AI Bot 被动/主动请求；
- fake outbound client 能返回成功和可分类的错误样本，但不验证 Reply Outbox
  retry/lease；这些场景统一在 IM-06 验收。

## 10. Feishu Adapter Spec

### 10.1 入站职责和出站接口边界

实现位置建议为 trpcservice/channels/feishu。公网入口先由 IM-01 完成
public_route 定位、Channel/ACTIVE 校验和 trusted scope；本 Adapter 只接收该
前置结果。事件 envelope、challenge、Verification Token、签名/加密和
Provider field extractor 保持在该包内；共享的 Channel Input、Reply Domain
和 route/admission contract 放在平台层。

本 Adapter 固定使用官方 `github.com/larksuite/oapi-sdk-go/v3`。HTTP webhook
使用其事件 dispatcher/typed event 能力，出站使用其消息、媒体和卡片 Open API；
不把 `channel-sdk-go` 的 WebSocket transport 混入当前 HTTP callback 入口。

IM-01 前置（不由 IM-03 实现）：

1. 通过 route_key 定位 Binding，并确认 Channel=feishu、状态 ACTIVE，导出
   trusted scope。

IM-03 入站：

2. 取得 Binding 作用域的 Verification/Encrypt/Secret 配置；
3. 处理 url_verification，原样返回 challenge；
4. 对正常事件校验 Verification Token、签名或加密配置；
5. 提取 open_id、chat_id、message_id、thread/topic（若该事件版本提供）、
   message type、text、image/file key 和 provider timestamp；
6. 转成与 WeCom 相同的 `VerifiedProviderEnvelope`，并原样携带 IM-01 提供的
   public_route_id/binding_revision 快照；Adapter 不得从 payload 覆盖它们；
7. 提取并校验 image/file 的 provider media reference；对 interactive/card/action
   只保留平台级 action metadata，不在本任务写 Artifact；
8. 通过共享 ingress orchestrator 完成 IM-07 预入站媒体处理和 ChannelInput
   构造，再调用 Gateway；不调用 Runner；
9. commit 后按事件订阅协议返回 ACK；
10. 为 IM-06 提供 Feishu outbound client/codec contract 和 fake；不在 IM-03
  建立 Reply Outbox、Claim/Lease、Retry loop 或完整 Reply Sender lifecycle。

### 10.2 支持范围和拒绝

- url_verification：只做 webhook 配置验证，不创建 Execution；
- `im.message.receive_v1` 的 text、image、file、post、interactive/card 和
  thread/reply：验证成功且有稳定 message_id 时进入标准化流程；媒体必须完成
  IM-07 完成 Open API 下载和 Artifact ingest 后才能进入 Runner；
- direct/group/topic：按 Identity/Conversation 规则映射；缺少 thread/topic
  时按 conversation 维度处理；
- open_id/chat_id/message_id：只用于 Adapter 映射和 Reply target，不进入
  Gateway/Worker/Session 的 provider DTO；
- interactive/card action：保留受控动作信息并按平台级事件进入 Session；原始
  card JSON 不穿过 Gateway；
- Provider 明确不支持或当前平台 Reply 无法表达的类型，验证成功且有稳定消息
  ID 时写入 `channel_inbox(REJECTED, UNSUPPORTED_MESSAGE_TYPE)` 和脱敏审计，
  不创建 Execution/Dispatch，commit 后返回协议成功 ACK；不能因此拒绝本需求
  已明确要求的文本、图片、文件、卡片和异步回复；
- 发送 API 的 receive_id、message type、card JSON 和 token 获取由 Feishu
  outbound client/codec 提供给 IM-06；发送调度、Provider error classification、
  Reply Outbox retry 和 lease 不在 IM-03。

飞书事件固定为 `im.message.receive_v1`，事件解密/签名按官方事件订阅配置，
消息、媒体和卡片权限按目标 App 配置。账号级权限、大小限制和错误码进入
capability/config，不得改变平台级契约；实现只能使用官方 `oapi-sdk-go` 的
typed API 和事件 dispatcher，不以社区 SDK 作为安全边界。

### 10.3 Feishu 测试

- url_verification challenge 成功/失败；
- token、签名、加密配置错误；
- `im.message.receive_v1` direct/group text、image、file、post、interactive；
- open_id、chat_id、message_id 缺失；
- thread/topic 有/无；
- provider media reference 提取、卡片动作和回复更新边界；媒体下载和 Artifact
  ingest 在 IM-07 验收；
- body 上限、非法 JSON、重复和乱序事件；
- Open API request codec/client 的成功和错误样本；
- 不在 IM-03 验证 Reply Sender retry、lease 或 Runner invocation 不增加；
  这些属于 IM-06。

### 10.4 协议差异矩阵

下表把已经由选定 Provider 协议确定的内容写死；只有目标账号的权限、配额和
错误码等部署参数保留为配置项。`PROVIDER-SPEC-VERIFY` 不再用于选择产品或
SDK，也不能被编码 Agent 用来回退到普通自建应用或社区封装。

| Capability | WeCom | Feishu | Platform Handling |
| --- | --- | --- | --- |
| webhook verification | AI Bot GET `msg_signature`/`timestamp`/`nonce`/`echostr`，解密后返回明文 | `url_verification` 返回 `challenge` | Adapter 处理，不创建 Execution；成功响应不走 Gateway |
| signature | AI Bot 按 Token、时间戳、nonce 和密文计算官方签名 | 官方事件订阅 Token/签名校验，由 SDK dispatcher 执行 | secret_ref 取密钥，常量时间比较，失败不进 Inbox |
| encryption | 加密 JSON `encrypt`；官方 JSON 回调解密，内部 AI Bot `receive_id` 为空 | Encrypt Key 按事件订阅配置解密 | 解密后才提取业务字段；原始密文不进入平台层 |
| user identifier | `from.userid` | `event.sender.sender_id.open_id` 或事件配置指定的用户 ID | Adapter 映射为 binding-scoped Identity |
| group identifier | `chattype=group` 时使用 `chatid` | `event.message.chat_id` | 映射为 binding-scoped Conversation |
| message identifier | `msgid` | `event.message.message_id` | 作为 external_message_id 和 Inbox 幂等键 |
| thread/topic | AI Bot 没有独立 topic 主键；quote 保留为受控引用元数据 | reply/thread 字段存在时使用 `chat_id + thread_key` | 没有独立 thread 时为空；引用不冒充新会话 |
| text | `text.content` 或 Provider 明确提供的文本内容 | 消息 content JSON 中的 text/post 内容 | 解析成平台文本，去除协议 envelope |
| image | 消息 image URL，按 AI Bot AES 规则下载/解密 | image key 或消息资源接口，使用官方 Open API 下载 | IM-07 ingest 为 ArtifactRef，不把二进制放 Session |
| file | 文件 URL，按 AI Bot AES 规则下载/解密 | file key/消息资源接口，使用官方 Open API 下载 | IM-07 ingest 为 ArtifactRef，不把二进制放 Session |
| card | template card、stream with template card | interactive/card JSON | 平台 Reply 只用平台卡片模型；Adapter 编码 |
| message update | 流式消息按 stream id 更新；模板卡片事件可更新卡片 | 消息更新 API/卡片更新能力按 App 权限 | capability flag；无能力时只发最终结果 |
| async send | 回调携带 `response_url`；URL 有效期、一次性和内容限制由 capability 固定 | 消息 Open API + tenant/app token | Reply Outbox 异步发送，不把临时 URL 当永久凭据 |
| rate limit | AI Bot 回调/回复/媒体接口限制由配置记录 | Open API 限流和错误码由 SDK/配置记录 | Adapter 暴露 Provider 结果；IM-06 统一分类和退避 |
| retryable error | timeout/5xx/限流/临时 Provider code | timeout/5xx/限流/临时 Provider code | IM-06 统一分类、退避和 Reply Outbox 重试；永久错误终止 |
| recall event | 选定 AI Bot API 当前不提供通用消息撤回事件；能力标记为 false，不能伪造 recall | 使用官方撤回事件（如账号订阅并授权）时进入 IM-07 | 只有已验证且可关联的撤回事件执行取消/审计；无事件不宣称支持 |
| max callback size | Provider/账号限制，服务端先设置更小的保护上限 | Provider/账号限制，服务端先设置更小的保护上限 | Adapter 在解析前限制 body；超限不写 Inbox |
| max reply size | 流式/Markdown 单次内容限制和媒体限制由 AI Bot capability 固定 | text/card/file 限制由 Open API capability 固定 | Reply Projection 分片、流式更新或 Artifact/链接降级 |

## 11. Attachment / Recall Boundary

### 11.1 附件

本 Spec 中“附件/文件”是统一的平台级类别，表示消息中的非文本内容。图片、文档
以及 Provider 以独立媒体消息表示的其他内容，都沿用同一条
`provider media reference -> Artifact -> ArtifactRef` 链路；不为每一种媒体再设计
独立的 Gateway、Session 或 Runner 输入类型。原始需求明确提到的是图片/文件消息，
因此验收按“图片、文件等非文本附件”组织，不把某一种媒体另列为独立必做需求。

本需求必须完成以下附件链路：

1. IM-02/IM-03 解析 Provider 的 image/file/mixed/post 等非文本内容并提取受控
   media reference；IM-07 调用对应的 provider media client 下载、解密媒体；
2. 通过现有 Artifact/Object 能力写入租户作用域对象，记录 `artifact_ref`、
   MIME、size、digest 和 provider media kind；
3. Artifact 写入使用 `tenant/app/binding/external_message_id/item_no` 幂等键；
   数据库事务回滚时，孤儿对象由现有清理能力回收，不能因此先返回成功 ACK；
4. `gateway.Message.Validate` 接受文本、ArtifactRefs 或二者组合，拒绝空输入、
   未授权引用、跨租户引用和超出限制的媒体；
5. Worker 将已授权 ArtifactRef 转换为 tRPC-Agent-Go Runner 可消费的
   `model.Message` 内容，转换失败必须让 Execution 进入明确失败状态；
6. Inbox payload hash 和 Execution 输入快照覆盖 artifact digest/ref，不能只
   hash 文本；
7. Reply Projection 对平台级 ArtifactRef 执行 Provider upload/send，卡片和附件
   的降级只由 capability 决定，不能暴露内部对象路径。

raw binary 不进入 Session event、execution_event payload 或普通日志；只保存
租户作用域的受控引用。Provider 明确不支持的媒体类型可以写入
`REJECTED/ATTACHMENT_REJECTED` 或降级为文本/下载链接，但企业微信 AI Bot
和飞书 App 已明确支持的图片、文件、卡片和流式能力必须实现，不能以“附件后续
再做”为默认行为。

只修改 ArtifactRefs.Validate 而不完成上述链路，不算附件支持。

### 11.2 消息撤回

只有已验签的 recall event 才进入处理。撤回使用独立的
`channel_recall_inbox` 做幂等；它不复用消息 `channel_inbox` 的 ADMITTED/REJECTED
状态机，也不创建新的 Execution。

| Execution 状态 | 行为 |
| --- | --- |
| PENDING | 标记取消请求，阻止 Worker 新执行；不删除 Inbox |
| RUNNING | 通过 runner.ManagedRunner.Cancel(request_id) 请求取消；继续排空 Event Channel；不假设模型、Tool 或外部副作用已经撤销 |
| SUCCEEDED/FAILED | 不删除 Session、Memory、Tool 结果或已发送 Reply；只追加审计 |

撤回本身也需要 Provider event ID 的幂等键，且带 tenant/app/binding scope。撤回
记录、取消请求和审计必须在同一事务中提交；未知原消息可记录 REJECTED 并按协议
ACK，事务失败则不成功 ACK。不能把“IM 消息被撤回”解释为数据库事务回滚。

### 11.3 限流和失败

- 入站 body size、请求并发和解析耗时由 Adapter 保护；
- 出站 rate limit、token refresh 和 Provider HTTP 细节由 Adapter 的
  outbound client/codec 提供；错误分类、退避、Reply retry 和 lease 由 IM-06；
- Reply Outbox 提供有限 retry/lease；
- 不可恢复失败必须保留 PERMANENTLY_FAILED 和错误类别；
- 不做全平台容量治理、全局 Worker failover 或灾难恢复。

## 12. IM-01～IM-07 Dependency DAG

### 12.1 逻辑依赖

~~~text
IM-01 Trusted Binding
       |
       v
IM-04 Identity / Conversation
       |
       v
IM-05 Inbox + Execution + Dispatch
       | \
       |  +--> IM-02 WeCom Adapter --+
       |                              |
       +----> IM-03 Feishu Adapter --+--> IM-06 Reply Domain + Reply Outbox + Sender
                                             |
                                             v
                                  IM-07 Attachment / Recall / Channel Limits
~~~

正式逻辑边为：

- IM-01 -> IM-04：映射必须在 Binding scope 内；
- IM-01 -> IM-05：Inbox admission 只能使用 IM-01 导出的 trusted scope；
- IM-04 -> IM-05：Inbox transaction 需要已完成的 Identity/Conversation 映射；
- IM-05 -> IM-02、IM-03：Provider Adapter 建立在稳定的 Gateway、Inbox、
  Execution、Dispatch 和 commit-before-ACK 语义之上；
- IM-02、IM-03 -> IM-06：Reply Sender 只消费 Adapter 提供的 outbound
  codec/client contract 和 capability；
- IM-02、IM-03 -> IM-07：入站 AttachmentIngestor 依赖各 Adapter 提供的
  provider media reference 和 Binding-scoped media client；
- IM-06 -> IM-07：附件、撤回和 Provider 限制依赖已存在的入站/出站生命周期。

IM-02 与 IM-03 在逻辑上可以并行，二者不互相依赖；它们都必须先满足 IM-05
的统一 Inbox/事务契约。这里的逻辑依赖不改变“一个一个需求来”的串行交付约束。

`IM-05 -> IM-07` 还有一个有意的交付例外：IM-05 先冻结
`AttachmentIngestor.Prepare` 的调用点和失败语义，IM-07 在最后补上该接口的
实际实现。运行时它位于 Gateway/Store.Admit 之前；这不是把 IM-07 提前交付，
也不是允许媒体在 Execution 创建后再补录。IM-06 -> IM-07 只表示出站附件、撤回
和通道限制依赖 Reply 生命周期。

最终运行时路径（不是交付依赖图）为：

~~~text
IM-02/IM-03 Adapter
  -> VerifiedProviderEnvelope
  -> IM-07 inbound AttachmentIngestor
  -> IM-05 Inbox/Execution/Dispatch transaction
  -> Worker/Runner/Event
  -> IM-06 Reply Projection/Reply Outbox/Sender
  -> IM-07 outbound attachment/recall/limit handling
~~~

### 12.2 一次只开发一个任务的执行顺序

按当前“一个一个需求来”的约束，执行顺序固定为：

    IM-01 -> IM-04 -> IM-05 -> IM-02 -> IM-03 -> IM-06 -> IM-07

这只是串行交付顺序，不代表 IM-02 和 IM-03 存在互相依赖。未完成前一个
任务的代码、单元测试、必要数据库测试和验收结果，不进入下一个任务。

## 13. Per-task Implementation Plan

本节各任务按领域编号编排；实际单线程交付严格使用以下顺序：

    IM-01 -> IM-04 -> IM-05 -> IM-02 -> IM-03 -> IM-06 -> IM-07

因此 IM-05 的测试输入使用 fake trusted-scope Channel Input，不依赖已完成的
企业微信或飞书 Adapter；IM-02/IM-03 在 IM-05 的 Inbox、Execution、Dispatch
和 commit-before-ACK 语义冻结后实现。

### 13.0 任务 ownership、交接物和推荐落点

下面的目录是编码时的建议落点，不代表这些文件当前已经存在。文件可以合并，
但职责不能合并成一个跨协议的大 Adapter；同一文件如果被两个任务依赖修改，
必须按串行顺序逐次扩展，后一个任务不能重定义前一个任务的契约。

```text
trpcservice/channels/
  channels.go                 # 现有 Binding/Identity/Conversation 的领域模型
  input.go                    # 平台级 ChannelInput
  reply.go                    # Reply/Capability/单次出站调用契约，IM-05 冻结
  target.go                   # TargetProtector 窄契约，IM-04 使用
  wecom/
    adapter.go                # HTTP callback 编排
    protocol.go               # 企业微信 DTO，仅包内可见
    verify.go                 # URL 验证、签名、解密
    normalize.go              # DTO -> VerifiedProviderEnvelope
    outbound.go               # 单次出站 codec/client，不含 retry/lease
  feishu/
    adapter.go
    protocol.go               # 飞书 event/challenge DTO，仅包内可见
    verify.go
    normalize.go               # DTO -> VerifiedProviderEnvelope
    outbound.go               # 单次出站 codec/client，不含 retry/lease
trpcservice/postgres/
  binding_route.go            # public_route_id 定位
  channel_identity.go         # Identity/Conversation/Membership 持久化
  channel_inbox.go            # Inbox repository
  reply_outbox.go             # Reply Outbox repository
  admission.go                # IM-01 trusted seam，IM-05 原子事务扩展
trpcservice/worker/
  reply_projection.go         # Runner event -> Reply
  reply_sender.go             # Claim/Lease/Send/Retry
```

建议的唯一任务边界和交接如下：

| 任务 | 只负责 | 交给下一任务的稳定产物 | 明确不修改 |
| --- | --- | --- | --- |
| IM-01 | route lookup、Channel/ACTIVE、trusted Binding snapshot、Admission revalidation | `BindingSnapshot`、`ChannelBindingIdentityResolver`、stale/disable race 语义 | Provider DTO、验签/解密、Inbox、Identity、Runner |
| IM-04 | scoped Identity/Conversation、Session principal、target envelope | `MappedPrincipal`、lookup-or-create、`target_ref` 语义和 TargetProtector | Provider 协议、Inbox/Execution/Dispatch 事务、Reply Sender |
| IM-05 | verified input 的 Inbox 幂等、Identity/Conversation 与 Execution/Dispatch 原子提交、commit-before-ACK；冻结 `AttachmentIngestor` 调用点及 Reply/Capability/单次 outbound contract | `ChannelAdmissionResult`、Inbox replay/conflict 语义、预入站媒体失败语义、公共 Reply contract | WeCom/Feishu 协议、Reply Outbox 表、发送循环 |
| IM-02 | 企业微信 AI Bot HTTP、验签/解密、DTO 提取、provider media reference/消息级 target 提取、标准化、协议 ACK、WeCom 单次 outbound codec/client | WeCom `VerifiedProviderEnvelope` 适配和 outbound fake；媒体下载/解密和 Artifact ingest 交给 IM-07 | 公共模型重定义、Runner、Reply Outbox/Claim/Retry、Feishu |
| IM-03 | 飞书 HTTP、challenge、验证/解密、DTO 提取、provider media reference/target 提取、标准化、协议 ACK、Feishu 单次 outbound codec/client | Feishu `VerifiedProviderEnvelope` 适配和 outbound fake；媒体下载和 Artifact ingest 交给 IM-07 | 公共模型重定义、Runner、Reply Outbox/Claim/Retry、WeCom |
| IM-06 | Reply Projection、Reply Outbox、Claim/Lease、Sender lifecycle、发送错误策略和重试 | `SENT`/失败状态、provider_message_id、有限重试语义 | 入站协议、重新执行 Runner、通用故障接管 |
| IM-07 | 入站 pre-admission `AttachmentIngestor` 实现、ArtifactRef 到 Runner 的完整输入转换、撤回、Provider limit/capability、媒体和出站降级 | 入站/出站 Artifact、recall、限流和降级验收 | 通用多模态平台、全局容量治理 |

`ChannelInput`、`Reply`、`ProviderCapability`、`ProviderOutboundClient` 和
`TargetProtector` 只能各有一份平台契约。Provider 包可以有自己的 DTO、签名
和 HTTP request，但不能在包内复制平台 Reply 或把 Provider 字段加到 Gateway
请求中。`ProviderOutboundClient` 在 IM-02/03 阶段最多提供一次发送调用和 fake；
Claim、Lease、错误到状态的映射、退避和重试只在 IM-06 出现。

每次任务完成后，下一任务只读取上一任务的交接物和验收结果，不再重新评审
整个 IM 方案。若编码中发现必须改变已冻结契约，先停止当前任务并更新本 Spec，
不得在实现分支中私自扩展第二套入口。

### IM-01：可信 Binding 和入站准入

**Inputs**

- channels.Binding 当前字段和 ACTIVE/SUSPENDED 状态；
- public_route_id 路由决策；
- binding_revision 单调授权版本；
- gateway.TenantSourceVerifiedChannelBinding；
- tenant.RuntimeContext；
- 当前 Store.Admit 的 authenticated claims 事务模板。

**Existing code reused**

- trpcservice/channels/channels.go；
- trpcservice/config/config.go 的 exact BindingResolver；
- trpcservice/postgres/store.go 的 ResolveBinding、ResolveAgentApp；
- trpcservice/gateway/gateway.go；
- trpcservice/postgres/admission.go；
- tenant.Scope/RuntimeContext。

**Files likely affected**

- trpcservice/channels/channels.go；
- trpcservice/config/config.go 或新增窄的 route resolver contract；
- trpcservice/postgres/store.go；
- trpcservice/postgres/admission.go；
- trpcservice/gateway/gateway.go；
- 新增 IM migration；
- 对应 unit/PG integration tests。

**Schema changes**

- channel_binding.public_route_id；
- channel_binding.binding_revision；
- 全局 unique index；
- 不新增 route alias 双活结构；旧 route 轮换后立即失效。

**APIs introduced/extended**

- 保留 ResolveBinding(ctx, tenant, app, binding)；
- 增加独立 ResolveBindingByPublicRoute(ctx, channel, route_key) 窄接口；
- Channel binding identity resolver；
- AdmissionIdentity/ChannelBindingIdentityResolver 携带 PublicRouteID 和 binding_revision
  快照；
- Store.Admit 增加 verified_channel_binding 的 trusted source 校验和事务重校验
  hook；Inbox 幂等、Execution/Dispatch 写入归 IM-05 扩展。

**Tests**

- route 命中/未知/Channel 不匹配；
- route resolver 的校验不依赖 HTTP handler；Provider HTTP method/path、Content-Type、
  body limit 和脱敏 route fingerprint 日志由 IM-02/IM-03 的入口测试覆盖；
- public route 不能泄露或改变 tenant/app；
- ACTIVE/SUSPENDED；
- fake trusted-scope channel binding input 能进入 trusted admission；
- Tenant/App/Binding transaction revalidation；
- test trusted-scope 生成后、commit 前的 disable race；Provider verification
  的真实测试归 IM-02/IM-03；
- route rotation/stale route；
- public_route_id 不进入普通日志；
- 旧 exact ResolveBinding 调用方兼容。

**Done criteria**

- 合法 Binding 能产生 verified channel identity；
- 所有 tenant/app 来自 Binding；
- PostgreSQL transaction 能拒绝错误 scope、SUSPENDED、stale route 和
  binding_revision/授权属性不一致；
- Provider signature、decrypt、Verification Token、external_account extraction
  均不在本任务测试或实现范围内；
- 不触碰 Runner。

**Explicit non-goals**

- 企业微信/飞书具体协议；
- Inbox/Reply Outbox；
- 附件和撤回。

### IM-04：Identity 和 Session 映射

**Inputs**

- IM-01 导出的 tenant/app/binding/channel；
- 已冻结的 provider-neutral sender/chat/thread normalized input contract；
- channels.Identity、Conversation、Membership。

进入本任务编码前，必须使用 6.2.1 已冻结的 Provider target 持久化方案：先
复用现有 SecretProvider 取密钥，再以窄的 TargetProtector 生成 DB ciphertext
envelope；不得自行引入 KMS、Vault 或通用 Secret 系统。

**Existing code reused**

- trpcservice/channels/channels.go；
- tenant.Scope；
- platform.session_lane；
- internal/execution.Job 的 PartitionKey；
- Worker 对 SessionPrincipalID/UserID 的现有语义。

**Files likely affected**

- trpcservice/channels/channels.go；
- 新增 identity/conversation repository；
- trpcservice/postgres 新增 mapping 文件；
- 新增 migration；
- gateway/runtime context mapping tests。

**Schema changes**

- channel_identity；
- channel_conversation；
- channel_membership；
- HMAC key version；
- provider target ciphertext envelope（algorithm、key_version、nonce、
  ciphertext）；

**APIs introduced/extended**

- scoped Identity lookup-or-create；
- scoped Conversation lookup-or-create；
- group membership upsert；
- provider target seal/open，限定在 identity/reply adapter 层；内部 target
  明文只在同一 Admission/Reply 事务的受控作用域内短暂存在；
- 不新增第二套用户或会话领域模型。

**Tests**

- 跨 tenant、跨 app、跨 binding 相同 external ID；
- direct/group/topic；
- concurrent first insert；
- same conversation membership；
- HMAC key version 读取；
- target 不能跨 Binding 使用；
- TargetProtector round-trip、错误 scope/AAD 拒绝、active key version 写入；
- key rotation 后新 envelope 使用新版本，旧 envelope 仍可读取。

**Done criteria**

- 同一个 scope 的外部主体稳定映射到同一个内部 ID；
- 不同 scope 永不冲突；
- direct/group/topic 生成正确 SessionPrincipalID；
- user_id 保留真实发言人；
- Session Lane key 与现有 Worker 一致。

**Explicit non-goals**

- Provider 协议解析；
- 通用用户合并/SSO；
- Tool Permission；
- 个人资料同步。

### IM-02：企业微信 Adapter

**Inputs**

- IM-01 route/admission contract；
- IM-04 normalized mapping；
- IM-05 已冻结的 Inbox/Execution/Dispatch transaction 和 commit-before-ACK
  contract；
- 已冻结的企业微信 AI Bot URL 回调协议、目标账号配置和 capability。

**Existing code reused**

- trpcservice/channels/wecom/doc.go 作为包入口；
- channels.Binding；
- gateway.Gateway；
- config/secret resolver；
- IM-05 的 Gateway/Inbox admission contract；
- provider media reference/client contract；实际 Artifact ingest 由 IM-07 编排；
- 供 IM-06 使用的 platform Reply outbound codec/client contract。

**Files likely affected**

- trpcservice/channels/wecom/*.go；
- adapter HTTP wiring；
- fake WeCom server tests；
- 不直接修改 worker/runner。

**Schema changes**

- 无新增专属 Schema；依赖 IM-01/04 的表。

**APIs introduced/extended**

- WeCom protocol adapter；
- WeCom outbound codec/client contract 和 fake（不包含 Sender lifecycle）；
- provider capability declaration；
- 只暴露平台级 Input/Reply，不暴露加密 JSON DTO。

**Tests**

- AI Bot URL verification、签名、解密、空 receive_id、aibotid；
- message ID、direct/group、text/image/mixed/file 和非文本附件引用；
- provider media reference 提取、card/stream/event；媒体下载/解密和 Artifact ingest
  在 IM-07 验收；
- unsupported provider type；
- ACK timing and transaction failure；
- outbound codec/client fake 的请求编码和 provider target 绑定。

**Done criteria**

- 合法 AI Bot 回调能把所有已声明支持的消息类型变成标准 Input 并调用 Gateway；
- 图片/文件能产出受控 provider media reference，供 IM-07 完成 Artifact ingest；
- Adapter 不直接调用 Runner；
- commit 前失败不返回成功 ACK；
- 只提供 IM-06 可调用的 outbound codec/client contract；
- 不创建 Reply Outbox、Claim/Lease 或 Reply retry loop。

**Explicit non-goals**

- Feishu 协议；
- 通用 outbox lease；
- Reply Sender lifecycle；
- 通用消息平台治理。

### IM-03：飞书 Adapter

**Inputs**

- IM-01 route/admission；
- IM-04 mapping；
- IM-05 已冻结的 Inbox/Execution/Dispatch transaction 和 commit-before-ACK
  contract；
- 已冻结的 `im.message.receive_v1`、官方 `oapi-sdk-go/v3`、目标 App 权限和
  Open API 配置。

**Existing code reused**

- trpcservice/channels/feishu/doc.go；
- channels.Binding；
- gateway.Gateway；
- config/secret resolver；
- IM-05 的 Gateway/Inbox admission contract；
- provider media reference/client contract；实际 Artifact ingest 由 IM-07 编排；
- 供 IM-06 使用的 platform Reply outbound codec/client contract。

**Files likely affected**

- trpcservice/channels/feishu/*.go；
- HTTP wiring；
- fake Feishu server tests。

**Schema changes**

- 无新增专属 Schema；依赖公共 Inbox/identity/conversation。

**APIs introduced/extended**

- Feishu event adapter；
- Feishu Open API outbound codec/client contract 和 fake（不包含 Sender lifecycle）；
- challenge response；
- capability declaration。

**Tests**

- challenge、token/signature/encryption；
- `im.message.receive_v1` 的 open_id/chat_id/message_id；
- direct/group/thread 的 text/image/file/post/interactive；
- provider media reference 提取、卡片动作和消息更新；媒体下载和 Artifact ingest
  在 IM-07 验收；
- malformed/oversized/duplicate event；
- Open API request encoding、target binding 和 fake client contract；
- 不在本任务验证 Reply retry/lease 或完整 error classification。

**Done criteria**

- challenge 不创建 Execution；
- 合法事件和媒体 reference 事件走同一 Gateway/Inbox 入口；
- Provider JSON 不进入 Gateway/Worker/Session；
- provider media reference 能交给 IM-07 完成租户作用域 Artifact ingest；
- 只提供 IM-06 使用的 outbound codec/client contract；
- 不创建 Reply Outbox、Claim/Lease 或 Reply retry loop。

**Explicit non-goals**

- WeCom 协议；
- 通用卡片设计器；
- 全局限流系统。

### IM-05：Inbox、幂等和 ACK

**Inputs**

- IM-01 trusted admission；
- IM-04 mapping；
- fake provider-verified `VerifiedProviderEnvelope`/materialized ChannelInput contract；
- `AttachmentIngestor.Prepare` 的调用点、返回值和 no-row/REJECTED 失败语义；
- 当前 Execution/Dispatch transaction。

**Existing code reused**

- trpcservice/postgres/admission.go；
- platform.execution；
- platform.dispatch_outbox；
- platform.session_lane；
- gateway.AdmissionResult。

**Files likely affected**

- 新增 channel_inbox migration；
- trpcservice/postgres/admission.go；
- 新增 inbox/replay helper；
- Gateway integration tests；
- PostgreSQL integration tests。

**Schema changes**

- channel_inbox；
- unique scope key；
- provider_timestamp/message_type、消息级 reply target envelope 和过期时间；
- 撤回事件使用独立 `channel_recall_inbox`，不改变消息 Inbox 的状态机；
- 不改 Dispatch Outbox 的生命周期。

**APIs introduced/extended**

- 在 IM-01 verified channel branch 上扩展 Store.Admit 的 Inbox 幂等与写入逻辑；
- Inbox lookup/replay result；
- ChannelAdmissionMetadata（包含 binding_revision、message_type、reject_reason）；
- 冻结 platform Reply、ProviderCapability、ProviderOutboundClient 的最小类型
  契约，供 IM-02/03 定义 codec/client fake；不实现 Reply Outbox；
- 不增加绕过 Gateway 的 enqueue API。

**Tests**

- first, sequential/concurrent duplicate；
- same key different hash；
- DB rollback；
- lost HTTP ACK retry；
- Binding disable race；
- Execution/Dispatch count；
- verified unsupported type 写入 REJECTED + audit 且不创建 Execution/Dispatch；
- pre-admission AttachmentIngestor 的成功、可重试失败和永久不支持媒体语义；
- supported text 的 Identity/Conversation 新建与 Inbox/Execution/Dispatch 同事务
  回滚。

这些测试直接使用 fake trusted-scope channel binding input；不要求 WeCom 或
Feishu 协议解析已经完成。

**Done criteria**

- Inbox/Execution/Dispatch 同事务；
- commit 前不 ACK；
- duplicate 原 request_id；
- same key different hash 冲突；
- DB 唯一约束证明并发安全；
- 只扩展 IM-01 已建立的 verified channel branch，不重新定义 trusted admission；
- platform Reply/Capability/OutboundClient 最小契约已冻结，供 IM-02/03 使用。

**Explicit non-goals**

- Reply Outbox；
- Provider-specific reply；
- 全局灾备。

### IM-06：Runner 到 IM Reply

**Inputs**

- 已提交 Execution；
- Worker Runner Event Channel；
- execution_event；
- Adapter capability；
- IM-05 已冻结的 Reply、ProviderCapability、ProviderOutboundClient 契约。

**Existing code reused**

- worker.Worker EventSink；
- event.Event.IsRunnerCompletion；
- runner.ManagedRunner；
- execution_event；
- Dispatch/Execution claim pattern。

**Files likely affected**

- trpcservice/worker/worker.go 或新增 Reply Projection；
- trpcservice/postgres 新增 reply outbox repository；
- channels/wecom、channels/feishu sender；
- 新 migration；
- fake sender/integration tests。

**Schema changes**

- reply_outbox；
- reply_projection_state（按 tenant/app/request_id 保存最后已投影的 event_seq）；
- logical_reply_id/revision/reply_id；
- source_event_id、target_ref、provider_message_id、attempt、lease。

**APIs introduced/extended**

- ReplyProjector；
- ordered event projection/cursor update；
- ReplySender；
- Reply Outbox Claim/Complete/Retry；
- Adapter capability query。

**Tests**

- final text；
- streaming update/final-only；
- multipart ordering；
- success/timeout/429/permanent；
- duplicate outbox delivery；
- source event replay、event gap 和 projection cursor；
- fallback；
- Runner invocation count unchanged on retry。

**Done criteria**

- Worker 不依赖 Provider；
- Reply 重试不重新执行 Runner；
- outbox lease 可恢复；
- target scope 正确；
- 用户不可见内部事件不发送。

**Explicit non-goals**

- 完整 Worker takeover；
- 所有 Provider 的 exactly-once；
- 通用通知平台。

### IM-07：附件、撤回和通道限制

**Inputs**

- IM-02/03 capability；
- IM-05 已冻结的 `AttachmentIngestor.Prepare`、Inbox reject 和 no-row 失败语义；
- Artifact/Object 服务；
- `gateway.Message`/Admission command 的 ArtifactRef 契约；
- Execution 状态和 ManagedRunner；
- Reply Outbox。

**Existing code reused**

- tRPC-Agent-Go Artifact 能力，需以当前 runtime wiring 为准；
- worker ManagedRunner cancellation；
- Execution event/audit；
- Reply Outbox retry。

**Files likely affected**

- trpcservice/channels/wecom、feishu；
- trpcservice/artifact 或平台 artifact boundary；
- trpcservice/postgres；
- worker cancellation/recalled request；
- 新增边界测试。

**Schema changes**

- 扩展 Execution 输入快照/映射以保存租户作用域 ArtifactRef、digest 和媒体元数据，
  不保存二进制；
- 必要时补充 Artifact ingest 幂等键和清理标记；
- `channel_recall_inbox`：按 tenant/app/binding/external_event_id 持久化撤回事件
  幂等事实，不混入消息 Inbox 状态机；
- recall 不删除既有数据，不需要软删除 Session 表。

**APIs introduced/extended**

- `AttachmentIngestor.Prepare` 的实际实现：provider media reference -> ArtifactRef；
- attachment classification、artifact ingest/reference；
- recall/cancel request；
- Adapter limits/capabilities。

**Tests**

- image/file boundary；
- pre-admission media download/decrypt success、可重试失败和永久不支持；
- artifact reference 不进入 raw Session；
- pending/running/completed recall；
- duplicate recall event 不重复取消或追加审计；
- chunk boundary；
- unsupported type fallback；
- rate limit and permanent send failure。

**Done criteria**

- 入站部分：在 IM-05 Admission 前完成图片/文件及其他附件的下载、解密、Artifact
  ingest 和 ArtifactRef 返回；可重试失败 no-row/不成功 ACK，永久不支持写
  `REJECTED/ATTACHMENT_REJECTED`；
- 入站 ArtifactRef 已通过 Gateway 校验、持久化并完成 Worker/Runner 输入转换；
- 出站图片/文件/卡片已按 Provider capability 发送，无法表达时有明确降级；
- recall 行为按 Execution 状态执行；
- recall event 通过 `channel_recall_inbox` 幂等，取消请求和审计不重复；
- provider limit 和错误分类不泄漏到 Gateway；
- 不删除历史审计和 Session。

**Explicit non-goals**

- 完整多模态 Runner；
- 通用内容安全平台；
- 全局容量治理；
- 灾难恢复。

## 14. Database Changes

### 14.1 Existing tables reused without semantic reuse

| Table | Reuse |
| --- | --- |
| platform.tenant | Admission 锁定并验证 ACTIVE |
| platform.agent_app | Admission 锁定并验证 ACTIVE、读取 active_config_version |
| platform.app_config_version | 校验 published config 和 binding scope |
| platform.channel_binding | 绑定资源；增加 public_route_id |
| platform.session_lane | 按 session principal 分配 turn_seq |
| platform.execution | IM request 的唯一执行记录 |
| platform.dispatch_outbox | Execution 到 Worker 的分发 |
| platform.execution_event | Runner 事件和跨节点投影输入 |
| platform.audit_event | recall、永久失败和必要安全事件 |

Dispatch Outbox 不改成 Reply Outbox。两者只能共享 claim/lease 的实现模式，
不能共享表或状态。

### 14.2 New/extended tables

#### channel_binding extension

- public_route_id：NOT NULL、全局 UNIQUE、非空；
- binding_revision：NOT NULL，从 1 开始；所有授权相关变更单调递增；
- 不改变现有主键 tenant_id/app_id/binding_id；
- route key 不允许包含租户信息；
- route key 不立即复用。

#### channel_identity

建议字段：

- tenant_id、app_id、binding_id、channel；
- external_user_key_hash、key_version；
- user_id、status；
- provider_target_envelope JSONB NOT NULL（AES-256-GCM envelope，字段见 6.2.1）；
- created_at、updated_at。

唯一键：

    (tenant_id, app_id, binding_id, external_user_key_hash)

#### channel_conversation

建议字段：

- tenant_id、app_id、binding_id、channel；
- external_chat_key_hash、thread_key_hash、key_version（hash 字段 NOT NULL）；
- conversation_id、session_principal_id、scope；
- provider_target_envelope JSONB NOT NULL（chat/thread target 的 AES-256-GCM envelope）；
- created_at、updated_at。

唯一键：

    (tenant_id, app_id, binding_id, external_chat_key_hash, thread_key_hash)

#### channel_membership

建议字段：

- tenant_id、app_id、conversation_id、user_id；
- role、status、created_at、updated_at。

唯一键：

    (tenant_id, app_id, conversation_id, user_id)

#### reply_projection_state

这是 IM Reply Projection 专用的最小顺序游标，不是通用事件恢复框架。

建议字段：

- tenant_id、app_id、binding_id、request_id；
- last_event_seq NOT NULL，从 0 开始；
- updated_at。

唯一键：

    (tenant_id, app_id, binding_id, request_id)

Projection 在事务内锁定该行，按 `execution_event.event_seq` 递增处理；重复或
更旧事件不再生成 Reply，存在 event gap 时等待前序事件。该表与 Reply Outbox
同属 IM-06 的局部投影可靠性，不承担通用 Worker takeover。

#### channel_inbox

建议字段：

- tenant_id、app_id、binding_id；
- external_message_id；
- payload_hash BYTEA(32)；
- request_id；
- status；
- message_type NOT NULL；
- reject_reason NULL；REJECTED 行必须为 UNSUPPORTED_MESSAGE_TYPE 或
  ATTACHMENT_REJECTED；
- provider_reply_target_envelope JSONB NULL；消息级 Provider target 的
  AES-256-GCM envelope，AAD 使用 channel_inbox/request_id；
- reply_target_expires_at NULL；消息级 target 的有效期；
- provider_timestamp、provider_metadata_fingerprint（可选）；
- created_at、updated_at。

唯一键：

    (tenant_id, app_id, binding_id, external_message_id)

#### channel_recall_inbox

建议字段：

- tenant_id、app_id、binding_id；
- external_event_id：Provider 撤回事件 ID，已规范化；
- request_id：关联原消息 Execution 的内部 ID；
- status：`APPLIED` 或已验证但无法关联原消息的 `REJECTED`；
- payload_hash、created_at、updated_at。

唯一键：

    (tenant_id, app_id, binding_id, external_event_id)

撤回事件与取消请求、审计记录在同一事务中提交；不写 ACKED，不删除原 Inbox、
Session 或 Execution。重复撤回只返回原 request_id，不重复发送取消请求或审计动作。

#### reply_outbox

建议字段：

- reply_id、logical_reply_id、tenant_id、app_id、binding_id、request_id；
- source_event_id、part_no、revision、operation、reply_kind；
- target_ref JSONB（target_kind + internal_target_id）、payload、artifact_ref；
- status、attempt、next_attempt_at；
- lease_owner、lease_until；
- provider_message_id；
- last_error_type、last_error；
- created_at、updated_at。

唯一键：

    (tenant_id, app_id, binding_id, request_id, source_event_id,
     logical_reply_id, part_no, revision, operation)

### 14.3 Migration order

1. `000010_channel_binding_route_columns.sql` 增加带数据库默认值的可空
   `channel_binding.public_route_id` 与 `binding_revision`，用 `pgcrypto` 为存量
   Binding 回填随机 route 和 `1`；该迁移独立提交后仍允许约束尚未启用的过渡状态，
   旧二进制写入缺失的新列时由数据库默认值补齐；
2. `000011_channel_binding_route_constraints.sql` 在另一个独立事务中设置 NOT NULL、
   非空/正数检查、全局唯一索引，并建立 Binding scope 不可变及授权属性变更自动
   递增 `binding_revision` 的触发器；
3. 建立 Identity/Conversation/Membership 表；
4. 建立 channel_inbox；
5. 建立 channel_recall_inbox；
6. 建立 reply_projection_state；
7. 建立 reply_outbox；
8. 最后启用 IM Admission/Reply 代码。

迁移执行器按版本顺序为每个 migration 建立独立事务；发布时必须先完成
`000010`/`000011`，再启动读取新列的新二进制，不允许新二进制连接到未执行
`000010` 的旧 Schema。

## 15. API Changes

### 15.1 保持不变的现有契约

- config.BindingResolver.ResolveBinding(ctx, tenantID, appID, bindingID) 继续
  做 exact scoped lookup；
- gateway.TenantSourceVerifiedChannelBinding 的字符串值不变；
- gateway.Request、Gateway.Handle 继续作为通用入口；
- execution.Job、Worker.Run、runner.Runner、runner.ManagedRunner 不被 Adapter
  绕过；
- Dispatch Outbox 的状态和生命周期不承载 Reply。

### 15.2 需要新增或扩展的契约

| Contract | 设计 |
| --- | --- |
| Public route resolver | 独立窄接口 ResolveBindingByPublicRoute(ctx, channel, routeKey)，返回包含 public_route_id、binding_revision 和授权属性快照的 BindingSnapshot；Gateway 的 ResolveChannelBindingRoute 将其封装成带包内 provenance marker 的 LocatedChannelBinding；不修改现有 BindingResolver，避免破坏外部实现 |
| Binding | 增加 PublicRouteID、BindingRevision；Binding.Validate 检查非空/正数，唯一性和单调递增由 DB/更新事务保证 |
| Channel binding identity | Provider 验证完成后，只有由 ResolveChannelBindingRoute 返回的、带包内 provenance marker 的 LocatedChannelBinding 才能传入 NewChannelBindingIdentityResolver；它生成 Source=verified_channel_binding、SourceID=内部 binding_id 的 AdmissionIdentity；AdmissionIdentity 对外保留字段兼容性，但 direct channel-binding literal 无 provenance 时必须拒绝；PublicRouteID 和 BindingRevision 只用于 transaction stale snapshot 检查 |
| Binding provisioning | 管理面创建 Binding 时不接受调用方提供的 public_route_id/binding_revision，由 admin API 生成 route 并使用 revision=1；Store 对低层缺省调用生成 route，对显式 route 要求通过与 NewPublicRouteID 一致的生成格式校验 |
| Channel Input | 放在 channels/channel domain；只含平台级字段和受控引用，不含 XML/JSON DTO |
| Identity mapper | scoped lookup-or-create；返回现有 channels.Identity/Conversation |
| Store.Admit | IM-01 建立 verified channel trusted source 和 transaction revalidation；IM-05 在该分支内加入 Inbox 幂等、Identity/Conversation、Execution、Dispatch 写入 |
| Reply | IM-05 冻结平台级 Reply、ProviderCapability、ProviderOutboundClient 最小契约；不依赖具体 Provider client |
| TargetProtector | channels 层窄 Seal/Open 接口；复用 SecretProvider 取版本化密钥材料，使用 AES-256-GCM envelope；不拥有 Secret 生命周期 |
| Reply Outbox | Claim/Complete/Retry/Recover 的局部 API；所有方法显式带 tenant/app |
| Adapter | 每个 Provider 独立实现 Verify/Normalize/Capabilities，并提供 IM-06 使用的 outbound codec/client；Reply Sender lifecycle 不属于 Adapter |

为避免各任务自行发明包装层，最小调用语义固定如下（具体 Go 名称可按现有
包命名落地，但不得改变职责）：

```text
InboundAdapter.HandleHTTP(ctx, binding_snapshot, http_request)
    -> ChallengeResponse | VerifiedProviderEnvelope | ProtocolError

AttachmentIngestor.Prepare(ctx, trusted_scope, envelope)
    -> materialized ChannelInput | IngestError

ProviderCapability
    -> supports_update / supports_card / supports_artifact / supports_streaming
    -> requires_initial_stream_response / supports_finalize
    -> max_text_size / target_ttl / provider_limit_metadata

ProviderOutboundClient.SendOnce(ctx, reply, resolved_provider_target, outbound_context)
    -> ProviderReceipt(provider_message_id) | ProviderSendError
```

其中：

- `HandleHTTP` 是一次请求级调用；Webhook Adapter 不要求实现 OpenClaw 的后台
  `Channel.Run`。只有未来接入长轮询 Provider 时，才引入后台生命周期；
- `SendOnce` 只编码并执行一次 Provider 操作（SEND、UPDATE 或 FINALIZE），不
  Claim、不更新 Reply Outbox、不重试、不调用 Runner。`outbound_context` 只包含
  当前 operation 所需的受控 provider_message_id/stream context，来源于 scoped
  target 解密或前一条已发送 Reply；不能写入平台普通字段。Provider 包可把 wire
  error 转成不含完整 body/secret 的稳定 `ProviderSendError`；是否重试、退避、
  永久失败和状态转换由 IM-06 决定；
- `resolved_provider_target` 只在受控发送调用期间由 `target_ref` 解密得到，
  不写入 Gateway、Session 或日志。平台记录仍只保存 `target_ref`；
- `ChallengeResponse` 不创建 Inbox、Execution 或 Dispatch；`ChannelInput`
  才进入 IM-05 的 Admission 事务。

### 15.3 ConfigVersion 兼容处理

当前 AdmissionIdentity.Validate 间接要求 RuntimeContext.ConfigVersion 非空，
而最终配置版本由 Admission transaction 决定。本需求不放松现有验证，而是：

1. route Binding 得到 tenant/app；
2. trusted resolver 读取当前 active_config_version，构造 RuntimeContext；
3. Gateway 校验；
4. Store.Admit transaction 再次锁 App 和 active config，并把最终版本写入
   Execution。

如果后续要允许 ingress 省略 ConfigVersion，应单独修改 RuntimeContext/
AdmissionIdentity contract，并增加公共 API 兼容评审；不在 Adapter 中填虚假
版本。

## 16. Gherkin Acceptance Scenarios

以下场景是实现和 QA 的验收基线。Provider 特有字段由对应 fake server 生成，
不把未确认的官方字段写入平台契约。

### Feature: IM-01 trusted binding and admission

~~~gherkin
Scenario: valid route and verified binding are admitted
  Given an ACTIVE channel Binding with public_route_id "route-a"
  And its binding_revision is 7
  And a test trusted-scope Channel Input carries the Binding trusted scope
  When IM-01 handles the route "/im/wecom/route-a"
  Then tenant_id and app_id are taken from the Binding
  And the request uses tenant source "verified_channel_binding"
  And PostgreSQL revalidates Tenant, App and Binding in the admission transaction

Scenario: disabled binding is rejected
  Given a SUSPENDED Binding
  And a test trusted-scope Channel Input targets its public_route_id
  When IM-01 handles the route
  Then the request is rejected
  And no successful HTTP ACK is returned
  And no Execution is created

Scenario: unknown route key is rejected
  Given no Binding has public_route_id "unknown"
  When IM-01 handles the route "/im/wecom/unknown"
  Then the request is rejected
  And no tenant is inferred from the payload
  And no Inbox or Execution is created

Scenario: channel mismatch is rejected before trusted admission
  Given public_route_id "route-a" belongs to a Feishu Binding
  When IM-01 handles "/im/wecom/route-a"
  Then the request is rejected
  And Gateway is not called
  And no Inbox or Execution is created

Scenario: payload tenant claim cannot cross route scope
  Given public_route_id "route-a" belongs to tenant "tenant-a"
  And a test trusted-scope input contains an untrusted tenant value "tenant-b"
  When IM-01 builds the trusted scope
  Then the request uses tenant "tenant-a"
  And it is never routed to tenant "tenant-b"

Scenario: stale public route is rejected after rotation
  Given Binding "b-1" rotated from public_route_id "route-a" to "route-b"
  When a test trusted-scope input carries the stale route snapshot "route-a"
  And PostgreSQL admission revalidates the Binding
  Then the transaction rejects the input
  And no Inbox, Execution or Dispatch Outbox is committed

Scenario: binding is disabled after verification but before commit
  Given a test trusted-scope Channel Input has passed the Adapter boundary
  When the Binding is changed to SUSPENDED before PostgreSQL commit
  Then the admission transaction rejects the callback
  And Inbox, Execution and Dispatch Outbox are rolled back
  And no successful HTTP ACK is returned

Scenario: authorization revision change rejects a stale verified input
  Given a test trusted-scope Channel Input carries binding_revision 7
  When Binding.ExternalAccount or its verification SecretRef changes to revision 8 before commit
  Then the admission transaction rejects the input
  And no Inbox, Execution or Dispatch Outbox is committed
  And no successful HTTP ACK is returned

Scenario: public route is not written to ordinary logs
  Given an ACTIVE Binding with public_route_id "route-a"
  When IM-01 processes a valid route
  Then ordinary logs contain only a route fingerprint
  And ordinary logs do not contain "route-a"
~~~

### Feature: IM-02 WeCom adapter

~~~gherkin
Scenario: WeCom URL verification does not create an execution
  Given a valid WeCom verification request
  When the WeCom Adapter handles it
  Then it returns the protocol verification response
  And Gateway is not called
  And Execution count remains zero

Scenario: WeCom invalid signature is rejected by the Adapter
  Given route lookup and ACTIVE Binding admission have succeeded
  When the WeCom callback signature is invalid
  Then IM-02 rejects the callback before producing Channel Input
  And Gateway is not called
  And no channel_inbox row is created

Scenario: WeCom decryption failure is rejected by the Adapter
  Given route lookup and ACTIVE Binding admission have succeeded
  When the WeCom encrypted body cannot be decrypted
  Then IM-02 rejects the callback
  And no Gateway submission is made

Scenario: WeCom external account mismatch is rejected
  Given a verified WeCom callback whose external account differs from Binding.ExternalAccount
  When IM-02 extracts provider fields
  Then it rejects the callback
  And no Channel Input is submitted

Scenario: WeCom direct text message is normalized
  Given a verified WeCom direct text callback with a stable message ID
  When the Adapter normalizes it
  Then it produces the standard Channel Input
  And no WeCom provider field is present in the Gateway message

Scenario: WeCom group message preserves the group principal
  Given a verified WeCom group text callback
  When the Adapter handles it
  Then user_id is the real sender
  And session_principal_id is the mapped group conversation
  And session_id is "default"

Scenario: WeCom adapter hands media reference to IM-07
  Given a verified WeCom direct image or file callback with a stable message ID
  When the WeCom Adapter normalizes the message
  Then it hands a validated provider media reference to IM-07
  And it does not write an Artifact or expose raw media bytes to Gateway, Session or ordinary logs

Scenario: WeCom callback body over the configured limit is rejected
  Given a callback body larger than the Adapter limit
  When the request is received
  Then it is rejected before full parsing
  And no Runner is invoked
~~~

### Feature: IM-03 Feishu adapter

~~~gherkin
Scenario: Feishu url_verification returns challenge
  Given a valid Feishu url_verification event
  When the Feishu Adapter handles it
  Then it returns the challenge response required by the provider
  And no Inbox or Execution is created

Scenario: Feishu text event uses open_id chat_id and message_id only inside adapter mapping
  Given a verified Feishu text event
  When the Adapter normalizes it
  Then it maps open_id to an internal Identity
  And it maps chat_id to an internal Conversation when present
  And message_id becomes external_message_id
  And raw Feishu JSON does not enter Gateway, Worker or Session

Scenario: Feishu adapter hands media reference to IM-07
  Given a verified Feishu message event contains an image or file resource key
  When the Feishu Adapter normalizes the message
  Then it hands a validated provider media reference to IM-07
  And the resource key and raw Feishu JSON do not enter Gateway, Worker or Session

Scenario: Feishu verification failure is rejected
  Given an event with an invalid token, signature or encryption configuration
  When the Adapter handles it
  Then Gateway is not called
  And no Inbox row is created

Scenario: WeCom and Feishu produce the same platform input semantics
  Given equivalent direct text messages from WeCom and Feishu
  When both adapters normalize them
  Then their platform fields are channel, binding, message ID, sender, session and text
  And no provider-specific field is required by Worker or Runner
~~~

### Feature: IM-04 identity and session mapping

~~~gherkin
Scenario: same external user ID in different tenants is isolated
  Given tenant-a and tenant-b use the same provider user ID
  When both messages are mapped
  Then they produce different scoped Identity records
  And neither tenant can resolve the other tenant's user_id

Scenario: same external user ID in different bindings is isolated
  Given two Bindings in one application use the same provider user ID
  When both messages are mapped
  Then the Binding scope is part of the lookup key
  And the records do not collide

Scenario: direct message uses user principal
  Given a verified direct message from an external sender
  When Identity mapping completes
  Then session_principal_id equals the internal user_id
  And session_id equals "default"

Scenario: group message uses conversation principal and real speaker
  Given a verified group message
  When Identity and Conversation mapping completes
  Then user_id equals the real speaker
  And session_principal_id equals conversation_id
  And the group context is retained

Scenario: concurrent first mapping returns one internal identity
  Given two valid callbacks concurrently reference the same scoped external user
  When both perform lookup-or-create
  Then the database unique constraint permits one Identity row
  And both callers receive the same user_id

Scenario: concurrent first group mapping returns one conversation
  Given two valid callbacks concurrently reference the same scoped group without a thread
  When both perform lookup-or-create
  Then the no-thread sentinel is non-NULL and binding-scoped
  And the database unique constraint permits one Conversation row
  And both callers receive the same conversation_id
~~~

### Feature: IM-05 Inbox idempotency and transaction

以下 IM-05 场景直接使用 test trusted-scope Channel Input，不依赖已完成的
企业微信或飞书协议 Adapter。

~~~gherkin
Scenario: first message creates one execution and one dispatch
  Given a valid test trusted-scope Channel Input with external_message_id "m-1"
  When admission commits
  Then channel_inbox count is 1
  And Execution count is 1
  And Dispatch Outbox count is 1
  And the HTTP ACK is sent only after commit

Scenario: sequential duplicate returns original request ID
  Given an admitted message with external_message_id "m-1"
  When the same message is delivered again with the same payload hash
  Then the original request_id is returned
  And no second Execution is created
  And no second Dispatch Outbox is created

Scenario: concurrent duplicate is serialized by database uniqueness
  Given two concurrent deliveries have the same tenant/app/binding/message key
  When both attempt admission
  Then exactly one Inbox row is committed
  And exactly one Execution is committed
  And both successful responses refer to the same request_id

Scenario: same idempotency key with different payload is rejected
  Given an admitted message with external_message_id "m-1"
  When the same key is delivered with a different payload hash
  Then an idempotency conflict is returned
  And the original Inbox and Execution are unchanged

Scenario: database rollback cannot produce a successful ACK
  Given the database fails before transaction commit
  When a valid callback is processed
  Then no Inbox, Execution or Dispatch Outbox remains
  And the response is non-success

Scenario: verified provider-unsupported type is durably rejected without execution
  Given a test trusted-scope Channel Input with message_type "unsupported" and external_message_id "m-unsupported"
  When admission commits
  Then one channel_inbox row has status "REJECTED"
  And reject_reason is "UNSUPPORTED_MESSAGE_TYPE"
  And one redacted audit event is recorded
  And Execution and Dispatch Outbox counts remain zero
  And a success ACK is allowed only after commit

Scenario: lost HTTP ACK is recovered by provider retry
  Given the admission transaction committed for external_message_id "m-1"
  And the first HTTP ACK is lost
  When the provider retries "m-1"
  Then the original request_id is returned
  And Execution count remains 1
  And Runner invocation count is not increased by the retry

Scenario: provider duplicate does not re-arm a failed execution
  Given an admitted Inbox has an associated FAILED Execution
  When the same provider message is delivered again
  Then the original request_id and Inbox status are returned
  And the FAILED Execution is not re-armed by the webhook path
  And Runner invocation count does not increase
  And an explicit business retry command is required to retry execution

Scenario: pre-admission media failure does not create execution
  Given a verified input contains a provider media reference
  When media download or Artifact ingest fails with a retryable error before admission
  Then no Inbox, Execution or Dispatch Outbox is committed
  And no successful HTTP ACK is returned

Scenario: permanent unsupported media is durably rejected
  Given a verified input contains a permanently unsupported media reference
  When pre-admission media classification completes
  Then one Inbox row has status "REJECTED"
  And reject_reason is "ATTACHMENT_REJECTED"
  And Execution and Dispatch Outbox counts remain zero
  And a success ACK is allowed only after the rejection transaction commits
~~~

### Feature: IM-06 reply projection and outbox

~~~gherkin
Scenario: final text is sent successfully
  Given an Execution reaches Runner completion with user-visible text
  When Reply Projection runs
  Then one Reply Outbox row is created
  And the fake Provider receives the text
  And the row becomes SENT

Scenario: sender timeout is retried without rerunning Runner
  Given a Reply Outbox row is PENDING
  And the Provider send request times out
  When the Sender retries
  Then attempt increases
  And the row remains retryable
  And Runner invocation count does not increase

Scenario: provider rate limit is retryable
  Given the Provider returns a rate-limit response
  When the Sender classifies the response
  Then next_attempt_at is set
  And the Reply remains PENDING
  And Runner is not called again

Scenario: permanent provider failure is retained
  Given the Provider returns a permanent invalid-target response
  When the Sender classifies the response
  Then the row becomes PERMANENTLY_FAILED
  And the failure is observable
  And no retry silently drops the row

Scenario: duplicate outbox delivery does not create duplicate logical operations
  Given one reply_id is claimed twice because a lease expired
  When both send attempts complete
  Then the database accepts at most one SENT transition for that reply_id
  And no new Execution is created

Scenario: source event replay reuses the existing reply operation
  Given one execution_event with source_event_id "req-1:7" was projected
  When the same execution_event is projected again
  Then no second Reply Outbox row is created
  And the original reply_id is reused
  And Runner invocation count does not increase

Scenario: out-of-order execution events do not send stale content
  Given execution_event sequence 8 arrives before sequence 7 for one request
  When Reply Projection processes sequence 8
  Then it waits for sequence 7 and does not create a send operation for sequence 8
  When sequence 7 is committed and sequence 8 is processed again
  Then projection follows event sequence order
  And no stale revision is sent before its predecessor

Scenario: streaming reply has explicit send update and finalize operations
  Given the target Adapter supports streaming and requires an initial response
  When Reply Projection receives visible intermediate and completion events
  Then it creates ordered SEND, UPDATE and FINALIZE operations for one logical reply
  And each operation is independently claimable from Reply Outbox
  And no operation invokes Runner a second time

Scenario: expired message-scoped reply target is not replaced by another target
  Given a Reply targets an expired inbound message target envelope
  When the Sender resolves the target
  Then the send is classified as a permanent or configured retryable failure
  And no other Identity or Conversation target is selected

Scenario: multipart reply preserves order
  Given a final text exceeds the Provider limit
  When Reply Projection splits it
  Then part_no values are consecutive starting at 1
  And the Sender does not claim or send part N+1 before part N is SENT
  And later parts remain PENDING when a previous part is retryable or permanently failed

Scenario: unsupported card falls back to text
  Given the platform Reply contains a card unsupported by the target Adapter
  When the Adapter builds the provider request
  Then it sends the specified text fallback or download link
  And it does not expose internal event payload
~~~

### Feature: IM-07 attachment and recall boundary

~~~gherkin
Scenario: supported inbound file enters the Runner boundary through ArtifactRef
  Given a verified callback contains a Provider-supported file
  When IM-07 handles the message
  Then the file is downloaded and passed through Artifact validation
  And the Execution input contains the scoped ArtifactRef
  And raw binary is not stored in Session events
  And Runner receives only the validated artifact content reference

Scenario: unsupported outbound attachment uses explicit fallback
  Given a platform Reply contains an attachment or card unsupported by the target Provider
  When IM-07 outbound handling prepares the reply
  Then it records the unsupported capability
  And it sends the configured text or download-link fallback when a reply is required
  And it does not silently drop the message

Scenario: pending execution is cancelled after verified recall
  Given a recalled message maps to a PENDING Execution
  When the recall event is admitted
  Then the Execution is prevented from starting
  And the original Inbox remains for audit

Scenario: running execution receives a cancellation request
  Given a recalled message maps to a RUNNING Execution
  When the recall event is admitted
  Then runner.ManagedRunner.Cancel(request_id) is requested
  And the Runner event channel is drained to close
  And external Tool side effects are not assumed to be reversed

Scenario: completed execution is not deleted by recall
  Given a recalled message maps to a completed Execution
  When the recall event is handled
  Then Session, Memory, Tool results and sent Replies are retained
  And an audit record is added

Scenario: duplicate recall event is idempotent
  Given a recall event with external_event_id "recall-1" has been applied
  When the same recall event is delivered again
  Then channel_recall_inbox count remains 1
  And no second cancellation request or audit event is created
~~~

## 17. Fake Provider QA Plan

### 17.1 Test topology

IM-01、IM-04、IM-05 的基础验收先使用 test trusted-scope input，验证可信路由、
身份映射和 commit-before-ACK 事务，不依赖任何 Provider 协议解析：

~~~text
test trusted-scope Channel Input
  -> IM-01 public route / Binding trusted scope
  -> IM-04 Identity / Conversation
  -> Gateway
  -> PostgreSQL channel_inbox
  -> PostgreSQL Execution
  -> PostgreSQL Dispatch Outbox
  -> COMMIT
  -> fake HTTP ACK
~~~

这条基础链路直接调用 IM-01 的 route resolver 和 Gateway/Store contract，不注册
生产 HTTP handler。Provider HTTP method/path、Content-Type、body limit 和 route
fingerprint 日志在 IM-02/IM-03 的 fake callback 入口测试中验证；这样 IM-01 的
test fixture 不会成为生产旁路。

Provider 协议和完整出站链路再分别验证：

~~~text
fake WeCom callback
  -> IM-01 public route / Binding trusted scope
  -> WeCom Adapter verification / normalization
  -> IM-07 inbound AttachmentIngestor (media ref -> ArtifactRef)
  -> Gateway
  -> PostgreSQL channel_inbox
  -> PostgreSQL Execution
  -> PostgreSQL Dispatch Outbox
  -> Relay / Worker
  -> fake Runner
  -> Reply Projection
  -> PostgreSQL Reply Outbox
  -> Reply Sender
  -> fake WeCom send endpoint
~~~

Feishu 使用完全相同的平台测试拓扑，只替换：

- fake callback envelope；
- verification/decryption fixture；
- user/chat/message field extractor；
- fake send API 和 Provider error fixtures；错误分类由 IM-06 验证。

其中 IM-02/IM-03 的 fake outbound client 只验证 codec/client contract；Reply
Outbox 的 Claim、Lease、错误分类和 retry loop 统一在 IM-06 测试。

不使用真实 Provider token、模型 key 或数据库凭据进入仓库。Fake server 的
secret 只在测试进程内生成。

### 17.2 Fixtures

每个测试建立独立的：

- tenant；
- agent app 和 published config；
- ACTIVE channel_binding；
- public_route_id；
- provider secret fixture；
- sender/chat/message fixture；
- fake Runner；
- fake Provider send endpoint。

测试必须显式创建两组不同 tenant、两组不同 Binding，并复用相同的外部 user
ID/chat ID，验证 scope 隔离。

### 17.3 必须观测的计数和关联

| Observable | 断言 |
| --- | --- |
| request_id | 首次和重复 webhook 的 request_id 一致 |
| execution count | 相同 external_message_id 为 1 |
| inbox count | 相同 scope/key 为 1 |
| dispatch count | 相同 request 只有 1 条有效 Dispatch |
| runner invocation count | 重复 webhook 为 1；Reply retry 不增加 |
| reply_outbox count | 一个 source event 的 operation/revision 只有预期行，Projection 重放不增加 |
| reply_id | 同一 source event 重放复用原 reply_id |
| provider send count | 正常成功为 1；故障重试符合 attempt；若 Provider 无幂等则验证平台不重复执行 |
| tenant/app/binding scope | 所有查询和回复 target 与原 Binding 一致 |

### 17.4 QA 用例组

1. test trusted route and Binding admission；
2. cross-tenant/cross-binding Identity、Conversation、Session mapping；
3. first Inbox、Execution、Dispatch transaction；
4. sequential/concurrent duplicate and same ID different payload；
5. DB rollback、lost ACK and Binding disable/rotation race；
6. WeCom URL verification、signature、decryption、external_account and text；
7. Feishu challenge、token/signature/encryption and text；
8. unknown/disabled/mismatched route；Provider HTTP method/path、Content-Type、body
   limit 和 route fingerprint 由 IM-02/03 入口覆盖；
9. malformed、oversize、empty payload and adapter cancellation；
10. Worker Runner event drain and completion；
11. pre-admission media ingest success、可重试失败和永久不支持；
12. final-only Reply and streaming update capability；
13. multipart boundary and ordering；
14. send timeout、rate limit、permanent failure；
15. duplicate Reply Outbox delivery/lease recovery；
16. source event replay does not create a second Reply Outbox row；
17. out-of-order execution_event and projection cursor；
18. verified unsupported inbound type or permanent attachment rejection -> REJECTED + audit without Execution；
19. image/file/card outbound fallback；
20. pending/running/completed recall；
21. logs/traces do not contain secrets, raw body, public route or full Tool arguments。

### 17.5 端到端成功判定

对同一条重复 webhook，必须同时证明：

    Execution = 1
    Runner invocation = 1

对 Reply Sender 重试，必须证明：

    Runner invocation 不增加
    Reply Outbox attempt 增加
    最终发送状态可观察

### 17.6 实现与验收门禁

每个 IM 子任务在进入下一个串行任务前必须同时通过：

- 目标包定向单元测试；
- 涉及并发 Inbox/Identity/Reply 的测试使用 `go test -race`；
- 涉及 PostgreSQL 事务、唯一约束或 Claim/Lease 的测试使用仓库现有的
  PostgreSQL 集成测试方式；没有真实 Provider 凭据时使用 fake protocol/server；
- Gherkin 场景对应到可执行测试或明确的集成测试用例；
- 代码审查确认没有 Adapter -> Runner 直连、payload tenant 信任、跨租户查询或
  原始 Provider 字段越界。

审查 Agent 的独立清理项必须与必须修复项分栏；只有当清理项实际涉及安全、数据
一致性、兼容性破坏或当前 Done criteria 时，才会阻塞 QA 门禁。QA 只验证主线程
修复后的工作树，不因已明确延期的清理项判定当前任务失败。

最终 IM-01～IM-07 全部完成后，必须在仓库根目录执行并通过：

~~~text
go build ./...
go test ./...
go test -race ./...
go vet ./...
golangci-lint run --timeout=10m
gofmt -r 'interface{} -> any' -l .
goimports -l .
~~~

`gofmt`/`goimports` 命令只用于检查时不应产生未提交改动；若输出文件名，
编码 Agent 必须修复后重新执行。外部 Provider 的真实凭据只用于人工联调，不能
成为自动化测试或 CI 的前置条件。

## 18. Security Invariants

1. tenant_id/app_id 永远由 verified Binding 得到，不信任外部 payload。
2. route_key 不包含可推断租户信息；路由命中不等于授权。
3. URL channel、Binding.Channel、external_account 必须一致。
4. secret 只能通过 Binding 的 TokenRef/SigningSecretRef/SecretRef 解析；原值
   不进日志、trace、错误报告或测试输出。
5. Admission transaction 必须按 tenant/app/binding scope 查询和锁定，并比较
   public_route_id、binding_revision 及授权属性快照。
6. Identity/Conversation 的外部 ID 使用 binding-scoped HMAC hash；回复所需
   target 只能使用 6.2.1 的 AES-256-GCM envelope 和内部 target_ref。
7. Inbox 和 Execution 的幂等键包含 tenant/app/binding；不能使用单独外部 ID。
8. raw provider XML/JSON、签名、加密 envelope 不进入 Gateway、Worker、Runner
   或 Session。
9. 回复 target 必须属于原 Binding；禁止仅凭用户输入的 target 发送。
10. 不记录完整 PII、模型 API key、IM token、数据库凭据或完整 Tool 参数。
11. HTTP ACK 只能在事务 commit 成功之后返回成功；ACKED 不作为虚假数据库状态。
12. Runner 取消后仍然排空 Event Channel，防止 goroutine 泄漏和事件丢失。
13. Reply Outbox 重试不重新创建 Execution，不重新调用 Runner。
14. 已验签但不支持的入站类型或永久不支持的媒体只能写 REJECTED +
    UNSUPPORTED_MESSAGE_TYPE/ATTACHMENT_REJECTED + 脱敏审计并在 commit 后 ACK；
    不创建 Execution；不支持的出站卡片/附件才可以按 capability 显式降级。
15. test trusted input 只能存在于测试 wiring，不能有生产 HTTP/API
    或客户端可构造的入口。
16. TargetProtector 必须校验 purpose、key_version、AAD、算法、nonce 和密钥
    长度；任何不匹配都 fail closed。
17. 消息级 Provider reply target 只能从原消息 Inbox 的密文 envelope 解析，并
    校验 tenant/app/binding、request_id 和有效期；不得回退到其他主体 target。
18. 撤回事件必须使用 tenant/app/binding/external_event_id 幂等，取消请求和审计
    不能因 Provider 重复投递而重复执行。
19. 预入站媒体的可重试失败不创建 Inbox/Execution，不返回成功 ACK；永久不支持
    媒体才允许写 `ATTACHMENT_REJECTED` 并在提交后 ACK。

## 19. Non-goals

本需求实现：

- IM Binding admission；
- public route 定位和 trusted tenant source；
- Identity/Conversation/Session mapping；
- Inbox 幂等；
- Execution/Dispatch integration；
- Reply Domain/Reply Outbox；
- IM 有限发送重试；
- provider-specific limits；
- IM cancellation request；
- attachment protocol boundary。

本需求不实现：

- 通用 Tool Permission / Approval；
- 全平台 Observability 体系；
- 全局 Worker failover；
- Lease takeover framework；
- 全局容量治理；
- 完整灾难恢复；
- 通用节点故障恢复；
- 所有 Provider 的 exactly-once 发送保证。

如果 Reply Outbox 需要 claim/retry lease，只实现本表对应的 IM 局部能力，
不得扩张为通用平台恢复系统。

## 20. Open Questions

这里只保留不能从当前仓库和需求直接推导、需要外部或产品决策的问题：

1. 目标企业微信 AI Bot 和飞书 App 的实际凭据、可见范围、媒体/卡片权限、
   Provider 配额和错误码，需要在部署账号中填写和核验；它们不能由仓库代码
   推导，也不改变本文已经冻结的产品、协议和平台契约。
2. 如果目标企业微信账号未开通 AI Bot API 模式，必须先完成 Provider 侧开通；
   不能把普通自建应用临时替换进来，因为它不满足群聊入站要求。

Provider target 持久化不再是 Open Question：当前仓库没有可复用的可逆加密、
KMS/Vault 或受控外部引用能力，已冻结为 6.2.1 的 SecretProvider + AES-256-GCM
密文 envelope + 内部 target_ref；它是 IM-04 的编码前置条件。

route key 是否全局唯一、是否信任 payload tenant、是否需要独立 Reply Outbox、
是否在事务提交后 ACK、是否保留现有 exact ResolveBinding，均已由本 Spec 基于
当前 Repo Facts 做出决定，不再作为 Open Question。

## 21. Final Consistency Freeze

本节及第 1～20 节是本 Spec 的唯一规范正文。第 22、23 节仅保留为历史审查和
进度附录，不得覆盖正文的范围、状态、DAG、验收或 Done criteria。

本次最终修订同步了以下 sections：

- 1、2.3、3.1、3.3、3.6：冻结 IM-01、IM-02/03 和 IM-06 的职责边界，以及
  企业微信 AI Bot、飞书官方 Go SDK 的产品/SDK 选择；
- 3.4、3.4.1、3.5、13.0、15.2：补充 tRPC-Agent-Go/OpenClaw 的参考、不复用
  边界和 Go 编码风格约束，冻结公共契约、推荐目录、任务 ownership、交接物和
  单次出站调用语义；
- 4.1、4.3、5.1、5.3、5.4、5.5：冻结标准输入、完整附件链路、route、
  trusted admission、binding_revision 和 Provider 验证位置；
- 6.2、6.2.1、14.2、15.2：冻结 group 非 NULL 唯一键、同事务映射和 IM-04
  Provider target 密文方案及窄接口；
- 7、8：冻结 Inbox 的 REJECTED 语义、映射事务边界、Reply contract 前置和
  multipart 严格顺序；
- 9.1、9.3、10.1、10.2、10.3、10.4、11.3：移除 Adapter 的完整出站
  Sender/retry/lease 职责，改为 IM-06 统一处理；
- 12、13：冻结逻辑 DAG、串行开发顺序和各任务 Inputs/Tests/Done criteria；
- 16、17、18：将 Provider 协议测试归入 IM-02/03，并补齐 test trusted-scope
  admission、路由轮换、日志和全链路计数验收。

前一轮审查提出的 P1/P2 已分别落到契约或实现计划中。当前代码缺少表、接口或
需要调整行为，属于第 2.3 节 Repo Facts 的 Required Change，不改变本 Spec 的
最终功能范围；历史审查记录不构成新的未决架构问题。

本轮结合 OpenClaw 参考实现后，新增的实现约束也已冻结：公开框架能力只用于
字段分层、Channel capability、factory 注册和 Provider 内部模块拆分；Webhook
不强制后台 `Run`，不导入 `openclaw/internal/...`，不引入第二套 Gateway；
Provider outbound 只提供一次 `SendOnce` 操作，操作可以是 SEND、UPDATE 或
FINALIZE，Reply Outbox 的 Claim/Lease/Retry 和错误状态转换只属于 IM-06。

本轮结合企业微信、飞书 Provider 调研后，新增约束也已冻结：Provider 选择固定为
企业微信和飞书；企微使用本 Spec 已选定的 AI Bot HTTP URL 回调，飞书使用官方
`oapi-sdk-go/v3`（参考 v3.11.0）事件/typed API；Provider Adapter 只提取媒体
引用和消息级 target 句柄，预入站媒体处理由 `AttachmentIngestor` 完成；IM-02/03
只提供一次出站 codec/client 操作，Reply Sender lifecycle 和重试仍只属于 IM-06。

本轮需求范围和附件术语也已冻结：原始需求只按“至少两类 IM、至少包含微信或企业
微信、图片/文件消息及相关平台限制”追溯；平台层将图片、文档及 Provider 其他非文本
媒体统一归入附件/文件，复用同一 ArtifactRef 链路，不新增按媒体种类拆分的 Gateway、
Session 或 Runner 类型，也不把未被原始需求单独列出的媒体扩成独立验收项。

最终逻辑依赖 DAG：

    IM-01 -> IM-04 -> IM-05
                         | \
                         v  v
                       IM-02 IM-03
                         \  /
                          v
                        IM-06 -> IM-07

IM-02 与 IM-03 逻辑上可以并行；在一次只开发一个子任务的约束下，最终串行
顺序固定为：

    IM-01 -> IM-04 -> IM-05 -> IM-02 -> IM-03 -> IM-06 -> IM-07

最终运行时链路不是交付依赖顺序：

    IM-02/IM-03 Adapter
        -> VerifiedProviderEnvelope
        -> IM-07 inbound AttachmentIngestor
        -> IM-05 Inbox/Execution/Dispatch admission
        -> Worker/Runner/Event
        -> IM-06 Reply Projection/Reply Outbox/Sender
        -> IM-07 outbound attachment/recall/limit handling

IM-05 先冻结 `AttachmentIngestor.Prepare` 的调用点、ArtifactRef 输入契约和失败
语义；IM-07 在串行交付最后实现入站和出站两部分。因此媒体处理不会晚于
Execution 创建，但 IM-07 也不需要在 IM-05 之前交付。

IM-01 的架构决策已闭合：route_key 定位、Channel/ACTIVE 校验、trusted
tenant/app/binding scope、channel binding admission、Gateway 入口、
PostgreSQL transaction revalidation、binding_revision、stale route、disable
race、日志边界和 test trusted-scope 输入均已定义。Provider signature、
decrypt、Verification Token 和 external_account 不属于 IM-01，Provider 细节
也不构成 IM-01 blocker。test trusted-scope 输入仅存在于测试 wiring，不能成为生产
入口。

IM-01 的架构决策已闭合，可以开始编码：Provider signature、decrypt、
Verification Token 和 external_account 不属于 IM-01；test trusted-scope 输入仅存在于
测试 wiring，不能成为生产入口。IM-01～IM-07 的最终验收包括正文和 Gherkin 中
冻结的文本、附件、卡片/流式、异步回复、分片、限流、失败重试和撤回边界。

## 22. 复审历史附录（非规范，2026-08-31）

本节记录历史审查过程，便于追溯，不是新的需求、冲突或 Open Question。若本节
与第 1～21 节有差异，以规范正文为准；下列 R/C 项不要求再次决策。

历史审查当时使用的分类规则如下：

- **改造项（REFACTOR）**：当前代码缺少能力、接口或表，或者现有行为需要调整，
  但不改变本需求和 Provider 协议的可实现性；记录为实现任务，不算规格冲突。
- **真正冲突（CONFLICT）**：本文已经冻结的两个约束无法同时成立，必须先改本文
  的流程或产品决策，不能只靠补代码解决。

### 22.1 改造项（不算真正冲突）

| ID | 改造内容 | 归属/验收 |
| --- | --- | --- |
| R-01 | 将 unresolved `ChannelInput` 映射改造成事务内 admission command：允许 Gateway 先传标准化外部键，由 IM-05 在同一 PostgreSQL 事务内映射并构造最终 RuntimeContext | IM-04/IM-05；映射、Inbox、Execution、Dispatch 同事务提交 |
| R-02 | 增加 `ChannelAdmissionMetadata`，承载 `message_type`、`provider_timestamp`、payload hash 和 reject reason；增加已验签 unsupported 分支，不让通用文本校验挡住 REJECTED 入库 | IM-05；REJECTED 有审计、无 Execution、commit 后 ACK |
| R-03 | 区分 `ProviderMediaRef` 与平台 `ArtifactRef`；媒体下载/解密/Artifact ingest 放在 Admission 前，出站附件处理放在 Reply Projection 后 | IM-07；失败不创建 Execution，二进制不进 Session/Event |
| R-04 | 为 WeCom `response_url` 增加按消息绑定、加密、带过期时间的 reply target，不能复用稳定用户/群 target | IM-02/IM-04/IM-06；不进日志、Session 或普通 Gateway 字段 |
| R-05 | 为 WeCom streaming 增加 provider-specific stream context、初始响应、刷新回调、结束/超时和幂等关联；普通 Provider 仍可只实现 `SendOnce` | IM-02/IM-06；流式与最终结果不能重复执行 Runner |
| R-06 | 扩展 `TargetProtector` 的 `Seal/Open` 参数，显式接收构造 AAD 所需的 Binding、Channel、EntityType 和内部 ID | IM-04；错误 scope/AAD/版本/算法必须 fail closed |
| R-07 | 增加 Binding 授权属性的原子更新/轮换 API，并让 Reply Outbox 保存并校验 binding revision/target generation | IM-01/IM-06；旧路由失效，旧目标不会被新配置误发 |
| R-08 | 将 Feishu `app_id + tenant_key` 定义成可校验的外部账号范围，而不是只依赖一个未结构化的 `ExternalAccount` 字符串 | IM-03；事件头账号必须与 Binding 一致 |
| R-09 | 分离“Provider 重复投递”和“业务重试”：明确 FAILED Execution 是否只能通过显式 retry command 重跑 | IM-05；重复 webhook 不产生第二次 Runner 执行 |
| R-10 | 为 Reply Projection 增加源事件唯一键、游标或幂等状态，并定义 provider timestamp 的乱序/过期策略 | IM-05/IM-06；多 Worker 重放不重复创建 Reply |

以上项目均属于代码、Schema 或平台契约改造。现有 `Store.Admit` 拒绝
`verified_channel_binding`、`Message.Validate` 拒绝 ArtifactRef、缺少 Inbox/Reply
Outbox/Adapter 等，均不再单列为规格冲突。

### 22.2 已关闭的历史冲突

| ID | 冲突 | 冲突原因 | 必须修改 |
| --- | --- | --- | --- |
| C-01（已关闭） | IM-07 在交付 DAG 中位于 IM-06 之后，但媒体处理要求发生在 Admission 前 | 入站和出站媒体处理属于同一 IM-07 任务的两个运行时位置；IM-05 先冻结 `AttachmentIngestor.Prepare`，IM-07 最后实现它 | 第 4.3、7.2、12.1、13 节已明确 pre-admission hook 和 post-projection 出站边界 |

C-01 已通过拆分附件阶段关闭，不是当前代码能力不足。

### 22.3 已关闭的历史条件项

WeCom HTTP AI Bot 的流式协议需要初始响应、后续刷新和结束操作。现已由正文
冻结 `SEND`、`UPDATE`、`FINALIZE` 三种平台操作；每次 Provider 调用仍是一次
`SendOnce`，而 Claim、Lease、节流、错误分类和 Retry 由 IM-06 管理。因此不再把
流式标记为后续待决项。

### 22.4 复审状态

- 确定真冲突：无；原 `C-01` 已通过 pre-admission/post-projection 拆分关闭。
- 后续条件性冲突：无；流式操作已纳入 Reply contract。
- 改造项：`R-01`～`R-10`，全部记录为实现改造，不作为需求否定依据。
- 历史状态：已完成 Spec 修订；实现进度不由本附录定义。

## 23. 历史实现进度快照（非规范）

下表只是生成本文时的工作树快照，不是功能范围、验收范围或任务状态的定义。
实现 Agent 必须以第 13、16、17 节为准；本快照中的阶段性状态不代表最终需求被
删除。

| 项目 | 状态 | 本轮验收 |
| --- | --- | --- |
| IM-01 route/Binding/trusted scope | TODO | route 定位、ACTIVE、channel 匹配、事务重校验 |
| IM-04 Identity/Conversation/Session | TODO | 单聊/群聊隔离、并发首次映射、稳定内部 ID |
| IM-05 Inbox/Execution/Dispatch | TODO | 重复投递、hash 冲突、原子提交、commit 后 ACK |
| IM-02 WeCom 文本入站 | TODO | 验签/解密、账号匹配、标准化、异步 ACK |
| IM-03 Feishu 文本入站 | TODO | challenge/签名校验、账号匹配、标准化、异步 ACK |
| IM-06 最终文本回复 | TODO | Runner 完成后入 Reply Outbox、发送重试不重跑 Runner |
| IM-07-inbound/outbound 附件 | NOT STARTED (snapshot) | 最终按第 11、12、13、16、17 节实现 |
| WeCom streaming/card/recall | NOT STARTED (snapshot) | 最终按第 8、9、11、16、17 节实现 |
