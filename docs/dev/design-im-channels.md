# IM 软件接入详细设计

> 开发用设计草稿，不作为题目交付物。正式交付物以 `docs/deliverables/` 为准。

## 1. 设计范围

首批通道为企业微信和飞书。企业微信适配器不绑定某一种外部消息格式，而是按实际可用的官方入站与出站能力实现统一通道契约；新增通道不改变 Gateway、Worker 或 Session 语义。

本专题定义 Channel Adapter 的绑定、身份映射、入站处理和出站回复。Adapter 屏蔽企业微信与飞书的协议差异，不负责 Agent 调度、Session 持久化或租户配置发布。

| 差异 | 企业微信 | 飞书 | 平台处理 |
| --- | --- | --- | --- |
| Binding 与校验 | 配置 URL 时以 GET 校验 `msg_signature` 并解密 `echostr`；后续 POST 校验签名并解密回调。Binding 保存 CorpID、AgentID、Token 与 EncodingAESKey | 事件订阅先处理 `url_verification` 并原样返回 `challenge`；后续事件按应用的 Verification Token 及已启用的签名或加密配置校验。Binding 保存 App ID 与对应安全配置 | 已验证 Binding 才能导出 Tenant/App，并统一标准化输入 |
| 消息与会话标识 | 从解密后的回调提取成员、群聊和消息标识 | 从事件体提取发送者 `open_id`、`chat_id` 和 `message_id` | 在 Binding 作用域内映射身份和会话，并按外部消息标识去重 |
| ACK 与回复 | 平台要求在 5 秒内响应；Inbox 持久化后可返回空 `200`，再通过主动发消息接口异步回复；被动回复必须按协议加密 | URL 校验返回 `challenge`；正常事件在 Inbox 持久化后按事件订阅协议确认，通过消息 Open API 异步发送或更新回复 | Inbox/Job 提交后才 ACK；Reply Outbox 负责异步发送、重试和 DLQ |
| 限流与异常 | Adapter 处理该接口的限流、凭据失效和回复格式 | Adapter 处理该接口的限流、凭据失效和回复格式 | 不把通道规则泄漏到 Gateway、Worker 或 Session |

企业微信和飞书的具体官方接入形态由实际可用能力决定，上表不承诺某一种固定回调格式；协议细节只留在各自 Adapter 内。

## 2. 可信 Channel Binding

每个外部账号对应一个 Channel Binding，并固定关联唯一的 `tenant_id + app_id`。Binding 保存通道类型、外部账号标识、状态和有作用域的 `secret_ref`；它是 IM 入口唯一的启停与准入资源，不放入版本化 AppConfig。Token、签名密钥、机器人 Token 等敏感值不进入日志或普通配置。

入站地址可以使用不透明的 Binding 标识：

```text
/im/{channel}/{binding_id}
```

处理顺序为：

```text
binding_id 查 Binding
-> 校验 Binding 为 ACTIVE
-> 用 Binding 的 secret_ref 验签或解密
-> 校验外部账号与 Binding 的 external_account 一致
-> 从 Binding 取得 tenant_id + app_id
-> 转交 Gateway
```

`binding_id` 只用于定位配置，不是身份凭据。Tenant 和 App 只能来自验签后的 Binding，不能相信外部 payload 中的租户字段。

## 3. 身份与 Session 映射

外部 ID 先在 Binding 作用域内映射为平台内部 ID，不直接进入平台 Session 主键：

```text
binding_id + external_sender_id
-> user_id
```

单聊、群聊和话题的 Session 规则如下：

```text
单聊：user_id = 用户；session_principal_id = 用户；session_id = default
群聊：user_id = 发言人；session_principal_id = 群；session_id = default
话题：user_id = 发言人；session_principal_id = 话题；session_id = default
```

群聊和话题共享其对应的 Session 上下文；`user_id` 始终记录真实发言人，用于权限校验、审计和个人 Memory。不同 Tenant、App、Binding 的映射相互隔离。平台保存规范化或哈希后的外部标识，不依赖外部 payload 中的租户字段。

`session_id = default` 用于普通连续对话；业务存在稳定会话标识时，可以使用工单号等业务 ID 代替。

## 4. 入站消息

Channel Adapter 将各通道回调规范化为平台输入：

```text
channel, binding_id, external_message_id,
user_id, session_principal_id, session_id,
content, attachments, received_at
```

入站处理固定为：

```text
验签、解密或校验 webhook secret
-> Identity / Conversation 映射
-> Gateway 按 external_message_id 去重并持久化 Inbox、Execution、Job
-> 事务提交后 ACK 外部 IM
-> Worker 将 Job 转换为 model.Message 并调用 Runner
```

不得先 ACK 后持久化。重复回调命中已有 Inbox 时只 ACK，不再创建 Job。模型调用、Tool、附件处理和发送回复均不占用 IM 回调连接。

入站唯一键遵循数据同步专题：

```text
tenant_id + app_id + binding_id + external_message_id
```

### 4.1 消息撤回

仅当外部通道提供已验证的撤回事件时，Adapter 才按 `binding_id + external_message_id` 定位原 Inbox/Job。`PENDING` Job 取消；等待审批的 Job 取消并使审批失效；运行中的 Job 请求取消，但不承诺撤销已发生的模型、Tool 或回复副作用；已完成 Job 只追加 Audit。撤回不自动删除 Session、Memory、Tool 结果或已发送回复，避免把外部展示状态误当作可安全回滚的业务事务。

## 5. 出站回复

Worker 消费 Runner Event Channel 到关闭，只把用户可见的助手文本、卡片和 Artifact 转换为 Reply。模型内部事件、Tool 参数和错误细节不得直接发送到 IM。

默认链路为：

```text
Runner 完成 -> 同一协调库事务写 Job/Execution 终态、Reply Outbox 和必要 Audit
-> Channel Adapter -> IM
```

- 支持增量更新的通道可按节流频率更新同一条消息；不支持时只发送最终结果。
- 超过通道文本长度时按 `part_no` 顺序拆分；不支持的卡片或文件降级为文本或下载链接。
- 通道特定消息上限、富消息格式和限流规则由各 Adapter 管理，平台不硬编码具体限制。
- 临时发送失败退避重试；不可恢复失败保留失败状态，交由运维流程处理。
- 支持外部幂等键时使用 `request_id + part_no`；不支持时，网络不确定性下可能出现重复发送，不能承诺恰好一次。

所有通道默认异步发送回复，不把 Agent 执行或回复绑定在 webhook 回调连接上。终态与 Outbox 同事务创建，避免 Job 已完成但回复从未进入发送队列。

## 6. 验证边界

企业微信和飞书都必须验证一条完整链路：外部用户消息进入 Adapter、可信 Binding 映射、Inbox/Job 事务提交、协议 ACK、Worker 执行和异步回复。具体官方接入方式由实际可用能力决定，但必须满足该链路，不能只验证出站发送。

没有外部账号或公网回调条件时，Adapter 使用已验签、已解密的固定回调样本进行契约测试；真实通道凭据不进入仓库或测试日志。真实端到端验收使用绑定的外部账号和平台提供的安全回调方式。
