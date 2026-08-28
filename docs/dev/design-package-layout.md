# 最终包结构

本文记录完整需求的代码包边界。仅含 `doc.go` 的目录只声明能力归属，不表示对应后端或 IM 接入已经启用。

## 目录

```text
cmd/
  trpc-service/                 进程入口

internal/
  execution/                    持久化 Job 私有模型

trpcservice/
  version.go                    服务版本
  admin/                        控制面管理 API
  agent/                        Agent 领域扩展点
  artifact/                     Artifact Provider 归属
  auth/                         入站凭据与可信身份解析
  channels/                     Channel Binding 领域模型
    feishu/                     飞书 Channel Adapter 归属
    wecom/                      企业微信 Channel Adapter 归属
  config/                       AppConfig 解析
  gateway/                      认证后原子准入与 QueuedRunner
  ingress/                      HTTP/RPC 协议入口
  knowledge/                    Knowledge Provider 归属
  log/                          日志配置与脱敏边界
  memory/                       Memory Provider 归属
  metrics/                      指标定义与采集
  postgres/                     平台 PostgreSQL 适配实现
    migrations/                 平台协调库迁移
  queue/                        消息投递与 Execution 租约契约
  redis/                        Redis Stream 与 Session Lease 适配实现
  relay/                        Outbox 到消息流的可靠发布
  runtime/                      Model、Tool、Session 到真实 Runner 的装配
  secret/                       带作用域 SecretRef 解析
  session/                      Session Provider 父包
    postgres/                   tRPC-Agent-Go PostgreSQL Session 装配
    mysql/                      MySQL Session Provider 归属
    redis/                      Redis Session Provider 归属
  skill/                        Skill 领域扩展点
  storage/                      租户后端配置与作用域校验
  tenant/                       Tenant、Agent App、配置领域模型
  tool/                         工具策略与执行校验
  web/                          Web 能力扩展点
  worker/                       消费、Claim、执行、重试和确认
  workspace/                    Workspace 领域扩展点
```

## 包职责

| 包 | 职责 |
| --- | --- |
| `cmd/trpc-service` | 读取部署配置，组装 Gateway、Relay、Worker、共享后端并管理关闭顺序。 |
| `internal/execution` | Gateway 与 Worker 之间的私有、已验证 Job 表示；外部调用方不能绕过准入构造任务。 |
| `admin` | Tenant、Agent App、配置版本、Credential 和 Channel Binding 的受保护管理接口。 |
| `auth` | 验证入站凭据，解析 Tenant/App 与可信运行时身份。API Key 请求使用 Credential 服务主体。 |
| `gateway` | 复核可信身份、执行原子准入、幂等和顺序分配；`QueuedRunner` 将协议 Run 转为持久化 Execution。 |
| `ingress` | 复用 tRPC-Agent-Go 协议 Server，完成 HTTP/RPC 请求校验、认证上下文注入和协议响应。 |
| `queue` | Redis Stream 之上的投递、消费、租约和确认所需的技术无关契约。 |
| `relay` | 将 PostgreSQL Dispatch Outbox 发布到消息流，不参与 Agent 执行。 |
| `worker` | 消费消息、条件 Claim Execution、持有 Session Lease、调用 Runner、写执行事件并处理重试/确认。 |
| `runtime` | 依据固定配置版本装配 Model、Tool、Session Service 和真实 tRPC-Agent-Go Runner。 |
| `secret` | 校验 Tenant/App 作用域后解析 `SecretRef`；不保存或记录密钥原文。 |
| `storage` | 把 App 的 `BackendConfig` 解析为带 Tenant/App 作用域的能力句柄；不定义跨后端万能存储接口。 |
| `session` | Session Provider 的归属边界，直接复用 tRPC-Agent-Go `session.Service`。 |
| `session/postgres` | 按已选后端的 DSN 与 Schema 准备并缓存 tRPC-Agent-Go PostgreSQL Session Service；框架 Session 表不由平台迁移管理。 |
| `session/mysql`、`session/redis` | MySQL、Redis Session Provider 的归属边界。 |
| `memory`、`knowledge`、`artifact` | 分别承载 Memory、Knowledge、Artifact Provider 的装配边界。 |
| `postgres` | 平台协调数据的 PostgreSQL 实现：控制面、Session Lane、Execution、Dispatch Outbox、Execution Event、Audit 与迁移执行；不承载租户可选业务数据 Provider 或其 Schema。 |
| `redis` | Redis Client、Stream Consumer Group 与 Session Lease 的适配实现；不承担平台控制面权威状态。 |
| `channels` | Channel Binding 模型和 IM Adapter 的父目录。 |
| `channels/wecom`、`channels/feishu` | 企业微信、飞书协议差异的归属边界，包括验签、入站标准化与回复发送。 |
| `tenant` | Tenant、Agent App、AppConfig、BackendConfig、ToolPolicy、AuditPolicy 等领域值对象与校验。 |
| `tool` | 工具可见性与执行前授权校验。 |
| `config` | 固定配置版本的读取与解析。 |
| `log`、`metrics` | 观测数据的字段约束、日志脱敏和指标定义。 |
| `agent`、`skill`、`workspace`、`web` | 面向 tRPC-Agent-Go 对应能力的领域扩展边界。 |

## 平台状态与可选数据后端

`postgres` 保存平台权威协调状态：Tenant、App、Credential、Binding、Session Lane、Execution、Outbox、Execution Event 和 Audit。其生产文件按数据聚合固定为：

```text
trpcservice/postgres/
  store.go       控制面与 Channel Binding
  execution.go   准入、Session Lane、Execution、Outbox、恢复与重试
  journal.go     Execution Event 与 Audit
  migrate.go     平台迁移执行
  migrations/    平台 Schema DDL
```

Session、Memory、Knowledge 和 Artifact 是租户按 `BackendConfig` 选择的业务数据能力。其具体 Provider 位于各自能力包，不能因为当前默认使用 PostgreSQL 就并入平台协调库。

## 依赖方向

```text
ingress / channels -> auth -> gateway -> postgres -> relay -> redis -> worker
worker -> runtime -> session|storage|secret|tool
admin -> postgres
cmd/trpc-service -> 全部具体实现并负责组装
```

`gateway` 不依赖 Worker；`worker` 不依赖协议入口；`postgres` 不依赖具体 Session、Memory、Knowledge 或 Artifact Provider。这样任意 Worker 可处理任意已准入 Session，协议和后端实现不会反向耦合到平台协调层。
