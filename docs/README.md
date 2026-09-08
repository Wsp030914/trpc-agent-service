# trpc-agent-service 文档

这套文档描述当前工作区中的真实实现，而不是目标架构或历史设计。实现基线包括 Go 源码、PostgreSQL migrations、测试、Compose/Kubernetes 部署文件、GitHub Actions、Admin UI 与脚本；本次状态还纳入了已完成的真实 Provider、真实部署、故障恢复和容量外部实测。文档只记录当前代码契约与实测结论，不虚构外部报告中没有提供的数值。

## 阅读顺序

1. [需求追踪](requirements-traceability.md)：先看 README 的每项要求落在哪里，以及当前实现边界。
2. [架构设计](architecture-design.md)：理解设计目标、权威状态、隔离、调度和故障语义。
3. [系统架构](system-architecture.md) 与 [系统架构图](diagrams/system-architecture.svg)：查看组件和部署边界。
4. [核心时序](core-sequence.md) 与 [核心时序图](diagrams/core-sequence.svg)：沿一条 IM 消息链路理解上下文和状态变化。
5. [数据模型](data-model.md) 与 [数据同步和幂等](data-sync-idempotency.md)：理解表、Session 后端、Outbox、租约、迁移和 uncertain 语义。
6. [后端适配](backend-adaptation.md)：核对当前实际可用的 Session、Memory、Knowledge、Artifact、队列和模型后端。
7. [部署](deployment.md)、[容量](capacity.md)、[风险登记](risk-register.md)：用于落地和运维评审。
8. [验收矩阵](acceptance.md)：最终查看 Requirement、证据、限制和状态。

## 文档入口索引

| 文档 | 负责回答的问题 |
| --- | --- |
| [requirements-traceability.md](requirements-traceability.md) | 根 README 的 R1–R5、交付物和验收点是否逐项覆盖 |
| [architecture-design.md](architecture-design.md) | 为什么这样设计，以及完整端到端系统如何工作 |
| [system-architecture.md](system-architecture.md) | 哪些组件真实存在、如何部署、状态由谁持有 |
| [core-sequence.md](core-sequence.md) | IM、Gateway、Worker、Runtime、持久化和回复的关键时序 |
| [data-model.md](data-model.md) | PostgreSQL 平台实体、框架 Session 和外部数据的关系 |
| [data-sync-idempotency.md](data-sync-idempotency.md) | 原子 Admission、排序、租约、Outbox、重试、迁移和恢复 |
| [backend-adaptation.md](backend-adaptation.md) | 当前代码真正接入的各类 Backend 及其一致性取舍 |
| [risk-register.md](risk-register.md) | 生产触发条件、影响、检测、缓解和恢复 |
| [deployment.md](deployment.md) | Compose、Kubernetes、探针、密钥、发布和回滚 |
| [capacity.md](capacity.md) | 容量工具、测量口径、规划公式和容量结论 |
| [acceptance.md](acceptance.md) | 五大需求、交付物和工程能力的最终验收矩阵 |

## 图文件

- [system-architecture.mmd](diagrams/system-architecture.mmd)：系统架构 Mermaid 源图。
- [system-architecture.svg](diagrams/system-architecture.svg)：与源图保持一致的可直接查看 SVG。
- [core-sequence.mmd](diagrams/core-sequence.mmd)：核心时序 Mermaid 源图。
- [core-sequence.svg](diagrams/core-sequence.svg)：与源图保持一致的可直接查看 SVG。

## 需求、部署和验收入口

- 需求落点：[`requirements-traceability.md`](requirements-traceability.md)。
- 架构图和完整链路：[`system-architecture.md`](system-architecture.md)、[`core-sequence.md`](core-sequence.md)。
- 部署操作边界：[`deployment.md`](deployment.md)。
- 容量证据边界：[`capacity.md`](capacity.md)。
- 最终实现与验证状态：[`acceptance.md`](acceptance.md)。

## 状态约定

`IMPLEMENTED_AND_VERIFIED` 表示实现、仓库内测试和本次外部实测证据均已覆盖；`PASS` 表示验收通过；`PASS_WITH_LIMITATION` 只描述当前设计边界，不表示验证缺失；`NOT_APPLICABLE` 表示该项不是当前实现路径的适用项。本次需求条目均已闭合。
