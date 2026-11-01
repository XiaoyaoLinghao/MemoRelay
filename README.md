# 承忆 · MemoRelay

让每一次接手，都不必从头开始。New agents. Shared memory.

MemoRelay 是为多个AI智能体提供共同工作记忆的本地优先系统：无论换宿主、模型或对话，已完成事项、项目背景、用户偏好及尚未完成的工作都能带着来源继续使用。当前版本为 **2.0.0rc1，试运行候选**。

## 子系统

| 仓库 | 中文名 | 职责 |
|---|---|---|
| [MemoRelay-Bridge](https://github.com/XiaoyaoLinghao/MemoRelay-Bridge) | 忆桥，原DMA | 宿主采集、归一化、秘密保护、可靠交付和召回接入 |
| [MemoRelay-Core](https://github.com/XiaoyaoLinghao/MemoRelay-Core) | 知枢，原KW | 唯一语义提炼中心、来源/事实/版本/冲突管理与检索 |

本仓库通过两个固定提交的Git submodule组合子系统，同时提供运行脚本、跨系统测试、架构和部署说明。

## 获取与安装

    git clone --recurse-submodules https://github.com/XiaoyaoLinghao/MemoRelay.git
    cd MemoRelay
    python3.11 -m venv .venv
    .venv/bin/python -m pip install ./components/core './components/bridge[redaction-yaml]'
    .venv/bin/memorelay-core --help
    .venv/bin/memorelay-bridge --help

上面是源码安装示例，不是生产配置向导。测试/构建锁文件见components/core/requirements，BGE可选独立环境锁见scripts/。建议分别使用干净核心、测试和BGE环境，避免混装旧distribution。

## 设计与当前能力

- 面向macOS13 / Intel /16GB；SQLite、原文与事实本地存储。语义LLM可外调，BGE-M3可本地。
- 共享历史保留来源、证据等级和时间；agent角色/身份由各宿主配置决定，不从记忆继承。
- 可靠outbox、幂等投递、更正与迟到提炼、向量代际保护、导出/恢复；未完成历史明确提示processing_pending。
- 历史分阶段处理，新对话优先，成功提炼不重复；凭据只用引用，不进入记忆。
- 外部定时器负责备份，周期可调；当前部署周备份保留4代。仓库不内置复杂备份调度或督办功能。

## 文档

[架构](docs/ARCHITECTURE.md) · [命名与迁移](docs/NAMING-MIGRATION.md) · [部署运行](docs/OPERATIONS.md) · [验证范围](docs/VALIDATION.md)

真实部署已接入Hermes拾光、钧实际profile与DSH；其他科研子角色不因此被视为已独立接入。历史已脱敏入库，语义提炼与自然使用观察持续进行。不要把试运行或组件回归视作54项最终验收全部完成。

## 资料边界

只收代码、合成fixture、协议、文档及配置示例。不上传私人历史、SQLite库、模型权重、生产配置、API key或私有运行报告。原DMA/KW仓库保留；新仓库从已审查当前源码快照起步，来源提交和逐文件哈希可追溯。当前未另行授予开源许可证。
