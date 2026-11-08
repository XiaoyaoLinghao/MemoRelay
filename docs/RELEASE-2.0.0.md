# MemoRelay 2.0.0

承忆首个正式版本，包含忆桥 memorelay-bridge 2.0.0 与知枢 memorelay-core 2.0.0。面向已配置的本地记忆服务日常运行；不代表所有宿主和自然业务场景已经验收。

## 本次合入

- 新对话与后台提炼采用公平队列；长来源无损分段、完整覆盖校验、断点缓存和续租，成功版本不重复消费模型调用。
- 精确来源片段回填和逐页校验，拒收时保留诊断；事实、身份、秘密与证据守卫保持有效。
- 分离失败原记录、有效修复关联与人工审核过的原生副本处置；保留原文与尝试，不把拒收记作零写入成功。
- Hermes 保留 message_uid 的压缩副本按完整负载及压缩血缘去重，沿用原事件时间；正文变化、分叉、重置及无UID回退仍分别处理。
- 导出快照 schema5，兼容导入旧schema1–4；提炼默认prompt10，仍支持旧处理器。升级后不得用不认识新schema的旧程序写当前库。
- 新命令 memorelay-core / memorelay-bridge；主动查询 memorelay_search，召回状态显示 MemoRelay（承忆）。旧命令与 unified_memory_search 兼容，配置ID和数据身份不机械重命名。

## 升级

在新的venv安装两个已固定版本的wheel，禁止与旧distribution混装；保留现有数据库、outbox、actor/client/instance、项目授权及模型配置。先做一致备份和隔离导入，再在宿主空闲时切换加载路径、重启宿主并核验工具schema/就绪/投递。不要重导历史或清零失败记录。

Core使用Python3.11+；Hermes宿主若为Python3.14，Bridge在对应独立环境安装。DSH需更新实际加载的JS工具插件和Python采集入口；只更新后台Core不能自动改变宿主显示名称。存在定制资源范围解析时保留原解析器，不能用通用插件覆盖定制授权。

内部兼容ID unified-memory-v2 / unified-memory、Python模块 dma / knowledge_weaver、既有socket、服务名和数据目录继续保留。不同时启用新旧两份采集插件。旧knowledge-weaver命令仍是可选的v1 MCP入口，不能当作新的Core命令。

## 验证边界

安装回归与发布结果见 publication-validation.json 和 docs/VALIDATION.md。真实消息不会为了验证而代发。自然复杂研究/写作、全部角色跨会话语义质量、严格压缩hook执行回执仍按实际覆盖记录；暂停的冷历史和待来源补缺摘要不会因发版自动放行。源码发布不包含私人运行数据。
