# 命名与兼容

总项目承忆/MemoRelay；DMA→忆桥/MemoRelay-Bridge；KW→知枢/MemoRelay-Core。新Python distribution分别memorelay-bridge、memorelay-core，版本2.0.0；新CLI同名。

Python模块dma、knowledge_weaver，已有CLI dma-memory/kw-memory，以及旧v1的knowledge-weaver MCP命令保持语义兼容。数据表、契约ID、socket协议和已有环境变量不机械替换；旧文档/迁移历史里的DMA/KW保留作为血缘。不要在同一venv混装新旧两个提供同名模块的distribution。

原仓库与其分支/历史保持不变。新仓库导入当前已审核源码树，SOURCE_PROVENANCE.json记录原提交和每文件hash；这是快照迁移，不宣称迁入全部Git历史。主仓库通过submodule锁定新仓库提交。

GitHub上传本身不改变已安装版本。部署改名包时必须新建venv/release，在隔离恢复/宿主探针通过后切换；复用原数据库/原生来源及outbox，对账后接续，不全量重提炼或强制改数据ID。

主动查询工具为memorelay_search，unified_memory_search保留兼容；显示MemoRelay（承忆）。插件稳定配置ID不变，避免同时加载两份导致重复捕获。CLI旧名kw-memory/dma-memory仍可用。
