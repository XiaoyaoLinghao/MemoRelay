# 命名与兼容

总项目承忆/MemoRelay；DMA→忆桥/MemoRelay-Bridge；KW→知枢/MemoRelay-Core。新Python distribution分别memorelay-bridge、memorelay-core，版本2.0.0rc1；新CLI同名。

Python模块dma、knowledge_weaver，已有CLI dma-memory/kw-memory，以及旧v1的knowledge-weaver MCP命令保持语义兼容。数据表、契约ID、socket协议和已有环境变量不机械替换；旧文档/迁移历史里的DMA/KW保留作为血缘。不要在同一venv混装新旧两个提供同名模块的distribution。

原仓库与其分支/历史保持不变。新仓库导入当前已审核源码树，SOURCE_PROVENANCE.json记录原提交和每文件hash；这是快照迁移，不宣称迁入全部Git历史。主仓库通过submodule锁定新仓库提交。

正在试运行的旧release不被这次GitHub上传自动替换。未来部署改名包时应新建venv/release，在隔离恢复/宿主探针通过后切换；复用原数据库/原生来源及outbox，对账后接续，不全量重提炼或强制改数据ID。
