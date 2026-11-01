# 部署与运行

先安装两个子系统到干净Python3.11环境，再用显式路径配置数据库、UDS、宿主实例、outbox、凭据引用和可用模型地址。不要让缺少参数的测试默认读取真实HOME。examples/runtime.example.json只演示结构，CHANGE_ME必须替换；它不是可直接启用的生产配置。

scripts/operations_trial.py是当前已验证的外部调度器，参数--config <显式配置路径>。不自带定时器：按平台配置launchd/cron。一次性任务等待时not running正常，互斥锁防重叠。新工作优先、并发2，12提炼/256向量每轮；冷历史初始暂停。history_backfill.storage=kw_jobs，run_after=9999仅暂缓标记，非业务时间。维护人员依据提炼质量/用量决定各阶段，不能重置成功任务。摘要与原生来源重叠时先补缺核对。

scripts/bge_server.py参数--model-dir <本地模型目录> --port <端口>，模型环境按scripts/embedding-lock-macos13-x86_64-py311.txt安装。固定版本5617a9f61b028005a4858fdac845db406aefb181，CPU2+1线程、1024维/512token。模型权重需另行获取，不在Git。

scripts/weekly_backup.py参数--db <DB> --local-dir <备份目录> --keep 4。外部定时每周/自定义周期，验证成功后再轮转。先只用本地备份；NAS需单独配置与验证。逻辑备份不含向量，可按有效revision本地重建，不调用语义LLM重提炼。恢复到新路径，核对来源时钟/版本/暂缓作业/首次提炼保护，再对账outbox。完整迁移另需宿主配置、outbox、控制面、原生源和凭据引用；这些私有材料不要上传GitHub。

清理先用memorelay-core cleanup --db <DB> --dry-run。试运行未开启实际删除。回退先停后台、备份新增库/outbox与原配置，再切回原宿主配置，不用部署前库覆盖试运行新数据。

日志只保存脱敏元数据，勿直接分享原始SDK日志/服务环境。凭据仅引用，轮换后检查引用并重载宿主。STALE_CURSOR仅在旧revision已失效且代际守卫确实拦下时视作正常作废，其他失败应处理；不要清失败表掩盖问题。
