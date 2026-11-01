# 验证范围

基线部署前/部署期间已安装回归：Core714、Bridge287+4子测试、跨系统E2E45通过；原54项为42通过/0失败/12未完整。当前命名发布另做干净wheel构建、依赖检查、版本/新旧CLI验证及同范围回归，结果记录publication-validation.json。

此处保留tests/e2e全部源码，包括不应自动运行的真实HOME集成测试，便于明确复现边界。默认隔离回归必须排除tests/e2e/test_hermes_manager_integration.py和Bridge tests/integrations/test_dsh_plugin.py；只有完成真实环境范围核对才可单独执行。helpers可能探测实际宿主，运行前必须阅读，不以pytest全目录盲跑。真实模型/钥匙串/NAS不用合成组件结果冒充。

持续观察仍包括自然新对话、真实压缩、DSH新agent回合、24h/7d、重启恢复；没有实际触发应记未测。私有历史、详细部署证据和回退备份留本机，不随源码上传。


跨仓库协议校验（克隆及安装后）：

    python components/bridge/scripts/sync_contracts.py --source components/core --check

协议内容未改；来源pin指向新Core仓库首次加入contracts/v1的提交，后续非协议提交不导致漂移。新主仓库通过固定submodule提交保证两侧一致。
