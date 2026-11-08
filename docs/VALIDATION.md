# 2.0.0 验证范围

在macOS13 Intel上新建Python3.11/3.14独立venv，哈希锁依赖、构建wheel、--no-index --no-deps安装；未混装旧distribution。Codex独立运行结果：Core877、Bridge335及4子测试（0跳过）、跨组件45、Hermes解释器兼容125、外部队列1项/10组断言通过。版本、新旧CLI、pip check、协议字节及来源pin通过。详细结果见publication-validation.json。

新增工具名最初未被采集器识别为记忆引用；已同时修复两宿主reader和Hermes工具来源判断，并验收新旧两个名称。保留失败记录，不将首轮描述成全部通过。另一次测试用例追加的语法错误修正后完整复跑通过。

排除真实默认HOME测试：tests/e2e/test_hermes_manager_integration.py及Bridge tests/integrations/test_dsh_plugin.py。单独宿主加载检查不得调用真实模型、发送用户消息或改变业务数据。Core有一条既知asyncio_mode配置警告；这不是隐含跳过测试。

七天运行观察及此前受控功能测试不代表54项全场景验收。真实复杂科研写作、全部角色自然语义接续、严格压缩hook回执等仍按各自证据记录；不得借用其他角色覆盖。私人历史、详细部署报告和回退备份仅保留本机。

跨仓库协议校验：

    python components/bridge/scripts/sync_contracts.py --source components/core --check

协议正文未改，来源pin仍指向新Core仓库首次加入contracts/v1的提交，固定submodule保证两侧一致。
