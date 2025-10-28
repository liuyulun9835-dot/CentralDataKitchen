# 版本管理约定

## exporter_version
- 语义化版本（MAJOR.MINOR），描述导出器实现行为。
- MINOR 变更可包含性能优化、可选字段补充；MAJOR 变更表示字段含义或业务逻辑发生破坏性调整。
- 当前版本：`6.3`。任何变更需在代码和文档中同步更新，并记录变更日志。

## schema_version
- 语义化命名，格式 `{dataset}.{stream}.{major_minor}`，如 `bar_1m.v6_3`、`tick.v1`。
- `major_minor` 变更影响下游解析；增加新字段或调整类型需递增版本，并更新 JSON Schema。

## 变更影响矩阵

| 变更项 | 示例 | 是否需要 bump exporter_version | 是否需要 bump schema_version | 后续动作 |
| --- | --- | --- | --- | --- |
| 仅优化性能/日志 | 改进缓冲策略 | ✅（MINOR） | ❌ | 更新 README/变更日志 |
| 新增可选字段 | 增加 `bar_vpo_*` | ✅（MINOR） | ✅（MINOR 或补丁） | 更新 Schema + 升级文档 |
| 字段语义调整 | 修改 volume 含义 | ✅（MAJOR） | ✅（MAJOR） | 发布迁移说明 + 下游协调 |
| 输出路径/命名调整 | 目录结构变更 | ✅（MAJOR） | ✅（按需） | 更新数据契约 + 自动化脚本 |
| Bug fix（无数据格式影响） | 修复心跳间隔 | ✅（补丁） | ❌ | 通知运营确认 |

> 任意版本提升后，需执行治理 checklist，并更新 `tools/governance` 内相关文档。
