# CentralDataKitchen ATAS Exporters

本目录提供两个遵循统一规范的 ATAS 自定义导出器：

- **Bar1mExporter**：输出右闭区间的 1 分钟 K 线数据（含可用的体积剖面扩展字段）。
- **TickTapeExporter**：输出逐笔成交数据，支持可选的盘口价/逐笔 ID。

## 编译与部署

1. 在 Windows 环境中安装 .NET SDK 6.0 以上版本。
2. 在仓库根目录执行：
   ```powershell
   dotnet build -c Release tools/atas/AtasIndicators.csproj
   ```
3. 构建结束后，`tools/atas/dist/` 下会生成 `CentralDataKitchen.Tools.ATAS.dll`。将该 DLL 拷贝至 ATAS 自定义指标目录（通常位于 `%LOCALAPPDATA%\Programs\ATAS\Indicators` 或安装根目录的 `Indicators` 子目录）。

> 💡 未安装 ATAS SDK 时，CI 默认启用 `NO_ATAS_SDK` 条件编译符号，仅生成空壳类以保证仓库编译顺利。安装 SDK 并重新构建即可获得完整功能。

## 图表挂载示例

- 在同一工作区中，新建一张 1 分钟主图，加载 **Bar1mExporter**。
- 再新建一张逐笔 Tape 图，加载 **TickTapeExporter**。
- 可分别设置 `Symbol Override`/`Exchange`、`Output Root`、缓冲及心跳参数；若留空 Symbol，将自动读取当前图表的交易品种。

## 输出路径规范

- 分钟线：`C:\CentralDataKitchen\staging\atas\bars_1m\{SYMBOL}\date=YYYY-MM-DD\bars_1m.jsonl`
- 逐笔：`C:\CentralDataKitchen\staging\atas\ticks\{SYMBOL}\date=YYYY-MM-DD\HH\ticks.jsonl`
- 心跳：`C:\CentralDataKitchen\staging\_heartbeats\{ExporterName}\{SYMBOL}\heartbeat.txt`

所有导出均使用 UTC 时间戳，右闭区间（分钟收盘时刻为该条记录的时间）。写盘采用 `.part` 临时文件 + 原子替换策略，批量缓冲参数默认 `FlushBatchSize=200`、`FlushIntervalMs=100`。心跳文件每 5 秒刷新一次，可用于运行监控。

## 版本约定

- `exporter_version="6.3"`
- Bar1mExporter 输出的 `schema_version="bar_1m.v6_3"`
- TickTapeExporter 输出的 `schema_version="tick.v1"`

如需扩展字段或变更结构，请同步更新 `tools/governance` 目录下的数据契约文档与 JSON Schema。
