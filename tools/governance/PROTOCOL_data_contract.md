# CentralDataKitchen 数据契约（ATAS & Binance）

## 生产者
- **ATAS Bar1mExporter**：输出分钟线（schema_version=bar_1m.v6_3，exporter_version=6.3）。
- **ATAS TickTapeExporter**：输出逐笔成交（schema_version=tick.v1，exporter_version=6.3）。
- **Binance 1m Fetcher**：Python 抓数脚本 `tools/binance/fetch_binance_1m.py`，生成分钟线 CSV。

## 消费者
- CentralDataKitchen 数据对齐、时间校准、数据发布流水线。

## 路径与命名
- ATAS Tick：`C:\CentralDataKitchen\staging\atas\ticks\{SYMBOL}\date=YYYY-MM-DD\HH\ticks.jsonl`
- ATAS 1m：`C:\CentralDataKitchen\staging\atas\bars_1m\{SYMBOL}\date=YYYY-MM-DD\bars_1m.jsonl`
- Binance 1m CSV：`<workspace>/data/binance/{symbol}_YYYY-MM-DD.csv`
- 心跳：`C:\CentralDataKitchen\staging\_heartbeats\{ExporterName}\{SYMBOL}\heartbeat.txt`

## 时间边界与时区
- 全部使用 **UTC**。
- 分钟线采用右闭区间 `[minute_open, minute_close]`，记录时间戳为 `minute_close`。
- Tick 使用成交时间；Binance CSV 同样使用分钟收盘时间戳。

## 错配容差
- 时间戳误差容忍 ±500ms，超出需标记异常。
- 数据缺口容忍 ≤5%（覆盖率 ≥95%），超出触发降级流程。
- JSON 字段需符合对应 JSON Schema，CSV 列与顺序固定。

## 降级白名单
- 缺失可选字段（如 `cvd`、`bar_vpo_*`、盘口价、逐笔 ID）允许为空或 `null`。
- Binance 抓数若遇到部分分钟无成交，允许 volume=0 但需保留记录。

## Manifest / Signature
- 每个生产批次需附加 manifest（记录文件列表、行数、时间范围、哈希）。
- Manifest 与数据文件采用四键签名（`producer`, `dataset`, `date`, `version`）进行对接校验。
- 签名文件必须在同一目录，以 `.sig` 为后缀。
