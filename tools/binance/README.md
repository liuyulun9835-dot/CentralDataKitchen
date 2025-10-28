# Binance 1m Data Fetcher

本目录提供 `fetch_binance_1m.py`，用于抓取 Binance USDT 永续合约 1 分钟 K 线并生成 CSV。

## 使用示例

```bash
python tools/binance/fetch_binance_1m.py --symbol BTCUSDT --date 2024-01-15 --out data/binance/btcusdt_2024-01-15.csv
```

脚本将自动断点续抓：如果目标 CSV 已存在，会读取最后一行时间戳并从下一分钟继续请求。输出文件采用 UTF-8（无 BOM），列顺序如下：

```
timestamp,open,high,low,close,volume
2024-01-15T00:01:00.000Z,42750.10,42780.00,42740.50,42760.25,152.381
```

所有时间戳均为 UTC，表示分钟的收盘时刻（右闭区间 [open, close]）。若遇到网络波动或 429 限流，脚本会采用指数退避重试。
