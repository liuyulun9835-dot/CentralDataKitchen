# 数据发布合规检查清单

1. **文件可读性**：所有 JSONL/CSV 文件必须以 UTF-8（无 BOM）编码，换行符使用 `\n`。
2. **Schema 校验**：
   - ATAS Bar1m 输出符合 `SCHEMA_bar_1m.v6_3.json`。
   - ATAS TickTape 输出符合 `SCHEMA_tick.v1.json`。
   - Binance CSV 列顺序固定：`timestamp,open,high,low,close,volume`。
3. **空行率 < 0.1%**：文件中空白行占比不得超过 0.1%。
4. **时间单调递增**：
   - 分钟线按 `timestamp` 非递减排列。
   - Tick 按 `ts` 严格递增。
5. **覆盖率 ≥95%**：以 manifest 为准，确认分钟线/Tick 数据覆盖度不低于 95%。
6. **心跳正常**：对应心跳文件更新时间距当前不得超过 15 秒。
7. **异常处理**：若某项不达标需回滚发布，或按治理规范申请降级豁免。
