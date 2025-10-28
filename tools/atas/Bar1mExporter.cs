using System;
using System.IO;
using System.Text.Json;
#if !NO_ATAS_SDK
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Collections;
using ATAS.Indicators.Technical;
using ATAS.Types;
#endif

namespace CentralDataKitchen.Tools.ATAS;

internal sealed class BarExporterCore : IDisposable
{
    private readonly object _sync = new();
    private BufferedJsonlWriter? _writer;
    private HeartbeatWriter? _heartbeat;
    private string? _activeSymbol;
    private string? _currentPath;

    public string OutputRoot { get; set; } = PathHelper.DefaultOutputRoot;
    public int FlushBatchSize { get; set; } = 200;
    public int FlushIntervalMs { get; set; } = 100;
    public int HeartbeatEveryMs { get; set; } = 5000;
    public bool PartitionByHour { get; set; }
    public string ExporterVersion { get; } = "6.3";
    public string SchemaVersion { get; } = "bar_1m.v6_3";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    public void Configure(string outputRoot, int flushBatchSize, int flushIntervalMs, int heartbeatEveryMs, bool partitionByHour)
    {
        if (!string.IsNullOrWhiteSpace(outputRoot))
        {
            OutputRoot = outputRoot;
        }

        FlushBatchSize = Math.Max(1, flushBatchSize);
        FlushIntervalMs = Math.Max(10, flushIntervalMs);
        HeartbeatEveryMs = Math.Max(1000, heartbeatEveryMs);
        PartitionByHour = partitionByHour;
    }

    public void EnsureWriters(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol))
        {
            symbol = "UNKNOWN";
        }

        lock (_sync)
        {
            if (string.Equals(symbol, _activeSymbol, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            _writer?.Dispose();
            _heartbeat?.Dispose();

            _activeSymbol = symbol;
            var heartbeatPath = PathHelper.BuildHeartbeatPath(OutputRoot, nameof(Bar1mExporter), symbol);
            _heartbeat = new HeartbeatWriter(heartbeatPath, HeartbeatEveryMs);
        }
    }

    public void WriteBar(string symbol, DateTime minuteCloseUtc, BarPayload payload)
    {
        EnsureWriters(symbol);
        var path = PathHelper.BuildBar1mPath(OutputRoot, symbol, minuteCloseUtc);

        lock (_sync)
        {
            if (!string.Equals(_currentPath, path, StringComparison.OrdinalIgnoreCase))
            {
                _writer?.Dispose();
                _currentPath = path;
                _writer = new BufferedJsonlWriter(path, FlushBatchSize, FlushIntervalMs);
            }

            var json = JsonSerializer.Serialize(payload, JsonOptions);
            _writer!.Enqueue(json);
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            _writer?.Dispose();
            _heartbeat?.Dispose();
            _writer = null;
            _heartbeat = null;
            _currentPath = null;
        }
    }
}

internal sealed record BarPayload(
    string timestamp,
    string timestamp_utc,
    decimal open,
    decimal high,
    decimal low,
    decimal close,
    decimal volume,
    decimal? cvd,
    decimal? vah,
    decimal? val,
    decimal? poc,
    decimal? bar_vpo_price,
    decimal? bar_vpo_vol,
    decimal? bar_vpo_loc,
    string? bar_vpo_side,
    string exporter_version,
    string schema_version
);

#if NO_ATAS_SDK
public class Bar1mExporter : IDisposable
{
    private readonly BarExporterCore _core = new();
    private string _outputRoot = PathHelper.DefaultOutputRoot;
    private int _flushBatchSize = 200;
    private int _flushIntervalMs = 100;
    private int _heartbeatEveryMs = 5000;
    private bool _partitionByHour;

    public string OutputRoot
    {
        get => _outputRoot;
        set
        {
            _outputRoot = string.IsNullOrWhiteSpace(value) ? PathHelper.DefaultOutputRoot : value;
            ApplyConfig();
        }
    }

    public int FlushBatchSize
    {
        get => _flushBatchSize;
        set
        {
            _flushBatchSize = Math.Max(1, value);
            ApplyConfig();
        }
    }

    public int FlushIntervalMs
    {
        get => _flushIntervalMs;
        set
        {
            _flushIntervalMs = Math.Max(10, value);
            ApplyConfig();
        }
    }

    public int HeartbeatEveryMs
    {
        get => _heartbeatEveryMs;
        set
        {
            _heartbeatEveryMs = Math.Max(1000, value);
            ApplyConfig();
        }
    }

    public bool PartitionByHour
    {
        get => _partitionByHour;
        set
        {
            _partitionByHour = value;
            ApplyConfig();
        }
    }

    public string Symbol { get; set; } = string.Empty;

    public Bar1mExporter()
    {
        SafeLogger.Info("Bar1mExporter compiled without ATAS SDK (NO_ATAS_SDK).");
        ApplyConfig();
    }

    private void ApplyConfig()
    {
        _core.Configure(_outputRoot, _flushBatchSize, _flushIntervalMs, _heartbeatEveryMs, _partitionByHour);
    }

    public void Dispose()
    {
        _core.Dispose();
    }
}
#else
public class Bar1mExporter : Indicator
{
    private readonly BarExporterCore _core = new();
    private string _outputRoot = PathHelper.DefaultOutputRoot;
    private int _flushBatchSize = 200;
    private int _flushIntervalMs = 100;
    private int _heartbeatEveryMs = 5000;
    private bool _partitionByHour;

    [Display(Name = "Output Root", GroupName = "CentralDataKitchen", Order = 0)]
    public string OutputRoot
    {
        get => _outputRoot;
        set
        {
            _outputRoot = string.IsNullOrWhiteSpace(value) ? PathHelper.DefaultOutputRoot : value;
            ApplyConfig();
        }
    }

    [Display(Name = "Flush Batch Size", GroupName = "CentralDataKitchen", Order = 1)]
    public int FlushBatchSize
    {
        get => _flushBatchSize;
        set
        {
            _flushBatchSize = Math.Max(1, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Flush Interval (ms)", GroupName = "CentralDataKitchen", Order = 2)]
    public int FlushIntervalMs
    {
        get => _flushIntervalMs;
        set
        {
            _flushIntervalMs = Math.Max(10, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Heartbeat (ms)", GroupName = "CentralDataKitchen", Order = 3)]
    public int HeartbeatEveryMs
    {
        get => _heartbeatEveryMs;
        set
        {
            _heartbeatEveryMs = Math.Max(1000, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Partition By Hour", GroupName = "CentralDataKitchen", Order = 4)]
    public bool PartitionByHour
    {
        get => _partitionByHour;
        set
        {
            _partitionByHour = value;
            ApplyConfig();
        }
    }

    [Display(Name = "Symbol Override", GroupName = "CentralDataKitchen", Order = 5)]
    public string Symbol { get; set; } = string.Empty;

    public Bar1mExporter()
    {
        Name = "CDK Bar1m Exporter";
        ApplyConfig();
    }

    private void ApplyConfig()
    {
        _core.Configure(_outputRoot, _flushBatchSize, _flushIntervalMs, _heartbeatEveryMs, _partitionByHour);
    }

    protected override void OnCalculate(int bar, decimal value)
    {
        if (bar < 0 || bar >= Count)
        {
            return;
        }

        var time = GetTime(bar);
        if (time == default)
        {
            return;
        }

        var minuteClose = DateTime.SpecifyKind(time, DateTimeKind.Utc);
        var payload = BuildPayload(bar, minuteClose);
        var symbol = string.IsNullOrWhiteSpace(Symbol) ? InstrumentInfo?.Instrument : Symbol;
        if (string.IsNullOrWhiteSpace(symbol))
        {
            symbol = "UNKNOWN";
        }

        ApplyConfig();
        _core.WriteBar(symbol!, minuteClose, payload);
    }

    private BarPayload BuildPayload(int bar, DateTime minuteClose)
    {
        decimal open = GetCandleValue(bar, "Open") ?? 0m;
        decimal high = GetCandleValue(bar, "High") ?? 0m;
        decimal low = GetCandleValue(bar, "Low") ?? 0m;
        decimal close = GetCandleValue(bar, "Close") ?? 0m;
        decimal volume = GetCandleValue(bar, "Volume") ?? 0m;

        decimal? cvd = TryGetIndicatorDecimal("CumulativeDelta", bar);
        decimal? vah = TryGetIndicatorDecimal("ValueAreaHigh", bar);
        decimal? val = TryGetIndicatorDecimal("ValueAreaLow", bar);
        decimal? poc = TryGetIndicatorDecimal("POC", bar);
        decimal? barVpoPrice = TryGetIndicatorDecimal("BarVPOPrice", bar);
        decimal? barVpoVol = TryGetIndicatorDecimal("BarVPOVolume", bar);
        decimal? barVpoLoc = TryGetIndicatorDecimal("BarVPOLocation", bar);
        string? barVpoSide = TryGetIndicatorString("BarVPOSide", bar);

        var isoTimestamp = minuteClose.ToString("O", CultureInfo.InvariantCulture);

        return new BarPayload(
            isoTimestamp,
            isoTimestamp,
            open,
            high,
            low,
            close,
            volume,
            cvd,
            vah,
            val,
            poc,
            barVpoPrice,
            barVpoVol,
            barVpoLoc,
            barVpoSide,
            _core.ExporterVersion,
            _core.SchemaVersion);
    }

    private decimal? GetCandleValue(int bar, string property)
    {
        try
        {
            var candle = ResolveCandle(bar);
            if (candle != null)
            {
                var prop = candle.GetType().GetProperty(property, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
                if (prop != null)
                {
                    return ConvertToDecimal(prop.GetValue(candle));
                }
            }

            var series = GetSeries(property);
            if (series != null)
            {
                var value = GetIndexedValue(series, bar);
                return ConvertToDecimal(value);
            }
        }
        catch
        {
            // ignore
        }

        return null;
    }

    private object? ResolveCandle(int bar)
    {
        var candlesProp = GetType().GetProperty("Candles", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        if (candlesProp == null)
        {
            candlesProp = GetType().GetProperty("CandleSeries", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        }

        var candles = candlesProp?.GetValue(this);
        return candles != null ? GetIndexedValue(candles, bar) : null;
    }

    private object? GetSeries(string name)
    {
        var prop = GetType().GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        return prop?.GetValue(this);
    }

    private static object? GetIndexedValue(object? source, int index)
    {
        if (source == null)
        {
            return null;
        }

        var type = source.GetType();
        var defaultMembers = type.GetDefaultMembers();
        foreach (var member in defaultMembers)
        {
            if (member is System.Reflection.PropertyInfo pi && pi.GetIndexParameters().Length == 1)
            {
                try
                {
                    return pi.GetValue(source, new object[] { index });
                }
                catch
                {
                    // ignore and continue
                }
            }
        }

        if (source is System.Collections.IList list && index >= 0 && index < list.Count)
        {
            return list[index];
        }

        return null;
    }

    private decimal? TryGetIndicatorDecimal(string name, int bar)
    {
        try
        {
            var method = GetType().GetMethod("GetIndicatorValue", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (method != null)
            {
                var value = method.Invoke(this, new object[] { name, bar });
                return ConvertToDecimal(value);
            }
        }
        catch
        {
            // ignore
        }

        return null;
    }

    private string? TryGetIndicatorString(string name, int bar)
    {
        try
        {
            var method = GetType().GetMethod("GetIndicatorString", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            if (method != null)
            {
                var value = method.Invoke(this, new object[] { name, bar });
                return value?.ToString();
            }
        }
        catch
        {
            // ignore
        }

        return null;
    }

    private static decimal? ConvertToDecimal(object? value)
    {
        if (value == null)
        {
            return null;
        }

        try
        {
            return value switch
            {
                decimal d => d,
                double d => (decimal)d,
                float f => (decimal)f,
                int i => i,
                long l => l,
                string s when decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var result) => result,
                _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture)
            };
        }
        catch
        {
            return null;
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _core.Dispose();
    }
}
#endif
