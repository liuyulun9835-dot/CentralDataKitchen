using System;
using System.IO;
using System.Text.Json;
#if !NO_ATAS_SDK
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using ATAS.Indicators.Technical;
using ATAS.Types;
#endif

namespace CentralDataKitchen.Tools.ATAS;

internal sealed class TickExporterCore : IDisposable
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
    public string SchemaVersion { get; } = "tick.v1";

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
            _currentPath = null;
            var heartbeatPath = PathHelper.BuildHeartbeatPath(OutputRoot, nameof(TickTapeExporter), symbol);
            _heartbeat = new HeartbeatWriter(heartbeatPath, HeartbeatEveryMs);
        }
    }

    public void WriteTick(string symbol, DateTime tradeTimeUtc, TickPayload payload)
    {
        EnsureWriters(symbol);
        var path = PathHelper.BuildTickPath(OutputRoot, symbol, tradeTimeUtc, PartitionByHour);

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

internal sealed record TickPayload(
    string ts,
    string exchange,
    string symbol,
    decimal price,
    decimal qty,
    string side,
    decimal? best_bid,
    decimal? best_ask,
    string? trade_id,
    string exporter_version,
    string schema_version
);

#if NO_ATAS_SDK
public class TickTapeExporter : IDisposable
{
    private readonly TickExporterCore _core = new();
    private string _outputRoot = PathHelper.DefaultOutputRoot;
    private int _flushBatchSize = 200;
    private int _flushIntervalMs = 100;
    private int _heartbeatEveryMs = 5000;
    private bool _partitionByHour = true;

    public string Exchange { get; set; } = "BINANCE_FUTURES";
    public string Symbol { get; set; } = string.Empty;

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

    public TickTapeExporter()
    {
        SafeLogger.Info("TickTapeExporter compiled without ATAS SDK (NO_ATAS_SDK).");
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
public class TickTapeExporter : Indicator
{
    private readonly TickExporterCore _core = new();
    private string _outputRoot = PathHelper.DefaultOutputRoot;
    private int _flushBatchSize = 200;
    private int _flushIntervalMs = 100;
    private int _heartbeatEveryMs = 5000;
    private bool _partitionByHour = true;

    [Display(Name = "Exchange", GroupName = "CentralDataKitchen", Order = 0)]
    public string Exchange { get; set; } = "BINANCE_FUTURES";

    [Display(Name = "Symbol Override", GroupName = "CentralDataKitchen", Order = 1)]
    public string Symbol { get; set; } = string.Empty;

    [Display(Name = "Output Root", GroupName = "CentralDataKitchen", Order = 2)]
    public string OutputRoot
    {
        get => _outputRoot;
        set
        {
            _outputRoot = string.IsNullOrWhiteSpace(value) ? PathHelper.DefaultOutputRoot : value;
            ApplyConfig();
        }
    }

    [Display(Name = "Flush Batch Size", GroupName = "CentralDataKitchen", Order = 3)]
    public int FlushBatchSize
    {
        get => _flushBatchSize;
        set
        {
            _flushBatchSize = Math.Max(1, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Flush Interval (ms)", GroupName = "CentralDataKitchen", Order = 4)]
    public int FlushIntervalMs
    {
        get => _flushIntervalMs;
        set
        {
            _flushIntervalMs = Math.Max(10, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Heartbeat (ms)", GroupName = "CentralDataKitchen", Order = 5)]
    public int HeartbeatEveryMs
    {
        get => _heartbeatEveryMs;
        set
        {
            _heartbeatEveryMs = Math.Max(1000, value);
            ApplyConfig();
        }
    }

    [Display(Name = "Partition By Hour", GroupName = "CentralDataKitchen", Order = 6)]
    public bool PartitionByHour
    {
        get => _partitionByHour;
        set
        {
            _partitionByHour = value;
            ApplyConfig();
        }
    }

    public TickTapeExporter()
    {
        Name = "CDK TickTape Exporter";
        ApplyConfig();
    }

    private void ApplyConfig()
    {
        _core.Configure(_outputRoot, _flushBatchSize, _flushIntervalMs, _heartbeatEveryMs, _partitionByHour);
    }

    protected override void OnNewTrade(MarketDataArg arg)
    {
        if (arg?.Trade == null)
        {
            return;
        }

        var trade = arg.Trade;
        var symbol = string.IsNullOrWhiteSpace(Symbol) ? GetStringProperty(trade, "Instrument") ?? Symbol : Symbol;
        if (string.IsNullOrWhiteSpace(symbol))
        {
            symbol = "UNKNOWN";
        }

        var utcTime = ResolveTradeTime(trade);
        var iso = utcTime.ToString("O", CultureInfo.InvariantCulture);

        var payload = new TickPayload(
            iso,
            string.IsNullOrWhiteSpace(Exchange) ? "BINANCE_FUTURES" : Exchange,
            symbol!,
            GetDecimalProperty(trade, "Price") ?? 0m,
            GetDecimalProperty(trade, "Volume") ?? 0m,
            ResolveSide(trade),
            GetDecimalProperty(trade, "Bid"),
            GetDecimalProperty(trade, "Ask"),
            GetStringProperty(trade, "Id"),
            _core.ExporterVersion,
            _core.SchemaVersion);

        ApplyConfig();
        _core.WriteTick(symbol!, utcTime, payload);
    }

    private static DateTime ResolveTradeTime(object trade)
    {
        var prop = trade.GetType().GetProperty("Time", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        if (prop != null)
        {
            var value = prop.GetValue(trade);
            if (value is DateTime dt)
            {
                return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            }

            if (value is long ms)
            {
                var seconds = ms / 1000;
                var remainder = ms % 1000;
                return DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime.AddMilliseconds(remainder);
            }
        }

        return DateTime.UtcNow;
    }

    private static decimal? GetDecimalProperty(object trade, string name)
    {
        var prop = trade.GetType().GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        if (prop == null)
        {
            return null;
        }

        try
        {
            var value = prop.GetValue(trade);
            if (value == null)
            {
                return null;
            }

            return value switch
            {
                decimal d => d,
                double d => (decimal)d,
                float f => (decimal)f,
                long l => l,
                int i => i,
                string s when decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var result) => result,
                _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture)
            };
        }
        catch
        {
            return null;
        }
    }

    private static string? GetStringProperty(object trade, string name)
    {
        var prop = trade.GetType().GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        return prop?.GetValue(trade)?.ToString();
    }

    private static string ResolveSide(object trade)
    {
        var sideProp = trade.GetType().GetProperty("Side", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        if (sideProp != null)
        {
            var value = sideProp.GetValue(trade)?.ToString();
            if (!string.IsNullOrWhiteSpace(value))
            {
                var normalized = value.Trim().ToLowerInvariant();
                if (normalized == "buy" || normalized == "sell")
                {
                    return normalized;
                }
            }
        }

        var isBuyProp = trade.GetType().GetProperty("IsBuy", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.IgnoreCase);
        if (isBuyProp != null)
        {
            try
            {
                var value = isBuyProp.GetValue(trade);
                if (value != null)
                {
                    return Convert.ToBoolean(value, CultureInfo.InvariantCulture) ? "buy" : "sell";
                }
            }
            catch
            {
                // ignore
            }
        }

        return "buy";
    }

    public override void Dispose()
    {
        base.Dispose();
        _core.Dispose();
    }
}
#endif
