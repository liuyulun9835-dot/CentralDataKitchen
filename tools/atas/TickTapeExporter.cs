using System;
using System.IO;
using System.Text;
using System.Globalization;
using System.ComponentModel.DataAnnotations;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using ATAS.Indicators;
using ATAS.Indicators.Technical;
using ATAS.Types;

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
    // Bump the schema version to reflect addition of vbuy/vsell fields
    // Increase schema version to reflect additional tick-level fields (bid/ask volumes,
    // cumulative depth and midprice).  Schema versions should be updated whenever the
    // structure of the emitted JSON changes to help downstream consumers handle
    // backward compatibility.
    public string SchemaVersion { get; } = "tick.v3";

    private static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver(),
        NullValueHandling = NullValueHandling.Ignore,
        Formatting = Formatting.None
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

            var json = JsonConvert.SerializeObject(payload, JsonSettings);
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

// Tick payload now includes vbuy/vsell fields which represent the quantity of
// each tick attributed to aggressive buys and sells.  For a buy trade
// vbuy equals qty and vsell is null; for a sell trade vsell equals qty and vbuy
// is null.  Adding these fields makes the tick export self‑sufficient for
// computing directional metrics (e.g. VPIN) without reconstructing from side.
// The TickPayload schema has been extended beyond the basic price, quantity and side to
// include order book information (best bid/ask price and size), cumulative DOM depth
// and a computed midprice.  When the ATAS SDK is unavailable these fields will be
// null.  Downstream consumers can use these values to compute order book imbalance,
// slippage and other microstructure metrics.
internal sealed class TickPayload
{
    [JsonProperty("ts")]
    public string Timestamp { get; set; } = string.Empty;

    [JsonProperty("exchange")]
    public string Exchange { get; set; } = string.Empty;

    [JsonProperty("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonProperty("price")]
    public decimal Price { get; set; }

    [JsonProperty("qty")]
    public decimal Quantity { get; set; }

    [JsonProperty("side")]
    public string Side { get; set; } = string.Empty;

    [JsonProperty("vbuy")]
    public decimal? VolumeBuy { get; set; }

    [JsonProperty("vsell")]
    public decimal? VolumeSell { get; set; }

    [JsonProperty("best_bid")]
    public decimal? BestBid { get; set; }

    [JsonProperty("best_ask")]
    public decimal? BestAsk { get; set; }

    [JsonProperty("bid_volume")]
    public decimal? BidVolume { get; set; }

    [JsonProperty("ask_volume")]
    public decimal? AskVolume { get; set; }

    [JsonProperty("cum_bid_depth")]
    public decimal? CumulativeBidDepth { get; set; }

    [JsonProperty("cum_ask_depth")]
    public decimal? CumulativeAskDepth { get; set; }

    [JsonProperty("mid_price")]
    public decimal? MidPrice { get; set; }

    [JsonProperty("trade_id")]
    public string? TradeId { get; set; }

    [JsonProperty("exporter_version")]
    public string ExporterVersion { get; set; } = string.Empty;

    [JsonProperty("schema_version")]
    public string SchemaVersion { get; set; } = string.Empty;
}

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

        // Derive quantity and side once to allow calculation of vbuy/vsell.  vbuy
        // equals qty for buy trades, vsell equals qty for sell trades.  When the
        // side is unknown both are left null.
        var qty = GetDecimalProperty(trade, "Volume") ?? 0m;
        var side = ResolveSide(trade);
        decimal? vbuy = null;
        decimal? vsell = null;
        if (string.Equals(side, "buy", System.StringComparison.OrdinalIgnoreCase))
        {
            vbuy = qty;
        }
        else if (string.Equals(side, "sell", System.StringComparison.OrdinalIgnoreCase))
        {
            vsell = qty;
        }

        // Extract best bid and ask prices and volumes.  These may not be present on
        // every data provider; GetDecimalProperty gracefully returns null if the
        // property is missing.  The volume on the bid/ask represents the size
        // resting at the top of the book at the time of the trade.  Additional
        // depth metrics are retrieved from the MarketDepthInfo object when
        // available.  A mid price is computed from the best bid and ask.
        var bestBid = GetDecimalProperty(trade, "Bid");
        var bestAsk = GetDecimalProperty(trade, "Ask");
        var bidVol = GetDecimalProperty(trade, "BidVolume");
        var askVol = GetDecimalProperty(trade, "AskVolume");
        decimal? cumBidDepth = null;
        decimal? cumAskDepth = null;
        decimal? midPrice = null;
        try
        {
            var md = MarketDepthInfo;
            if (md != null)
            {
                cumBidDepth = md.CumulativeDomBids;
                cumAskDepth = md.CumulativeDomAsks;
            }
        }
        catch
        {
            // ignore any issues retrieving market depth
        }
        if (bestBid.HasValue && bestAsk.HasValue)
        {
            midPrice = (bestBid.Value + bestAsk.Value) / 2m;
        }

        var payload = new TickPayload
        {
            Timestamp = iso,
            Exchange = string.IsNullOrWhiteSpace(Exchange) ? "BINANCE_FUTURES" : Exchange,
            Symbol = symbol!,
            Price = GetDecimalProperty(trade, "Price") ?? 0m,
            Quantity = qty,
            Side = side,
            VolumeBuy = vbuy,
            VolumeSell = vsell,
            BestBid = bestBid,
            BestAsk = bestAsk,
            BidVolume = bidVol,
            AskVolume = askVol,
            CumulativeBidDepth = cumBidDepth,
            CumulativeAskDepth = cumAskDepth,
            MidPrice = midPrice,
            TradeId = GetStringProperty(trade, "Id"),
            ExporterVersion = _core.ExporterVersion,
            SchemaVersion = _core.SchemaVersion
        };

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
