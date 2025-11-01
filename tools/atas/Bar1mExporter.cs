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
    // Bump the schema version to reflect the addition of vbuy/vsell fields and new
    // DOM and open interest fields.  Updating the schema version signals
    // downstream consumers to handle the additional columns.
    public string SchemaVersion { get; } = "bar_1m.v6_5";

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

// The bar payload now includes separate buy and sell volume fields (vbuy/vsell).
// These fields allow downstream processors to distinguish between aggressive
// buying and selling pressure within each one‑minute bar.  Without the ATAS
// SDK installed (see NO_ATAS_SDK), these values will be null.  When the SDK
// is present the values are aggregated from incoming trade events.
// The BarPayload has been extended to include open interest and aggregated DOM
// statistics (cumulative bid and ask depth).  These metrics provide
// information about the state of the order book and the positioning of market
// participants over each minute bar.  When the ATAS SDK is unavailable these
// fields will be null.
internal sealed class BarPayload
{
    [JsonProperty("timestamp")]
    public string Timestamp { get; set; } = string.Empty;

    [JsonProperty("timestamp_utc")]
    public string TimestampUtc { get; set; } = string.Empty;

    [JsonProperty("open")]
    public decimal Open { get; set; }

    [JsonProperty("high")]
    public decimal High { get; set; }

    [JsonProperty("low")]
    public decimal Low { get; set; }

    [JsonProperty("close")]
    public decimal Close { get; set; }

    [JsonProperty("volume")]
    public decimal Volume { get; set; }

    [JsonProperty("cvd")]
    public decimal? Cvd { get; set; }

    [JsonProperty("vah")]
    public decimal? ValueAreaHigh { get; set; }

    [JsonProperty("val")]
    public decimal? ValueAreaLow { get; set; }

    [JsonProperty("poc")]
    public decimal? PointOfControl { get; set; }

    [JsonProperty("bar_vpo_price")]
    public decimal? BarVpoPrice { get; set; }

    [JsonProperty("bar_vpo_vol")]
    public decimal? BarVpoVolume { get; set; }

    [JsonProperty("bar_vpo_loc")]
    public decimal? BarVpoLocation { get; set; }

    [JsonProperty("bar_vpo_side")]
    public string? BarVpoSide { get; set; }

    [JsonProperty("vbuy")]
    public decimal? VolumeBuy { get; set; }

    [JsonProperty("vsell")]
    public decimal? VolumeSell { get; set; }

    [JsonProperty("oi")]
    public decimal? OpenInterest { get; set; }

    [JsonProperty("cum_bid_depth")]
    public decimal? CumulativeBidDepth { get; set; }

    [JsonProperty("cum_ask_depth")]
    public decimal? CumulativeAskDepth { get; set; }

    [JsonProperty("exporter_version")]
    public string ExporterVersion { get; set; } = string.Empty;

    [JsonProperty("schema_version")]
    public string SchemaVersion { get; set; } = string.Empty;
}

public class Bar1mExporter : Indicator
{
    // Track buy/sell volume at the granularity of bar close times (minute precision).
    // The key is the UTC time of the bar close; values are the cumulative
    // aggressive buy and sell volumes seen via OnNewTrade.  This dictionary is
    // cleared lazily in BuildPayload to avoid unbounded growth.  When NO_ATAS_SDK
    // is defined this collection remains empty because OnNewTrade is not
    // available.
    private readonly System.Collections.Generic.Dictionary<DateTime, (decimal Buy, decimal Sell)> _volumeBySide = new();

    // Track cumulative DOM depth (bid and ask) by minute.  When MarketDepthChanged
    // fires we record the most recent depth values keyed by the minute.  These
    // values are exported in the bar payload via the cum_bid_depth and
    // cum_ask_depth fields.  When NO_ATAS_SDK is defined this collection
    // remains empty because MarketDepthChanged is not available.
    private readonly System.Collections.Generic.Dictionary<DateTime, (decimal CumBids, decimal CumAsks)> _depthByMinute = new();
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

    /// <summary>
    /// Handles each new trade and aggregates its volume into buy/sell buckets.
    /// This method is called by ATAS when a new tick is received.  It is only
    /// compiled when the ATAS SDK is available (NO_ATAS_SDK is not defined).
    /// </summary>
    protected override void OnNewTrade(ATAS.Types.MarketDataArg arg)
    {
        if (arg?.Trade == null)
        {
            return;
        }

        var trade = arg.Trade;
        // Resolve the UTC time of the trade.  It may be represented as a DateTime
        // or a Unix millisecond timestamp.  The helper returns a UTC DateTime.
        var utcTime = ResolveTradeTime(trade);
        // Round the time down to the start of its minute.  The exporter records
        // bars as right‑closed intervals ending at minute close.  We accumulate
        // volume for the current minute based on the trade time.
        var minute = new DateTime(utcTime.Year, utcTime.Month, utcTime.Day, utcTime.Hour, utcTime.Minute, 0, DateTimeKind.Utc);
        // Determine whether this trade is a buy or sell.  If the side cannot be
        // resolved the trade is treated as a buy by default.
        var side = ResolveSide(trade);
        var volume = GetDecimalProperty(trade, "Volume") ?? 0m;
        lock (_volumeBySide)
        {
            if (!_volumeBySide.TryGetValue(minute, out var entry))
            {
                entry = (0m, 0m);
            }
            if (string.Equals(side, "sell", System.StringComparison.OrdinalIgnoreCase))
            {
                entry.Sell += volume;
            }
            else
            {
                entry.Buy += volume;
            }
            _volumeBySide[minute] = entry;
        }
    }

    /// <summary>
    /// Handles changes in the market depth (DOM) and captures the cumulative
    /// bid and ask volumes at each minute boundary.  This method is only
    /// compiled when the ATAS SDK is available.  It uses the current UTC
    /// timestamp of the change event to determine which minute bucket to
    /// update.  If MarketDepthInfo is unavailable or throws, the update is
    /// skipped.
    /// </summary>
    protected override void MarketDepthChanged(ATAS.Types.MarketDataArg arg)
    {
        // Determine the current time of the market depth update.  We use
        // DateTime.UtcNow because MarketDepthChanged does not expose a time
        // property directly on the argument.  If the provider sets arg.Time
        // you could use ResolveTradeTime as with trades.
        var utcNow = DateTime.UtcNow;
        var minute = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, utcNow.Minute, 0, DateTimeKind.Utc);
        try
        {
            var md = MarketDepthInfo;
            if (md != null)
            {
                var bids = md.CumulativeDomBids;
                var asks = md.CumulativeDomAsks;
                lock (_depthByMinute)
                {
                    _depthByMinute[minute] = (bids, asks);
                }
            }
        }
        catch
        {
            // ignore exceptions retrieving market depth
        }
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

        // Extract the aggregated buy and sell volume for the current minute.  We
        // clear the entry after retrieving it to prevent memory growth across
        // subsequent bars.
        decimal? vbuy = null;
        decimal? vsell = null;
        lock (_volumeBySide)
        {
            if (_volumeBySide.TryGetValue(minuteClose, out var entry))
            {
                vbuy = entry.Buy;
                vsell = entry.Sell;
                _volumeBySide.Remove(minuteClose);
            }
        }

        // Extract the last observed DOM cumulative depth for the current minute.  We
        // clear the entry after retrieving it to prevent memory growth.  These
        // values represent the total bid and ask volume in the order book at a
        // point during the minute.  If the DOM has not been updated during
        // this minute the fields remain null.
        decimal? cumBidDepth = null;
        decimal? cumAskDepth = null;
        lock (_depthByMinute)
        {
            if (_depthByMinute.TryGetValue(minuteClose, out var depth))
            {
                cumBidDepth = depth.CumBids;
                cumAskDepth = depth.CumAsks;
                _depthByMinute.Remove(minuteClose);
            }
        }

        // Retrieve open interest (OI) if available.  Some data providers expose
        // this as the "OI" property on a candle.  The GetCandleValue helper
        // gracefully returns null if the property is missing.  OI provides
        // insight into the number of open contracts.
        decimal? oi = GetCandleValue(bar, "OI");

        var isoTimestamp = minuteClose.ToString("O", CultureInfo.InvariantCulture);

        return new BarPayload
        {
            Timestamp = isoTimestamp,
            TimestampUtc = isoTimestamp,
            Open = open,
            High = high,
            Low = low,
            Close = close,
            Volume = volume,
            Cvd = cvd,
            ValueAreaHigh = vah,
            ValueAreaLow = val,
            PointOfControl = poc,
            BarVpoPrice = barVpoPrice,
            BarVpoVolume = barVpoVol,
            BarVpoLocation = barVpoLoc,
            BarVpoSide = barVpoSide,
            VolumeBuy = vbuy,
            VolumeSell = vsell,
            OpenInterest = oi,
            CumulativeBidDepth = cumBidDepth,
            CumulativeAskDepth = cumAskDepth,
            ExporterVersion = _core.ExporterVersion,
            SchemaVersion = _core.SchemaVersion
        };
    }

    #region Trade helpers
    // The following helper methods mirror those used by TickTapeExporter.  They are
    // duplicated here rather than shared via a common base class to avoid
    // unnecessary refactoring and keep the impact localized.  Should future
    // indicators also need these helpers they could be extracted into a shared
    // utility class.

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
    #endregion

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
