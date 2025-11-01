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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CentralDataKitchen.Tools.ATAS;

public static class PathHelper
{
    public const string DefaultOutputRoot = "C:\\CentralDataKitchen\\staging";

    public static string BuildBar1mPath(string root, string symbol, DateTime utcMinuteClose)
    {
        var outputRoot = string.IsNullOrWhiteSpace(root) ? DefaultOutputRoot : root;
        var symbolSegment = SanitizeSymbol(symbol);
        var dateSegment = utcMinuteClose.ToString("yyyy-MM-dd");
        var directory = Path.Combine(outputRoot, "atas", "bars_1m", symbolSegment, $"date={dateSegment}");
        var fileName = "bars_1m.jsonl";
        return Path.Combine(directory, fileName);
    }

    public static string BuildTickPath(string root, string symbol, DateTime utcTradeTime, bool partitionByHour)
    {
        var outputRoot = string.IsNullOrWhiteSpace(root) ? DefaultOutputRoot : root;
        var symbolSegment = SanitizeSymbol(symbol);
        var dateSegment = utcTradeTime.ToString("yyyy-MM-dd");
        var baseDirectory = Path.Combine(outputRoot, "atas", "ticks", symbolSegment, $"date={dateSegment}");
        if (partitionByHour)
        {
            var hourSegment = utcTradeTime.ToString("HH");
            baseDirectory = Path.Combine(baseDirectory, hourSegment);
        }

        return Path.Combine(baseDirectory, "ticks.jsonl");
    }

    public static string BuildHeartbeatPath(string root, string exporterName, string symbol)
    {
        var outputRoot = string.IsNullOrWhiteSpace(root) ? DefaultOutputRoot : root;
        var symbolSegment = SanitizeSymbol(symbol);
        return Path.Combine(outputRoot, "_heartbeats", exporterName, symbolSegment, "heartbeat.txt");
    }

    private static string SanitizeSymbol(string symbol)
    {
        return string.IsNullOrWhiteSpace(symbol)
            ? "UNKNOWN"
            : symbol.Trim().Replace('/', '_').Replace(' ', '_');
    }
}

public sealed class BufferedJsonlWriter : IDisposable
{
    private readonly string _targetPath;
    private readonly int _flushBatchSize;
    private readonly int _flushIntervalMs;
    private readonly Encoding _encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);
    private readonly ConcurrentQueue<string> _queue = new();
    private readonly ManualResetEventSlim _signal = new(false);
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _worker;
    private readonly object _flushLock = new();

    public BufferedJsonlWriter(string targetPath, int flushBatchSize = 200, int flushIntervalMs = 100)
    {
        _targetPath = targetPath ?? throw new ArgumentNullException(nameof(targetPath));
        _flushBatchSize = Math.Max(1, flushBatchSize);
        _flushIntervalMs = Math.Max(10, flushIntervalMs);
        _worker = Task.Run(ProcessQueueAsync);
    }

    public void Enqueue(string jsonLine)
    {
        if (jsonLine == null)
        {
            return;
        }

        _queue.Enqueue(jsonLine);
        _signal.Set();
    }

    public void Flush()
    {
        FlushInternal(force: true, CancellationToken.None);
    }

    private async Task ProcessQueueAsync()
    {
        var token = _cts.Token;
        var pending = new List<string>(_flushBatchSize);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            while (!token.IsCancellationRequested)
            {
                bool signaled = _signal.Wait(_flushIntervalMs, token);
                _signal.Reset();

                while (_queue.TryDequeue(out var line))
                {
                    pending.Add(line);
                    if (pending.Count >= _flushBatchSize)
                    {
                        FlushBatch(pending);
                        pending.Clear();
                        stopwatch.Restart();
                    }
                }

                if (pending.Count > 0 && (signaled == false || stopwatch.ElapsedMilliseconds >= _flushIntervalMs))
                {
                    FlushBatch(pending);
                    pending.Clear();
                    stopwatch.Restart();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected during dispose
        }
        catch (Exception ex)
        {
            SafeLogger.Error($"BufferedJsonlWriter worker failed: {ex}");
        }
        finally
        {
            if (pending.Count > 0)
            {
                try
                {
                    FlushBatch(pending);
                }
                catch (Exception ex)
                {
                    SafeLogger.Error($"BufferedJsonlWriter final flush failed: {ex}");
                }
            }
        }
    }

    private void FlushBatch(List<string> batch)
    {
        if (batch.Count == 0)
        {
            return;
        }

        lock (_flushLock)
        {
            var directory = Path.GetDirectoryName(_targetPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var partPath = _targetPath + ".part";
            using (var partStream = new FileStream(partPath, FileMode.Create, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(partStream, _encoding))
            {
                if (File.Exists(_targetPath))
                {
                    using var existing = new FileStream(_targetPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    existing.CopyTo(partStream);
                }

                foreach (var line in batch)
                {
                    writer.WriteLine(line);
                }
            }

            File.Move(partPath, _targetPath, overwrite: true);
        }
    }

    private void FlushInternal(bool force, CancellationToken token)
    {
        var drained = new List<string>();
        while (_queue.TryDequeue(out var line))
        {
            drained.Add(line);
            if (!force && drained.Count >= _flushBatchSize)
            {
                break;
            }
        }

        if (drained.Count > 0)
        {
            FlushBatch(drained);
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _signal.Set();
        try
        {
            _worker.Wait();
        }
        catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
        {
            // ignore
        }

        FlushInternal(force: true, CancellationToken.None);
        _cts.Dispose();
        _signal.Dispose();
    }
}

public sealed class HeartbeatWriter : IDisposable
{
    private readonly string _heartbeatPath;
    private readonly int _intervalMs;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _worker;

    public HeartbeatWriter(string heartbeatPath, int intervalMs = 5000)
    {
        _heartbeatPath = heartbeatPath ?? throw new ArgumentNullException(nameof(heartbeatPath));
        _intervalMs = Math.Max(1000, intervalMs);
        _worker = Task.Run(WriteLoopAsync);
    }

    private async Task WriteLoopAsync()
    {
        var token = _cts.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                WriteHeartbeat();
                await Task.Delay(_intervalMs, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // expected on dispose
        }
        catch (Exception ex)
        {
            SafeLogger.Error($"Heartbeat writer failed: {ex}");
        }
        finally
        {
            try
            {
                WriteHeartbeat();
            }
            catch (Exception ex)
            {
                SafeLogger.Error($"Heartbeat final write failed: {ex}");
            }
        }
    }

    private void WriteHeartbeat()
    {
        var directory = Path.GetDirectoryName(_heartbeatPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var content = $"{DateTime.UtcNow:O}\n";
        File.WriteAllText(_heartbeatPath, content, new UTF8Encoding(false));
    }

    public void Dispose()
    {
        _cts.Cancel();
        try
        {
            _worker.Wait();
        }
        catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
        {
            // ignore
        }

        _cts.Dispose();
    }
}

public static class SafeLogger
{
    private static readonly object Sync = new();

    public static void Info(string message) => Write("INFO", message);

    public static void Warn(string message) => Write("WARN", message);

    public static void Error(string message) => Write("ERROR", message);

    private static void Write(string level, string message)
    {
        lock (Sync)
        {
            Console.WriteLine($"[{DateTime.UtcNow:O}] [{level}] {message}");
        }
    }
}
