namespace Creditbus.Facade.LoadTests;

public sealed class MetricsTracker
{
    private long _totalPublished;
    private long _totalErrors;
    private long _publishedInWindow;
    private long _totalLatencyMs;
    private long _windowLatencyMs;
    private long _windowCount;

    public void RecordSuccess(
        long latencyMs,
        Guid correlationId,
        long operationId,
        long tradingAccount,
        string status,
        decimal limit)
    {
        Interlocked.Increment(ref _totalPublished);
        Interlocked.Increment(ref _publishedInWindow);
        Interlocked.Add(ref _totalLatencyMs, latencyMs);
        Interlocked.Add(ref _windowLatencyMs, latencyMs);
        Interlocked.Increment(ref _windowCount);

        Console.WriteLine(
            $"[MSG] CorrelationId: {correlationId} | Op: {operationId} | " +
            $"Account: {tradingAccount} | Status: {status} | Limit: {limit:N2}");
    }

    public void RecordError(Guid correlationId, string reason)
    {
        Interlocked.Increment(ref _totalErrors);
        Console.WriteLine($"[ERR] Falha ao publicar CorrelationId: {correlationId} — {reason}");
    }

    public void PrintWindowMetrics()
    {
        var rate = Interlocked.Exchange(ref _publishedInWindow, 0);
        var windowLatency = Interlocked.Exchange(ref _windowLatencyMs, 0);
        var windowCount = Interlocked.Exchange(ref _windowCount, 0);
        var total = Interlocked.Read(ref _totalPublished);
        var errors = Interlocked.Read(ref _totalErrors);
        var avgLatency = windowCount > 0 ? windowLatency / windowCount : 0;

        Console.WriteLine(
            $"[{DateTime.Now:HH:mm:ss}] rate: {rate} msg/s | " +
            $"total: {total:N0} | erros: {errors} | latência média: {avgLatency}ms");
    }

    public void PrintSummary(DateTime startTime)
    {
        var duration = DateTime.UtcNow - startTime;
        var total = Interlocked.Read(ref _totalPublished);
        var errors = Interlocked.Read(ref _totalErrors);
        var avgRate = duration.TotalSeconds > 0 ? total / duration.TotalSeconds : 0;
        var avgLatency = total > 0
            ? Interlocked.Read(ref _totalLatencyMs) / total
            : 0;

        Console.WriteLine("""

            ═══════════════════════════════════════
              LOAD TEST CONCLUÍDO
            """);
        Console.WriteLine($"  Duração:        {duration:hh\\:mm\\:ss}");
        Console.WriteLine($"  Total enviado:  {total:N0}");
        Console.WriteLine($"  Total erros:    {errors:N0}");
        Console.WriteLine($"  Taxa média:     {avgRate:N1} msg/s");
        Console.WriteLine($"  Latência média: {avgLatency}ms");
        Console.WriteLine("═══════════════════════════════════════");
    }
}
