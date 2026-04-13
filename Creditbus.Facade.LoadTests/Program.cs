using Creditbus.Facade.LoadTests;

LoadTestOptions options;
try
{
    options = LoadTestOptions.Parse(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"Erro nos parâmetros: {ex.Message}");
    Console.Error.WriteLine();
    LoadTestOptions.PrintUsage();
    return 1;
}

Console.WriteLine("╔═══════════════════════════════════════╗");
Console.WriteLine("║     CREDITBUS KAFKA LOAD TEST         ║");
Console.WriteLine("╚═══════════════════════════════════════╝");
Console.WriteLine($"  Broker:   {options.Broker}");
Console.WriteLine($"  Tópico:   {options.Topic}");
Console.WriteLine($"  Taxa:     {options.Rate} msg/s");
Console.WriteLine($"  Workers:  {options.Workers}");
Console.WriteLine($"  Duração:  {(options.Duration.HasValue ? options.Duration.Value.ToString() : "até Ctrl+C")}");
Console.WriteLine();
Console.WriteLine("Iniciando... Pressione Ctrl+C para encerrar.");
Console.WriteLine();

using var cts = new CancellationTokenSource();

if (options.Duration.HasValue)
    cts.CancelAfter(options.Duration.Value);

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var startTime = DateTime.UtcNow;
var metrics = new MetricsTracker();
var publisher = new KafkaLoadPublisher(metrics);

var metricsTask = Task.Run(async () =>
{
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    try
    {
        while (await timer.WaitForNextTickAsync(cts.Token))
            metrics.PrintWindowMetrics();
    }
    catch (OperationCanceledException)
    {
        // encerramento gracioso — normal
    }
});

try
{
    await publisher.RunAsync(options, cts.Token);
}
catch (OperationCanceledException)
{
    // encerramento gracioso — normal
}

await metricsTask;
metrics.PrintSummary(startTime);

return 0;
