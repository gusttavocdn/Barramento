using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

namespace Creditbus.Facade.LoadTests;

public sealed class KafkaLoadPublisher
{
    private readonly MetricsTracker _metrics;

    public KafkaLoadPublisher(MetricsTracker metrics)
    {
        _metrics = metrics;
    }

    public async Task RunAsync(LoadTestOptions options, CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<PortfolioDataUpdatedEvent>(
            new BoundedChannelOptions(options.Rate * 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = false
            });

        var producerTask = ProduceAsync(channel.Writer, options.Rate, cancellationToken);

        var workerTasks = Enumerable.Range(0, options.Workers)
            .Select(_ => ConsumeAsync(channel.Reader, options.Broker, options.Topic, cancellationToken))
            .ToArray();

        await producerTask;
        channel.Writer.Complete();
        await Task.WhenAll(workerTasks);
    }

    private static async Task ProduceAsync(
        ChannelWriter<PortfolioDataUpdatedEvent> writer,
        int rate,
        CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                for (var i = 0; i < rate; i++)
                {
                    var msg = PayloadGenerator.Generate(Random.Shared);
                    await writer.WriteAsync(msg, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // encerramento gracioso — normal
        }
    }

    private async Task ConsumeAsync(
        ChannelReader<PortfolioDataUpdatedEvent> reader,
        string broker,
        string topic,
        CancellationToken cancellationToken)
    {
        var config = new ProducerConfig { BootstrapServers = broker };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        await foreach (var @event in reader.ReadAllAsync(cancellationToken))
        {
            var sw = Stopwatch.StartNew();
            try
            {
                var json = JsonSerializer.Serialize(@event);
                await producer.ProduceAsync(
                    topic,
                    new Message<string, string>
                    {
                        Key = @event.CorrelationId.ToString(),
                        Value = json
                    },
                    cancellationToken);

                sw.Stop();
                _metrics.RecordSuccess(
                    sw.ElapsedMilliseconds,
                    @event.CorrelationId,
                    @event.PortfolioDataUpdated.OperationId,
                    @event.PortfolioDataUpdated.CardHolderId.TradingAccount,
                    @event.PortfolioDataUpdated.Status,
                    @event.PortfolioDataUpdated.GlobalLimit);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _metrics.RecordError(@event.CorrelationId, ex.Message);
            }
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }
}
