using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaConsumerBackgroundService : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly PartitionWorkerRegistry _workerRegistry;
    private readonly string _topic;
    private readonly ILogger<KafkaConsumerBackgroundService> _logger;

    public KafkaConsumerBackgroundService(
        IConsumer<string, string> consumer,
        PartitionWorkerRegistry workerRegistry,
        IOptions<KafkaOptions> options,
        ILogger<KafkaConsumerBackgroundService> logger)
    {
        _consumer = consumer;
        _workerRegistry = workerRegistry;
        _topic = options.Value.Topic;
        _logger = logger;
    }

    internal static ResiliencePipeline BuildPipeline(RetryOptions options)
    {
        var builder = new ResiliencePipelineBuilder();

        if (options.MaxAttempts > 1)
        {
            builder.AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = options.MaxAttempts - 1,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                DelayGenerator = args =>
                {
                    double baseMs = Math.Min(
                        options.InitialDelayMs * Math.Pow(2, args.AttemptNumber),
                        options.MaxDelayMs);
                    double jitter = (Random.Shared.NextDouble() * 2 - 1) * baseMs * options.JitterFactor;
                    return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromMilliseconds(baseMs + jitter));
                }
            });
        }

        return builder.Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(stoppingToken);

                if (!_workerRegistry.TryGet(result.TopicPartition, out var worker))
                {
                    _logger.LogError(
                        "No worker registered for partition {Partition}. Message at offset {Offset} will be skipped.",
                        result.TopicPartition, result.Offset);
                    continue;
                }

                await worker!.EnqueueAsync(result, stoppingToken);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }
}