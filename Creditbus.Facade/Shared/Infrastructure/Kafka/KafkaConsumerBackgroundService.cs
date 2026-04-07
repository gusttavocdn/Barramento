using System.Text;
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
    private readonly KafkaHandlerRegistry _registry;
    private readonly KafkaDlqPublisher _dlqPublisher;
    private readonly string _topic;
    private readonly ResiliencePipeline _pipeline;
    private readonly ILogger<KafkaConsumerBackgroundService> _logger;

    public KafkaConsumerBackgroundService(
        IConsumer<string, string> consumer,
        KafkaHandlerRegistry registry,
        KafkaDlqPublisher dlqPublisher,
        IOptions<KafkaOptions> options,
        ILogger<KafkaConsumerBackgroundService> logger)
    {
        _consumer = consumer;
        _registry = registry;
        _dlqPublisher = dlqPublisher;
        _topic = options.Value.Topic;
        _logger = logger;
        _pipeline = BuildPipeline(options.Value.Retry);
    }

    private static ResiliencePipeline BuildPipeline(RetryOptions options)
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
                await ProcessMessageAsync(result, stoppingToken);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    internal async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken)
    {
        var messageTypeBytes = result.Message.Headers
            .FirstOrDefault(h => h.Key == "message-type")
            ?.GetValueBytes();

        if (messageTypeBytes is null)
        {
            _logger.LogWarning("Received message without 'message-type' header. Routing to DLQ.");
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                "Missing message-type header",
                cancellationToken);
            _consumer.Commit(result);
            return;
        }

        var messageType = Encoding.UTF8.GetString(messageTypeBytes);

        IKafkaMessageHandler handler;
        try
        {
            handler = _registry.Resolve(messageType);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Unrecognized message type '{MessageType}'. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
            _consumer.Commit(result);
            return;
        }

        try
        {
            await _pipeline.ExecuteAsync(
                async ct => await handler.HandleAsync(result.Message.Value, ct),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Handler for '{MessageType}' failed after all retry attempts. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
        }

        _consumer.Commit(result);
    }
}
