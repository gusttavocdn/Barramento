using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Polly;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class PartitionWorker : IPartitionWorker
{
    private readonly TopicPartition _partition;
    private readonly IConsumer<string, string> _consumer;
    private readonly Channel<ConsumeResult<string, string>> _channel;
    private readonly KafkaHandlerRegistry _handlerRegistry;
    private readonly KafkaDlqPublisher _dlqPublisher;
    private readonly ResiliencePipeline _pipeline;
    private readonly ILogger<PartitionWorker> _logger;
    private Task _loopTask = Task.CompletedTask;

    public PartitionWorker(
        TopicPartition partition,
        IConsumer<string, string> consumer,
        int channelCapacity,
        KafkaHandlerRegistry handlerRegistry,
        KafkaDlqPublisher dlqPublisher,
        ResiliencePipeline pipeline,
        ILogger<PartitionWorker> logger)
    {
        _partition = partition;
        _consumer = consumer;
        _handlerRegistry = handlerRegistry;
        _dlqPublisher = dlqPublisher;
        _pipeline = pipeline;
        _logger = logger;
        _channel = Channel.CreateBounded<ConsumeResult<string, string>>(
            new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = true
            });
    }

    public void Start() =>
        _loopTask = ProcessLoopAsync();

    public async Task StopAsync()
    {
        _channel.Writer.TryComplete();
        await _loopTask;
    }

    public ValueTask EnqueueAsync(ConsumeResult<string, string> result, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(result, cancellationToken);

    private async Task ProcessLoopAsync()
    {
        while (true)
        {
            try
            {
                await foreach (var result in _channel.Reader.ReadAllAsync())
                    await ProcessMessageAsync(result);

                break; // channel completado — parada limpa
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex,
                    "PartitionWorker for {Partition} crashed unexpectedly. Restarting in 1s.", _partition);
                await Task.Delay(1000);
            }
        }
    }

    internal async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken = default)
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
            _consumer.StoreOffset(result);
            return;
        }

        var messageType = Encoding.UTF8.GetString(messageTypeBytes);

        IKafkaMessageHandler handler;
        try
        {
            handler = _handlerRegistry.Resolve(messageType);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Unrecognized message type '{MessageType}'. Routing to DLQ.", messageType);
            await _dlqPublisher.PublishAsync(
                result.Message.Value,
                result.Message.Headers,
                ex.Message,
                cancellationToken);
            _consumer.StoreOffset(result);
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

        _consumer.StoreOffset(result);
    }
}