namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaConsumerRegistry
{
    private readonly Dictionary<string, IKafkaMessageConsumer> _handlers;

    public KafkaConsumerRegistry(IEnumerable<IKafkaMessageConsumer> handlers)
    {
        _handlers = new Dictionary<string, IKafkaMessageConsumer>(StringComparer.OrdinalIgnoreCase);

        foreach (var handler in handlers)
        {
            if (!_handlers.TryAdd(handler.MessageType, handler))
                throw new InvalidOperationException(
                    $"Duplicate handler registered for message type '{handler.MessageType}'.");
        }
    }

    public IKafkaMessageConsumer Resolve(string messageType)
    {
        if (_handlers.TryGetValue(messageType, out var handler))
            return handler;

        throw new InvalidOperationException(
            $"No handler registered for message type '{messageType}'. " +
            $"Registered types: [{string.Join(", ", _handlers.Keys)}]");
    }
}
