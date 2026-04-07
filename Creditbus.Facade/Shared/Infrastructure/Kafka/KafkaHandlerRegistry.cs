namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public sealed class KafkaHandlerRegistry
{
    private readonly Dictionary<string, IKafkaMessageHandler> _handlers;

    public KafkaHandlerRegistry(IEnumerable<IKafkaMessageHandler> handlers)
    {
        _handlers = new Dictionary<string, IKafkaMessageHandler>(StringComparer.OrdinalIgnoreCase);

        foreach (var handler in handlers)
        {
            if (!_handlers.TryAdd(handler.MessageType, handler))
                throw new InvalidOperationException(
                    $"Duplicate handler registered for message type '{handler.MessageType}'.");
        }
    }

    public IKafkaMessageHandler Resolve(string messageType)
    {
        if (_handlers.TryGetValue(messageType, out var handler))
            return handler;

        throw new InvalidOperationException(
            $"No handler registered for message type '{messageType}'. " +
            $"Registered types: [{string.Join(", ", _handlers.Keys)}]");
    }
}
