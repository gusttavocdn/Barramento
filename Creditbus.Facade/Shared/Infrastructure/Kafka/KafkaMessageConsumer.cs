using System.Text.Json;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public abstract class KafkaMessageConsumer<T> : IKafkaMessageConsumer
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public abstract string MessageType { get; }

    public async Task HandleAsync(string payload, CancellationToken cancellationToken)
    {
        var message = JsonSerializer.Deserialize<T>(payload, JsonOptions)
            ?? throw new JsonException($"Deserialized message of type '{typeof(T).Name}' was null.");

        await HandleAsync(message, cancellationToken);
    }

    protected abstract Task HandleAsync(T message, CancellationToken cancellationToken);
}
