namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public interface IKafkaMessageHandler
{
    string MessageType { get; }
    Task HandleAsync(string payload, CancellationToken cancellationToken);
}
