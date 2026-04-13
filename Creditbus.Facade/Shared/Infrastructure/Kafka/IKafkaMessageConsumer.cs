namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public interface IKafkaMessageConsumer
{
    string MessageType { get; }
    Task HandleAsync(string payload, CancellationToken cancellationToken);
}
