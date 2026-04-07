using Creditbus.Facade.Shared.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Features.CardsIngestion.Infrastructure;

public sealed class CardsIngestionKafkaConsumer : IKafkaMessageHandler
{
    private readonly ILogger<CardsIngestionKafkaConsumer> _logger;

    public CardsIngestionKafkaConsumer(ILogger<CardsIngestionKafkaConsumer> logger)
    {
        _logger = logger;
    }

    public string MessageType => "CardsIngestionEvent";

    public Task HandleAsync(string payload, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Received CardsIngestionEvent: {Payload}", payload);
        return Task.CompletedTask;
    }
}
