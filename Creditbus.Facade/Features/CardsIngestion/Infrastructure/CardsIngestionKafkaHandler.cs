using Creditbus.Facade.Shared.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Features.CardsIngestion.Infrastructure;

public sealed class CardsIngestionKafkaHandler : IKafkaMessageHandler
{
    private readonly ILogger<CardsIngestionKafkaHandler> _logger;

    public CardsIngestionKafkaHandler(ILogger<CardsIngestionKafkaHandler> logger)
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
