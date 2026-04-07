using System.Text.Json;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace Creditbus.Facade.Features.CardsIngestion.Infrastructure;

public sealed class CardsIngestionKafkaConsumer : IKafkaMessageHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly IServiceScopeFactory _scopeFactory;

    public CardsIngestionKafkaConsumer(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory;
    }

    public string MessageType => "CardsIngestionEvent";

    public async Task HandleAsync(string payload, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var useCase = scope.ServiceProvider.GetRequiredService<IProcessCardEventUseCase>();
        var @event = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(payload, JsonOptions)
            ?? throw new JsonException("Deserialized event was null.");
        await useCase.ExecuteAsync(@event, cancellationToken);
    }
}
