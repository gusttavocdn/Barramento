using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Shared.Infrastructure.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace Creditbus.Facade.Features.CardsIngestion.Infrastructure;

public sealed class CardsIngestionKafkaConsumer : KafkaMessageConsumer<PortfolioDataUpdatedEvent>
{
	private readonly IServiceScopeFactory _scopeFactory;

	public CardsIngestionKafkaConsumer(IServiceScopeFactory scopeFactory)
	{
		_scopeFactory = scopeFactory;
	}

	public override string MessageType => "CardsIngestionEvent";

	protected override async Task HandleAsync(PortfolioDataUpdatedEvent message, CancellationToken cancellationToken)
	{
		using var scope = _scopeFactory.CreateScope();
		var useCase = scope.ServiceProvider.GetRequiredService<IProcessCardEventUseCase>();
		await useCase.ExecuteAsync(message, cancellationToken);
	}
}
