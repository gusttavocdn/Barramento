using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

namespace Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;

public interface IProcessCardEventUseCase
{
    Task ExecuteAsync(PortfolioDataUpdatedEvent @event, CancellationToken cancellationToken);
}
