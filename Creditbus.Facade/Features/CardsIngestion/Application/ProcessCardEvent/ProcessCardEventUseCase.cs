using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;

public sealed class ProcessCardEventUseCase : IProcessCardEventUseCase
{
    private readonly ILogger<ProcessCardEventUseCase> _logger;

    public ProcessCardEventUseCase(ILogger<ProcessCardEventUseCase> logger)
    {
        _logger = logger;
    }

    public Task ExecuteAsync(PortfolioDataUpdatedEvent @event, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "[ProcessCardEvent] CorrelationId={CorrelationId} | TradingAccount={TradingAccount} | Brand={Brand} | OperationId={OperationId}",
            @event.CorrelationId,
            @event.PortfolioDataUpdated.CardHolderId.TradingAccount,
            @event.PortfolioDataUpdated.CardHolderId.Brand,
            @event.PortfolioDataUpdated.OperationId);

        return Task.CompletedTask;
    }
}
