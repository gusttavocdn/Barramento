using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Tests.Helpers;
using FluentAssertions;
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Tests.Features.CardsIngestion.Application;

public class ProcessCardEventUseCaseTests
{
    private static PortfolioDataUpdatedEvent MakeEvent(
        Guid? correlationId = null,
        long tradingAccount = 12345L,
        int brand = 2,
        long operationId = 99999L) => new(
        CorrelationId: correlationId ?? Guid.NewGuid(),
        CreatedAt: DateTime.UtcNow,
        EventOrigin: "test",
        PortfolioDataUpdated: new PortfolioDataUpdated(
            CardHolderId: new CardHolderId(tradingAccount, brand),
            OperationId: operationId,
            ProductId: 1,
            ProductDescription: "Gold",
            ReferenceDate: DateTime.UtcNow,
            Status: "Active",
            PaymentStatus: "Ok",
            UsedLimit: 0, GlobalLimit: 0, LastGlobalLimit: 0,
            MaximumCustomerLimit: 0, LimitExpected: 0,
            LimitExpectedUpdateDate: DateTime.UtcNow,
            CollateralLock: false, OverdueDays: 0, OverdueInvoiceBalance: 0,
            DueDate: 10, OverLimitGroupId: 0,
            InvoiceInstallmentPlanRequested: false,
            InvoicePaymentPercentage: 0, LastPaymentAmount: 0,
            LastInvoiceClosedValue: 0,
            LastInvoicePaidDate: DateTime.UtcNow,
            LastInvoiceClosedDate: DateTime.UtcNow,
            LastCollectionCanceledDate: DateTime.UtcNow,
            DateOfLastGlobalLimitIncrease: DateTime.UtcNow,
            CustomerAcquisitionDate: DateTime.UtcNow,
            MonthlyUsedLimit: new MonthlyUsedLimit(0, 0, 0, 0, 0, 0),
            LastInvoicesReceived: [],
            LastPaymentsReceived: [],
            InstallmentsInformation: new InstallmentsInformation(0, 0, 0, "none")
        )
    );

    [Fact]
    public async Task ExecuteAsync_LogsAtInformationLevel()
    {
        var logger = new CapturingLogger<ProcessCardEventUseCase>();
        var sut = new ProcessCardEventUseCase(logger);

        await sut.ExecuteAsync(MakeEvent(), CancellationToken.None);

        logger.Entries.Should().ContainSingle(e => e.Level == LogLevel.Information);
    }

    [Fact]
    public async Task ExecuteAsync_LogsCorrelationId()
    {
        var correlationId = Guid.NewGuid();
        var logger = new CapturingLogger<ProcessCardEventUseCase>();
        var sut = new ProcessCardEventUseCase(logger);

        await sut.ExecuteAsync(MakeEvent(correlationId: correlationId), CancellationToken.None);

        logger.Entries[0].Message.Should().Contain(correlationId.ToString());
    }

    [Fact]
    public async Task ExecuteAsync_LogsTradingAccount()
    {
        var logger = new CapturingLogger<ProcessCardEventUseCase>();
        var sut = new ProcessCardEventUseCase(logger);

        await sut.ExecuteAsync(MakeEvent(tradingAccount: 77777L), CancellationToken.None);

        logger.Entries[0].Message.Should().Contain("77777");
    }

    [Fact]
    public async Task ExecuteAsync_LogsBrand()
    {
        var logger = new CapturingLogger<ProcessCardEventUseCase>();
        var sut = new ProcessCardEventUseCase(logger);

        await sut.ExecuteAsync(MakeEvent(brand: 3), CancellationToken.None);

        logger.Entries[0].Message.Should().Contain("3");
    }

    [Fact]
    public async Task ExecuteAsync_LogsOperationId()
    {
        var logger = new CapturingLogger<ProcessCardEventUseCase>();
        var sut = new ProcessCardEventUseCase(logger);

        await sut.ExecuteAsync(MakeEvent(operationId: 88888L), CancellationToken.None);

        logger.Entries[0].Message.Should().Contain("88888");
    }
}
