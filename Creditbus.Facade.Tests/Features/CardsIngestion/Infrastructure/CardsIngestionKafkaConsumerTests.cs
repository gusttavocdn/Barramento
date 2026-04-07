using System.Text.Json;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Features.CardsIngestion.Infrastructure;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;

namespace Creditbus.Facade.Tests.Features.CardsIngestion.Infrastructure;

public class CardsIngestionKafkaConsumerTests
{
    private const string ValidPayload = """
    {
      "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "createdAt": "2026-01-01T00:00:00Z",
      "eventOrigin": "test",
      "portfolioDataUpdated": {
        "cardHolderId": { "tradingAccount": 12345, "brand": 2 },
        "operationId": 99999,
        "productId": 1,
        "productDescription": "Gold",
        "referenceDate": "2026-01-01T00:00:00Z",
        "status": 1,
        "paymentStatus": "Ok",
        "usedLimit": 0, "globalLimit": 0, "lastGlobalLimit": 0,
        "maximumCustomerLimit": 0, "limitExpected": 0,
        "limitExpectedUpdateDate": "2026-01-01T00:00:00Z",
        "collateralLock": false, "overdueDays": 0, "overdueInvoiceBalance": 0,
        "dueDate": 10, "overLimitGroupId": 0,
        "invoiceInstallmentPlanRequested": false,
        "invoicePaymentPercentage": 0, "lastPaymentAmount": 0,
        "lastInvoiceClosedValue": 0,
        "lastInvoicePaidDate": "2026-01-01T00:00:00Z",
        "lastInvoiceClosedDate": "2026-01-01T00:00:00Z",
        "lastCollectionCanceledDate": "2026-01-01T00:00:00Z",
        "dateOfLastGlobalLimitIncrease": "2026-01-01T00:00:00Z",
        "customerAcquisitionDate": "2026-01-01T00:00:00Z",
        "monthlyUsedLimit": {
          "lastOneMonthsUsedLimit": 0, "lastTwoMonthsUsedLimit": 0,
          "lastThreeMonthsUsedLimit": 0, "lastFourMonthsUsedLimit": 0,
          "lastFiveMonthsUsedLimit": 0, "lastSixMonthUsedLimit": 0
        },
        "lastInvoicesReceived": [],
        "lastPaymentsReceived": [],
        "installmentsInformation": {
          "installmentsQuantity": 0, "currentInstallment": 0,
          "installmentValue": 0, "type": "none"
        }
      }
    }
    """;

    private static (CardsIngestionKafkaConsumer sut, IProcessCardEventUseCase useCase) BuildSut()
    {
        var useCase = Substitute.For<IProcessCardEventUseCase>();
        useCase.ExecuteAsync(Arg.Any<PortfolioDataUpdatedEvent>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddSingleton(useCase);
        var serviceProvider = services.BuildServiceProvider();

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);

        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return (new CardsIngestionKafkaConsumer(scopeFactory), useCase);
    }

    [Fact]
    public async Task HandleAsync_CallsUseCaseWithDeserializedEvent()
    {
        var (sut, useCase) = BuildSut();

        await sut.HandleAsync(ValidPayload, CancellationToken.None);

        await useCase.Received(1).ExecuteAsync(
            Arg.Is<PortfolioDataUpdatedEvent>(e =>
                e.CorrelationId == Guid.Parse("3fa85f64-5717-4562-b3fc-2c963f66afa6") &&
                e.PortfolioDataUpdated.CardHolderId.TradingAccount == 12345L &&
                e.PortfolioDataUpdated.OperationId == 99999L),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HandleAsync_ThrowsJsonException_WhenPayloadIsMalformed()
    {
        var (sut, _) = BuildSut();

        var act = async () => await sut.HandleAsync("not valid json", CancellationToken.None);

        await act.Should().ThrowAsync<JsonException>();
    }

    [Fact]
    public void MessageType_IsCardsIngestionEvent()
    {
        var (sut, _) = BuildSut();
        sut.MessageType.Should().Be("CardsIngestionEvent");
    }
}
