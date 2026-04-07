using System.Text.Json;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using FluentAssertions;

namespace Creditbus.Facade.Tests.Features.CardsIngestion.Application;

public class PortfolioDataUpdatedEventTests
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    private const string ValidJson = """
    {
      "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "createdAt": "2026-01-01T00:00:00Z",
      "eventOrigin": "test-origin",
      "portfolioDataUpdated": {
        "cardHolderId": { "tradingAccount": 12345, "brand": 2 },
        "operationId": 99999,
        "productId": 1,
        "productDescription": "Gold Card",
        "referenceDate": "2026-01-01T00:00:00Z",
        "status": 1,
        "paymentStatus": "Ok",
        "usedLimit": 500.00,
        "globalLimit": 1000.00,
        "lastGlobalLimit": 900.00,
        "maximumCustomerLimit": 2000.00,
        "limitExpected": 1500.00,
        "limitExpectedUpdateDate": "2026-01-01T00:00:00Z",
        "collateralLock": false,
        "overdueDays": 0,
        "overdueInvoiceBalance": 0.00,
        "dueDate": 10,
        "overLimitGroupId": 0,
        "invoiceInstallmentPlanRequested": false,
        "invoicePaymentPercentage": 0.00,
        "lastPaymentAmount": 0.00,
        "lastInvoiceClosedValue": 0.00,
        "lastInvoicePaidDate": "2026-01-01T00:00:00Z",
        "lastInvoiceClosedDate": "2026-01-01T00:00:00Z",
        "lastCollectionCanceledDate": "2026-01-01T00:00:00Z",
        "dateOfLastGlobalLimitIncrease": "2026-01-01T00:00:00Z",
        "customerAcquisitionDate": "2026-01-01T00:00:00Z",
        "monthlyUsedLimit": {
          "lastOneMonthsUsedLimit": 0,
          "lastTwoMonthsUsedLimit": 0,
          "lastThreeMonthsUsedLimit": 0,
          "lastFourMonthsUsedLimit": 0,
          "lastFiveMonthsUsedLimit": 0,
          "lastSixMonthUsedLimit": 0
        },
        "lastInvoicesReceived": [],
        "lastPaymentsReceived": [],
        "installmentsInformation": {
          "installmentsQuantity": 3,
          "currentInstallment": 1,
          "installmentValue": 100.00,
          "type": "standard"
        }
      }
    }
    """;

    [Fact]
    public void Deserialize_MapsCorrelationId()
    {
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(ValidJson, Options)!;
        result.CorrelationId.Should().Be(Guid.Parse("3fa85f64-5717-4562-b3fc-2c963f66afa6"));
    }

    [Fact]
    public void Deserialize_MapsCardHolderId()
    {
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(ValidJson, Options)!;
        result.PortfolioDataUpdated.CardHolderId.TradingAccount.Should().Be(12345L);
        result.PortfolioDataUpdated.CardHolderId.Brand.Should().Be(2);
    }

    [Fact]
    public void Deserialize_MapsOperationId()
    {
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(ValidJson, Options)!;
        result.PortfolioDataUpdated.OperationId.Should().Be(99999L);
    }

    [Fact]
    public void Deserialize_ConvertsIntStatusToEnumName()
    {
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(ValidJson, Options)!;
        result.PortfolioDataUpdated.Status.Should().Be("Active");
    }

    [Fact]
    public void Deserialize_ConvertsStringStatusToEnumName()
    {
        var json = ValidJson.Replace("\"status\": 1", "\"status\": \"blocked\"");
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(json, Options)!;
        result.PortfolioDataUpdated.Status.Should().Be("Blocked");
    }
}
