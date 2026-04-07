# CardsIngestion — ProcessCardEvent UseCase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `ProcessCardEventUseCase` that receives the `PortfolioDataUpdatedEvent` from the Kafka consumer, deserializes it (handling `status` as int or string via `CardHolderStatusConverter`), and logs the customer-identifying fields.

**Architecture:** `CardsIngestionKafkaConsumer` (Infrastructure) deserializes the raw JSON payload and delegates to `IProcessCardEventUseCase` (Application) resolved per-message through an `IServiceScopeFactory` scope. The use case logs `CorrelationId`, `TradingAccount`, `Brand`, and `OperationId`. The `CardHolderStatus` enum lives in Domain; its JSON converter lives in Application/Contracts alongside the input DTO.

**Tech Stack:** .NET 10, System.Text.Json, xUnit, NSubstitute, FluentAssertions

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `Creditbus.Facade/Features/CardsIngestion/Domain/CardHolderStatus.cs` | Enum int ↔ name |
| Create | `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/CardHolderStatusConverter.cs` | JsonConverter handling int and string |
| Create | `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/PortfolioDataUpdatedEvent.cs` | Full input DTO |
| Create | `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/IProcessCardEventUseCase.cs` | Interface |
| Create | `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/ProcessCardEventUseCase.cs` | Logging implementation |
| Modify | `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumer.cs` | Inject IServiceScopeFactory, deserialize, call use case |
| Modify | `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs` | Register IProcessCardEventUseCase as Scoped |
| Create | `Creditbus.Facade.Tests/Helpers/CapturingLogger.cs` | Test helper to capture log entries |
| Create | `Creditbus.Facade.Tests/Features/CardsIngestion/Application/CardHolderStatusConverterTests.cs` | Converter unit tests |
| Create | `Creditbus.Facade.Tests/Features/CardsIngestion/Application/PortfolioDataUpdatedEventTests.cs` | Deserialization tests |
| Create | `Creditbus.Facade.Tests/Features/CardsIngestion/Application/ProcessCardEventUseCaseTests.cs` | Use case unit tests |
| Create | `Creditbus.Facade.Tests/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumerTests.cs` | Consumer unit tests |

---

## Task 1: CardHolderStatus enum

**Files:**
- Create: `Creditbus.Facade/Features/CardsIngestion/Domain/CardHolderStatus.cs`

- [ ] **Step 1: Create `CardHolderStatus.cs`**

```csharp
namespace Creditbus.Facade.Features.CardsIngestion.Domain;

public enum CardHolderStatus
{
    Eligible,
    Active,
    CreliqAgreement,
    CanceledCustomer,
    Blocked,
    BlockedDefault,
    Creliq,
    Lost,
    Released,
    CanceledCpfExchange,
    CanceledBankruptcyHolder,
    BlockedBouncedCheck,
    BillingOfficeAgreement,
    LossAgreement,
    Clearance,
    Shutdown,
    LossOfMargin,
    Lawsuit,
    MaternityLeave,
    BlockedPrevention,
    CanceledDefinitiveFraud,
    BlockedPartner,
    PrepaidBalanceToBeRecovered,
    AccountNotActivated,
    BlockedPreventiveFraud,
    Inactive,
    BlockedAutoFraud
}
```

- [ ] **Step 2: Verify build**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet build Creditbus.Facade.sln
```

Expected: Build succeeded, 0 errors.

- [ ] **Step 3: Commit**

```bash
cd "C:\repos\BarramentoV3\Facade"
git add Creditbus.Facade/Features/CardsIngestion/Domain/CardHolderStatus.cs
git commit -m "feat: add CardHolderStatus domain enum"
```

---

## Task 2: CardHolderStatusConverter (TDD)

**Files:**
- Create: `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/CardHolderStatusConverter.cs`
- Create: `Creditbus.Facade.Tests/Features/CardsIngestion/Application/CardHolderStatusConverterTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Features/CardsIngestion/Application/CardHolderStatusConverterTests.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;
using FluentAssertions;

namespace Creditbus.Facade.Tests.Features.CardsIngestion.Application;

public class CardHolderStatusConverterTests
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    // Helper record to test the converter in isolation
    private record StatusWrapper(
        [property: JsonConverter(typeof(CardHolderStatusConverter))] string Status);

    private static string Deserialize(string json)
        => JsonSerializer.Deserialize<StatusWrapper>(json, Options)!.Status;

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsValidInt()
    {
        var result = Deserialize("""{"status": 0}""");
        result.Should().Be("Eligible");
    }

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsNonZeroInt()
    {
        var result = Deserialize("""{"status": 1}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_ReturnsEnumName_WhenValueIsValidString()
    {
        var result = Deserialize("""{"status": "Active"}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_IsCaseInsensitive_WhenValueIsString()
    {
        var result = Deserialize("""{"status": "active"}""");
        result.Should().Be("Active");
    }

    [Fact]
    public void Read_ThrowsJsonException_WhenIntValueIsOutOfRange()
    {
        var act = () => Deserialize("""{"status": 9999}""");
        act.Should().Throw<JsonException>().WithMessage("*9999*");
    }

    [Fact]
    public void Read_ThrowsJsonException_WhenStringValueIsInvalid()
    {
        var act = () => Deserialize("""{"status": "InvalidStatus"}""");
        act.Should().Throw<JsonException>().WithMessage("*InvalidStatus*");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~CardHolderStatusConverterTests" 2>&1 | head -20
```

Expected: Compilation error — `CardHolderStatusConverter` does not exist yet.

- [ ] **Step 3: Implement `CardHolderStatusConverter`**

Create `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/CardHolderStatusConverter.cs`:

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Creditbus.Facade.Features.CardsIngestion.Domain;

namespace Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

public sealed class CardHolderStatusConverter : JsonConverter<string>
{
    public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Number)
        {
            var value = reader.GetInt32();
            if (!Enum.IsDefined(typeof(CardHolderStatus), value))
                throw new JsonException($"Invalid CardHolderStatus integer value: {value}.");
            return ((CardHolderStatus)value).ToString();
        }

        if (reader.TokenType == JsonTokenType.String)
        {
            var value = reader.GetString()!;
            if (!Enum.TryParse<CardHolderStatus>(value, ignoreCase: true, out var result))
                throw new JsonException($"Invalid CardHolderStatus string value: '{value}'.");
            return result.ToString();
        }

        throw new JsonException($"Unexpected token type for CardHolderStatus: {reader.TokenType}.");
    }

    public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        => writer.WriteStringValue(value);
}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~CardHolderStatusConverterTests"
```

Expected: 6 tests passed.

- [ ] **Step 5: Commit**

```bash
cd "C:\repos\BarramentoV3\Facade"
git add Creditbus.Facade/Features/CardsIngestion/Application/Contracts/CardHolderStatusConverter.cs \
        Creditbus.Facade.Tests/Features/CardsIngestion/Application/CardHolderStatusConverterTests.cs
git commit -m "feat: add CardHolderStatusConverter for int/string status deserialization"
```

---

## Task 3: PortfolioDataUpdatedEvent contract (TDD)

**Files:**
- Create: `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/PortfolioDataUpdatedEvent.cs`
- Create: `Creditbus.Facade.Tests/Features/CardsIngestion/Application/PortfolioDataUpdatedEventTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Features/CardsIngestion/Application/PortfolioDataUpdatedEventTests.cs`:

```csharp
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
        var json = ValidJson.Replace("""\"status\": 1""", """\"status\": \"blocked\"""");
        var result = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(json, Options)!;
        result.PortfolioDataUpdated.Status.Should().Be("Blocked");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~PortfolioDataUpdatedEventTests" 2>&1 | head -20
```

Expected: Compilation error — `PortfolioDataUpdatedEvent` does not exist yet.

- [ ] **Step 3: Implement `PortfolioDataUpdatedEvent.cs`**

Create `Creditbus.Facade/Features/CardsIngestion/Application/Contracts/PortfolioDataUpdatedEvent.cs`:

```csharp
using System.Text.Json.Serialization;

namespace Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

public record PortfolioDataUpdatedEvent(
    Guid CorrelationId,
    DateTime CreatedAt,
    string EventOrigin,
    PortfolioDataUpdated PortfolioDataUpdated
);

public record PortfolioDataUpdated(
    CardHolderId CardHolderId,
    long OperationId,
    int ProductId,
    string ProductDescription,
    DateTime ReferenceDate,
    [property: JsonConverter(typeof(CardHolderStatusConverter))] string Status,
    string PaymentStatus,
    decimal UsedLimit,
    decimal GlobalLimit,
    decimal LastGlobalLimit,
    decimal MaximumCustomerLimit,
    decimal LimitExpected,
    DateTime LimitExpectedUpdateDate,
    bool CollateralLock,
    int OverdueDays,
    decimal OverdueInvoiceBalance,
    int DueDate,
    int OverLimitGroupId,
    bool InvoiceInstallmentPlanRequested,
    decimal InvoicePaymentPercentage,
    decimal LastPaymentAmount,
    decimal LastInvoiceClosedValue,
    DateTime LastInvoicePaidDate,
    DateTime LastInvoiceClosedDate,
    DateTime LastCollectionCanceledDate,
    DateTime DateOfLastGlobalLimitIncrease,
    DateTime CustomerAcquisitionDate,
    MonthlyUsedLimit MonthlyUsedLimit,
    List<InvoiceEntry> LastInvoicesReceived,
    List<PaymentEntry> LastPaymentsReceived,
    InstallmentsInformation InstallmentsInformation
);

public record CardHolderId(long TradingAccount, int Brand);

public record MonthlyUsedLimit(
    decimal LastOneMonthsUsedLimit,
    decimal LastTwoMonthsUsedLimit,
    decimal LastThreeMonthsUsedLimit,
    decimal LastFourMonthsUsedLimit,
    decimal LastFiveMonthsUsedLimit,
    decimal LastSixMonthUsedLimit
);

public record InvoiceEntry(int Index, DateTime ActualDueDate, string DateFormatted, decimal Value);

public record PaymentEntry(int Index, DateTime Date, string DateFormatted, decimal Value);

public record InstallmentsInformation(
    int InstallmentsQuantity,
    int CurrentInstallment,
    decimal InstallmentValue,
    string Type
);
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~PortfolioDataUpdatedEventTests"
```

Expected: 5 tests passed.

- [ ] **Step 5: Commit**

```bash
cd "C:\repos\BarramentoV3\Facade"
git add Creditbus.Facade/Features/CardsIngestion/Application/Contracts/PortfolioDataUpdatedEvent.cs \
        Creditbus.Facade.Tests/Features/CardsIngestion/Application/PortfolioDataUpdatedEventTests.cs
git commit -m "feat: add PortfolioDataUpdatedEvent input DTO"
```

---

## Task 4: IProcessCardEventUseCase + ProcessCardEventUseCase (TDD)

**Files:**
- Create: `Creditbus.Facade.Tests/Helpers/CapturingLogger.cs`
- Create: `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/IProcessCardEventUseCase.cs`
- Create: `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/ProcessCardEventUseCase.cs`
- Create: `Creditbus.Facade.Tests/Features/CardsIngestion/Application/ProcessCardEventUseCaseTests.cs`

- [ ] **Step 1: Create the `CapturingLogger` test helper**

Create `Creditbus.Facade.Tests/Helpers/CapturingLogger.cs`:

```csharp
using Microsoft.Extensions.Logging;

namespace Creditbus.Facade.Tests.Helpers;

public sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];
    public IReadOnlyList<(LogLevel Level, string Message)> Entries => _entries;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        _entries.Add((logLevel, formatter(state, exception)));
    }
}
```

- [ ] **Step 2: Write the failing tests**

Create `Creditbus.Facade.Tests/Features/CardsIngestion/Application/ProcessCardEventUseCaseTests.cs`:

```csharp
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
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~ProcessCardEventUseCaseTests" 2>&1 | head -20
```

Expected: Compilation error — `IProcessCardEventUseCase` and `ProcessCardEventUseCase` do not exist yet.

- [ ] **Step 4: Create the interface**

Create `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/IProcessCardEventUseCase.cs`:

```csharp
using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

namespace Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;

public interface IProcessCardEventUseCase
{
    Task ExecuteAsync(PortfolioDataUpdatedEvent @event, CancellationToken cancellationToken);
}
```

- [ ] **Step 5: Create the implementation**

Create `Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/ProcessCardEventUseCase.cs`:

```csharp
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
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~ProcessCardEventUseCaseTests"
```

Expected: 5 tests passed.

- [ ] **Step 7: Commit**

```bash
cd "C:\repos\BarramentoV3\Facade"
git add Creditbus.Facade.Tests/Helpers/CapturingLogger.cs \
        Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/IProcessCardEventUseCase.cs \
        Creditbus.Facade/Features/CardsIngestion/Application/ProcessCardEvent/ProcessCardEventUseCase.cs \
        Creditbus.Facade.Tests/Features/CardsIngestion/Application/ProcessCardEventUseCaseTests.cs
git commit -m "feat: add ProcessCardEventUseCase with customer-identifying log"
```

---

## Task 5: Update CardsIngestionKafkaConsumer + DI wiring (TDD)

**Files:**
- Modify: `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumer.cs`
- Modify: `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs`
- Create: `Creditbus.Facade.Tests/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumerTests.cs`

- [ ] **Step 1: Write the failing tests**

Create `Creditbus.Facade.Tests/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumerTests.cs`:

```csharp
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

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IProcessCardEventUseCase)).Returns(useCase);

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

        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public void MessageType_IsCardsIngestionEvent()
    {
        var (sut, _) = BuildSut();
        sut.MessageType.Should().Be("CardsIngestionEvent");
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~CardsIngestionKafkaConsumerTests" 2>&1 | head -20
```

Expected: Compilation error — constructor signature does not match yet.

- [ ] **Step 3: Update `CardsIngestionKafkaConsumer.cs`**

Replace the full content of `Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumer.cs`:

```csharp
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
        var @event = JsonSerializer.Deserialize<PortfolioDataUpdatedEvent>(payload, JsonOptions)!;
        await useCase.ExecuteAsync(@event, cancellationToken);
    }
}
```

- [ ] **Step 4: Update `KafkaServiceExtensions.cs`**

Add `services.AddScoped<IProcessCardEventUseCase, ProcessCardEventUseCase>();` and the required using statements. The full updated file:

```csharp
using Confluent.Kafka;
using Creditbus.Facade.Features.CardsIngestion.Application.ProcessCardEvent;
using Creditbus.Facade.Features.CardsIngestion.Infrastructure;
using Microsoft.Extensions.Options;

namespace Creditbus.Facade.Shared.Infrastructure.Kafka;

public static class KafkaServiceExtensions
{
    public static IServiceCollection AddKafkaInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ProducerConfig { BootstrapServers = options.BootstrapServers };
            return new ProducerBuilder<string, string>(config).Build();
        });

        services.AddSingleton<IConsumer<string, string>>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                GroupId = options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            return new ConsumerBuilder<string, string>(config).Build();
        });

        services.AddScoped<IProcessCardEventUseCase, ProcessCardEventUseCase>();
        services.AddSingleton<IKafkaMessageHandler, CardsIngestionKafkaConsumer>();
        services.AddSingleton<KafkaHandlerRegistry>();
        services.AddSingleton<KafkaDlqPublisher>();
        services.AddHostedService<KafkaConsumerBackgroundService>();

        return services;
    }
}
```

- [ ] **Step 5: Run the new tests**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln --filter "FullyQualifiedName~CardsIngestionKafkaConsumerTests"
```

Expected: 3 tests passed.

- [ ] **Step 6: Run all tests**

```bash
cd "C:\repos\BarramentoV3\Facade"
dotnet test Creditbus.Facade.sln
```

Expected: All tests pass (previous 14 + new 19 = 33 total).

- [ ] **Step 7: Commit**

```bash
cd "C:\repos\BarramentoV3\Facade"
git add Creditbus.Facade/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumer.cs \
        Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs \
        Creditbus.Facade.Tests/Features/CardsIngestion/Infrastructure/CardsIngestionKafkaConsumerTests.cs
git commit -m "feat: wire ProcessCardEventUseCase into CardsIngestionKafkaConsumer"
```
