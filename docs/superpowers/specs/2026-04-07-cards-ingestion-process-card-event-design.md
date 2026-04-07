# CardsIngestion — ProcessCardEvent UseCase Design

**Date:** 2026-04-07
**Feature:** `CardsIngestion`
**Status:** Approved

---

## Overview

Implement the initial version of `ProcessCardEventUseCase`, which processes the `PortfolioDataUpdatedEvent` message received by `CardsIngestionKafkaConsumer`. In this first iteration, the use case logs the `CorrelationId` and the key customer-identifying fields from the contract.

---

## Folder Structure

```
Creditbus.Facade/
└── Features/
    └── CardsIngestion/
        ├── Domain/ 
        
        │   └── CardHolderStatus.cs                          # Enum: int ↔ status name
        │
        ├── Application/
        │   ├── Contracts/
        │   │   ├── PortfolioDataUpdatedEvent.cs             # Input DTO (root + nested types)
        │   │   └── CardHolderStatusConverter.cs             # System.Text.Json converter
        │   │
        │   └── ProcessCardEvent/
        │       ├── IProcessCardEventUseCase.cs              # Use case interface
        │       └── ProcessCardEventUseCase.cs               # Initial implementation (logging)
        │
        └── Infrastructure/
            └── CardsIngestionKafkaConsumer.cs               # Updated: deserialize + call use case
```

**Modified files:**
- `Creditbus.Facade/Shared/Infrastructure/Kafka/KafkaServiceExtensions.cs` — register `IProcessCardEventUseCase` and update consumer scope wiring

---

## Domain

### `CardHolderStatus.cs`

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

---

## Application

### `Contracts/CardHolderStatusConverter.cs`

A `System.Text.Json` `JsonConverter<string>` applied to the `Status` property of `PortfolioDataUpdated`. Handles two input forms:

- **`int`** → cast to `CardHolderStatus` → `.ToString()` (e.g., `1` → `"Active"`)
- **`string`** → `Enum.Parse<CardHolderStatus>(value, ignoreCase: true)` → `.ToString()`
- **Invalid value** → throws `JsonException` with a descriptive message including the received value

### `Contracts/PortfolioDataUpdatedEvent.cs`

Top-level record bound from the JSON root. All nested types are declared in the same file.

```csharp
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

Deserialization uses `System.Text.Json` with `JsonSerializerOptions` set to `PropertyNameCaseInsensitive = true`.

### `ProcessCardEvent/IProcessCardEventUseCase.cs`

```csharp
public interface IProcessCardEventUseCase
{
    Task ExecuteAsync(PortfolioDataUpdatedEvent @event, CancellationToken cancellationToken);
}
```

### `ProcessCardEvent/ProcessCardEventUseCase.cs`

Initial implementation logs the customer-identifying fields at `Information` level:

```
[ProcessCardEvent] CorrelationId={correlationId} | TradingAccount={tradingAccount} | Brand={brand} | OperationId={operationId}
```

Uses `ILogger<ProcessCardEventUseCase>` injected via constructor.

---

## Infrastructure

### `CardsIngestionKafkaConsumer.cs` (updated)

Constructor changes:
- Remove `ILogger<CardsIngestionKafkaConsumer>` (logging is now the use case's responsibility)
- Inject `IServiceScopeFactory` to resolve `Scoped` services per message

`HandleAsync` flow:
1. Create `IServiceScope` via `IServiceScopeFactory.CreateScope()`
2. Resolve `IProcessCardEventUseCase` from the scope
3. Deserialize payload to `PortfolioDataUpdatedEvent` using `System.Text.Json` with `PropertyNameCaseInsensitive = true`
4. Call `useCase.ExecuteAsync(@event, cancellationToken)`
5. Dispose scope

Deserialization failure (malformed JSON or invalid `status` value) propagates as an exception, which the Kafka retry/DLQ infrastructure handles automatically.

---

## DI Registration

In `KafkaServiceExtensions.AddKafkaInfrastructure()`:

```csharp
services.AddScoped<IProcessCardEventUseCase, ProcessCardEventUseCase>();
```

`Scoped` lifetime: each message processing cycle creates and disposes its own scope via `IServiceScopeFactory`, ensuring use case state is isolated per message.

---

## Verification

```bash
dotnet build Creditbus.Facade.sln   # 0 errors
dotnet test Creditbus.Facade.sln    # all existing tests still pass
```

Manual smoke test: publish a message to `creditbus.ingestion` with `message-type: CardsIngestionEvent` and a valid JSON body. The log line `[ProcessCardEvent] CorrelationId=... | TradingAccount=... | Brand=... | OperationId=...` must appear.
