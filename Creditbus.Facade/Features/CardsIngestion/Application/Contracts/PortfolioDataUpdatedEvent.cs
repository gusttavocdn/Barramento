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
