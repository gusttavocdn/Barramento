using Creditbus.Facade.Features.CardsIngestion.Application.Contracts;

namespace Creditbus.Facade.LoadTests;

public static class PayloadGenerator
{
    private static readonly string[] StatusPool =
    [
        "Eligible", "Active", "CreliqAgreement", "CanceledCustomer", "Blocked",
        "BlockedDefault", "Creliq", "Lost", "Released", "CanceledCpfExchange",
        "CanceledBankruptcyHolder", "BlockedBouncedCheck", "BillingOfficeAgreement",
        "LossAgreement", "Clearance", "Shutdown", "LossOfMargin", "Lawsuit",
        "MaternityLeave", "BlockedPrevention", "CanceledDefinitiveFraud",
        "BlockedPartner", "PrepaidBalanceToBeRecovered", "AccountNotActivated",
        "BlockedPreventiveFraud", "Inactive", "BlockedAutoFraud"
    ];

    private static readonly string[] PaymentStatusPool =
        ["Regular", "Inadimplente", "AcordoAtivo", "Bloqueado", "EmAcordo"];

    private static readonly string[] InstallmentTypePool =
        ["None", "Parcelado", "ParcelamentoFatura"];

    public static PortfolioDataUpdatedEvent Generate(Random rng)
    {
        var globalLimit = Math.Round((decimal)(rng.NextDouble() * (50_000 - 500) + 500), 2);
        var usedLimit = Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2);
        var lastGlobalLimit = Math.Round(globalLimit * (decimal)(0.8 + rng.NextDouble() * 0.4), 2);
        var maxLimit = Math.Round(globalLimit * (decimal)(1.0 + rng.NextDouble() * 0.5), 2);
        var now = DateTime.UtcNow;
        var twoYearsAgo = now.AddYears(-2);

        var overdueDays = rng.NextDouble() < 0.70 ? 0 : rng.Next(1, 366);
        var overdueBalance = overdueDays > 0
            ? Math.Round((decimal)(rng.NextDouble() * (double)usedLimit), 2)
            : 0m;

        var invoiceCount = rng.Next(1, 7);
        var paymentCount = rng.Next(0, 5);

        var installmentsQty = rng.Next(0, 13);
        var currentInstallment = installmentsQty > 0 ? rng.Next(1, installmentsQty + 1) : 0;
        var installmentValue = installmentsQty > 0
            ? Math.Round(usedLimit / installmentsQty, 2)
            : 0m;

        return new PortfolioDataUpdatedEvent(
            CorrelationId: Guid.NewGuid(),
            CreatedAt: now,
            EventOrigin: "CoreBanking",
            PortfolioDataUpdated: new PortfolioDataUpdated(
                CardHolderId: new CardHolderId(
                    TradingAccount: rng.NextInt64(1_000, 9_999_999),
                    Brand: 1
                ),
                OperationId: rng.NextInt64(100_000, 999_999_999),
                ProductId: 10,
                ProductDescription: "Cartão Crédito Padrão",
                ReferenceDate: RandomDate(rng, now.AddDays(-90), now),
                Status: PickStatus(rng),
                PaymentStatus: PaymentStatusPool[rng.Next(PaymentStatusPool.Length)],
                UsedLimit: usedLimit,
                GlobalLimit: globalLimit,
                LastGlobalLimit: lastGlobalLimit,
                MaximumCustomerLimit: maxLimit,
                LimitExpected: Math.Round(globalLimit * (decimal)(1.0 + rng.NextDouble() * 0.2), 2),
                LimitExpectedUpdateDate: now.AddDays(rng.Next(1, 90)),
                CollateralLock: rng.NextDouble() < 0.05,
                OverdueDays: overdueDays,
                OverdueInvoiceBalance: overdueBalance,
                DueDate: rng.Next(1, 29),
                OverLimitGroupId: 0,
                InvoiceInstallmentPlanRequested: rng.NextDouble() < 0.1,
                InvoicePaymentPercentage: Math.Round((decimal)(rng.NextDouble() * 0.9 + 0.1), 4),
                LastPaymentAmount: Math.Round((decimal)(rng.NextDouble() * (double)usedLimit), 2),
                LastInvoiceClosedValue: Math.Round((decimal)(rng.NextDouble() * (double)usedLimit), 2),
                LastInvoicePaidDate: RandomDate(rng, twoYearsAgo, now),
                LastInvoiceClosedDate: RandomDate(rng, twoYearsAgo, now),
                LastCollectionCanceledDate: RandomDate(rng, twoYearsAgo, now),
                DateOfLastGlobalLimitIncrease: RandomDate(rng, twoYearsAgo, now),
                CustomerAcquisitionDate: RandomDate(rng, now.AddYears(-10), now.AddYears(-1)),
                MonthlyUsedLimit: new MonthlyUsedLimit(
                    LastOneMonthsUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2),
                    LastTwoMonthsUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2),
                    LastThreeMonthsUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2),
                    LastFourMonthsUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2),
                    LastFiveMonthsUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2),
                    LastSixMonthUsedLimit: Math.Round((decimal)(rng.NextDouble() * (double)globalLimit), 2)
                ),
                LastInvoicesReceived: Enumerable.Range(1, invoiceCount)
                    .Select(i =>
                    {
                        var date = RandomDate(rng, twoYearsAgo, now);
                        return new InvoiceEntry(i, date, date.ToString("dd/MM/yyyy"),
                            Math.Round((decimal)(rng.NextDouble() * (double)globalLimit * 0.5 + 50), 2));
                    })
                    .ToList(),
                LastPaymentsReceived: Enumerable.Range(1, paymentCount)
                    .Select(i =>
                    {
                        var date = RandomDate(rng, twoYearsAgo, now);
                        return new PaymentEntry(i, date, date.ToString("dd/MM/yyyy"),
                            Math.Round((decimal)(rng.NextDouble() * (double)globalLimit * 0.5 + 50), 2));
                    })
                    .ToList(),
                InstallmentsInformation: new InstallmentsInformation(
                    InstallmentsQuantity: installmentsQty,
                    CurrentInstallment: currentInstallment,
                    InstallmentValue: installmentValue,
                    Type: installmentsQty > 0
                        ? InstallmentTypePool[rng.Next(1, InstallmentTypePool.Length)]
                        : "None"
                )
            )
        );
    }

    private static string PickStatus(Random rng)
    {
        if (rng.NextDouble() < 0.60)
            return "Active";
        return StatusPool[rng.Next(StatusPool.Length)];
    }

    private static DateTime RandomDate(Random rng, DateTime from, DateTime to)
    {
        var range = (to - from).TotalSeconds;
        return from.AddSeconds(rng.NextDouble() * range);
    }
}
