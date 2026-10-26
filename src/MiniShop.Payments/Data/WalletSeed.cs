using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Data;

/// <summary>The default storefront customer and their starting balance (Toman).</summary>
public static class WalletSeed
{
    public const string DemoCustomer = "demo-customer";
    public const long DemoBalance = 2_000_000;

    public static async Task SeedAsync(PaymentsDbContext db, CancellationToken ct)
    {
        if (!await db.Wallets.AnyAsync(w => w.CustomerId == DemoCustomer, ct))
            db.Wallets.Add(new Wallet { CustomerId = DemoCustomer, Balance = DemoBalance, Version = 1 });
        await db.SaveChangesAsync(ct);

        foreach (var wallet in await db.Wallets.ToListAsync(ct))
            db.PublishViaOutbox(BalanceChanged(wallet, null));
        await db.SaveChangesAsync(ct);
    }

    public static WalletBalanceChanged BalanceChanged(Wallet wallet, Message? causation) =>
        new(wallet.CustomerId, wallet.Balance)
        {
            CorrelationId = causation?.CorrelationId ?? Guid.Empty,
            CausationId = causation?.MessageId,
            ScenarioId = causation?.ScenarioId,
            Producer = ServiceNames.Payments,
        };
}
