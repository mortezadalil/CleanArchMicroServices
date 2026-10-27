// ==================================================================================================
// FILE: src/MiniShop.Payments/Data/WalletSeed.cs
//
// WHAT THIS FILE IS
//   The demo data of the Payments service (one customer with 2,000,000 Toman) and the helper that builds
//   the WalletBalanceChanged event announcing a wallet's balance to the rest of the system.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Payments owns the balances; the Web only shows a read model built from WalletBalanceChanged events.
//   Those events always leave through the Transactional Outbox.  Used by:
//     - Program.cs on start-up and POST /demo/reset   (SeedAsync)
//     - ChargeWalletConsumer after a debit           (BalanceChanged)
//     - SnapshotRequestedConsumer for a late subscriber, and POST /demo/wallets   (BalanceChanged)
//
// Read next: Consumers/ChargeWalletConsumer.cs (the heart of the service).
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Data;

/// <summary>The default storefront customer and their starting balance (Toman).</summary>
public static class WalletSeed
{
    /// <summary>Fixed id of the customer the storefront uses (there is no login in the demo).</summary>
    public const string DemoCustomer = "demo-customer";

    /// <summary>Starting balance after a seed or reset: enough for a few orders, so S01 succeeds out of the box.</summary>
    public const long DemoBalance = 2_000_000;

    /// <summary>
    /// Creates the demo wallet if missing, then announces EVERY wallet's balance through the outbox so the
    /// Web's read model can (re)build its wallet summary.  Idempotent: a restart never resets a balance.
    /// </summary>
    /// <param name="db">The Payments database context.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task SeedAsync(PaymentsDbContext db, CancellationToken ct)
    {
        // Insert only when missing, so balances that changed during the demo survive a restart.
        if (!await db.Wallets.AnyAsync(w => w.CustomerId == DemoCustomer, ct))
            db.Wallets.Add(new Wallet { CustomerId = DemoCustomer, Balance = DemoBalance, Version = 1 });
        // Save first, so the query below sees the new wallet.
        await db.SaveChangesAsync(ct);

        // One WalletBalanceChanged per wallet (causation null: nobody's message caused this announcement).
        foreach (var wallet in await db.Wallets.ToListAsync(ct))
            db.PublishViaOutbox(BalanceChanged(wallet, null));
        // Save the outbox rows; the OutboxDispatcher publishes them (right away if the caller calls Notify()).
        await db.SaveChangesAsync(ct);
    }

    /// <summary>Builds the WalletBalanceChanged event for one wallet.</summary>
    /// <param name="wallet">The wallet whose CURRENT balance is announced.</param>
    /// <param name="causation">The message that caused the change (e.g. ChargeWallet), or null for seed / snapshot / manual changes.</param>
    public static WalletBalanceChanged BalanceChanged(Wallet wallet, Message? causation) =>
        new(wallet.CustomerId, wallet.Balance)
        {
            // With a cause: the order's id, so the balance change appears in that order's timeline.
            // Without one: Guid.Empty, i.e. "not part of any order flow".
            CorrelationId = causation?.CorrelationId ?? Guid.Empty,
            // Parent link in the message chain ("this event was caused by that command").
            CausationId = causation?.MessageId,
            // Keep the Scenario Lab experiment id so the UI shows the event inside the experiment.
            ScenarioId = causation?.ScenarioId,
            // Who produced the event.
            Producer = ServiceNames.Payments,
        };
}
