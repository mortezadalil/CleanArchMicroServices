// ==================================================================================================
// FILE: src/MiniShop.Payments/Consumers/SnapshotRequestedConsumer.cs
//
// WHAT THIS FILE IS
//   The handler for ReadModelSnapshotRequested: the Web says "my read model is empty or incomplete,
//   please announce your wallets again", and Payments re-publishes one WalletBalanceChanged per wallet.
//
// WHY IT EXISTS: THE LATE SUBSCRIBER PROBLEM
//   RabbitMQ copies a published event only into queues that are bound to its exchange AT THAT MOMENT;
//   with no bound queue the event is dropped.  The broker is not an event store.  On a first start,
//   Payments may seed and publish WalletBalanceChanged before the Web has created its queue, so the Web
//   would never see the balance.  The fix: the late subscriber asks the owner for a snapshot.  Projecting
//   WalletBalanceChanged is idempotent (it overwrites the balance), so answering more than once is harmless.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Web --ReadModelSnapshotRequested (published)--> Payments AND Inventory (each in its own queue)
//   Payments --WalletBalanceChanged × N (via the Outbox)--> Web
//
// Read next: Program.cs
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Data;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Consumers;

/// <summary>
/// A subscriber (the Web) asked for the wallets again: re-announce every balance through the outbox.
/// Inherits <see cref="IdempotentConsumer{TMessage,TDbContext}"/> like every consumer (Inbox de-duplication,
/// one transaction for the Inbox row and the outbox rows).
/// </summary>
/// <param name="db">The Payments database, a fresh instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit so the snapshot leaves immediately.</param>
/// <param name="faults">Fault-injection rules (no fault point is defined for this consumer).</param>
/// <param name="logger">Standard logger.</param>
public sealed class SnapshotRequestedConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<SnapshotRequestedConsumer> logger)
    : IdempotentConsumer<ReadModelSnapshotRequested, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>Queues one WalletBalanceChanged per wallet; the base class saves and commits.</summary>
    /// <param name="context">The delivered request (who asks and why).</param>
    /// <returns>A human-readable detail for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReadModelSnapshotRequested> context)
    {
        // All wallets (only a handful in the demo).
        var wallets = await Db.Wallets.ToListAsync(context.CancellationToken);
        // One event per wallet, through the Outbox (so the snapshot survives a broker hiccup).  causation null:
        // the announcement is not part of any order flow.
        foreach (var wallet in wallets)
            Db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, null));
        // Report how many balances were re-announced and to whom.
        return new HandleResult($"{wallets.Count} wallet balance(s) re-announced for {context.Message.RequestedBy}");
    }
}
