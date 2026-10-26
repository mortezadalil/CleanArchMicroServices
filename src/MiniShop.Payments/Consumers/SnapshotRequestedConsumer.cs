using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Data;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Consumers;

/// <summary>A subscriber (the Web) asked for the wallets again: re-announce every balance through the outbox.</summary>
public sealed class SnapshotRequestedConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<SnapshotRequestedConsumer> logger)
    : IdempotentConsumer<ReadModelSnapshotRequested, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReadModelSnapshotRequested> context)
    {
        var wallets = await Db.Wallets.ToListAsync(context.CancellationToken);
        foreach (var wallet in wallets)
            Db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, null));
        return new HandleResult($"{wallets.Count} wallet balance(s) re-announced for {context.Message.RequestedBy}");
    }
}
