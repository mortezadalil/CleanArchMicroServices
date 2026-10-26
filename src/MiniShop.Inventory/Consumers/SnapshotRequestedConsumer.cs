using MassTransit;
using MiniShop.Contracts;
using MiniShop.Inventory.Data;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Consumers;

/// <summary>A subscriber (the Web) asked for the catalog again: re-announce every product through the outbox.</summary>
public sealed class SnapshotRequestedConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<SnapshotRequestedConsumer> logger)
    : IdempotentConsumer<ReadModelSnapshotRequested, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReadModelSnapshotRequested> context)
    {
        await CatalogSeed.AnnounceCatalogAsync(Db, context.CancellationToken);
        return new HandleResult($"catalog re-announced for {context.Message.RequestedBy} ({context.Message.Reason})");
    }
}
