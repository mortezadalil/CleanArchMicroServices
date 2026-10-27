// ==================================================================================================
// FILE: src/MiniShop.Inventory/Consumers/SnapshotRequestedConsumer.cs
//
// WHAT THIS FILE IS
//   The handler for ReadModelSnapshotRequested: the Web says "my read model is empty or incomplete,
//   please announce your catalog again", and Inventory re-publishes one ProductCatalogChanged per product.
//
// WHY IT EXISTS: THE LATE SUBSCRIBER PROBLEM
//   RabbitMQ is a message broker, not an event store.  When an event is published, the broker copies it
//   only into the queues that are bound to the event's exchange AT THAT MOMENT.  If no queue is bound,
//   the event is simply dropped.  On a first start, Inventory seeds its catalog and publishes
//   ProductCatalogChanged possibly BEFORE the Web has created and bound its queue, so the Web would never
//   learn about the products.  The fix: the subscriber (Web) asks the owner for a snapshot, and the owner
//   answers with the full current state.  Because projecting ProductCatalogChanged is idempotent (it just
//   overwrites the product card), answering several times is harmless.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Web --ReadModelSnapshotRequested (published event)--> Inventory AND Payments (each in its own queue)
//   Inventory --ProductCatalogChanged × N (published via the Outbox)--> Web
//
// Read next: Program.cs (how all these consumers are wired to RabbitMQ).
// ==================================================================================================

using MassTransit;
using MiniShop.Contracts;
using MiniShop.Inventory.Data;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Consumers;

/// <summary>
/// A subscriber (the Web) asked for the catalog again: re-announce every product through the outbox.
/// It inherits <see cref="IdempotentConsumer{TMessage,TDbContext}"/> like every consumer, so the request is
/// de-duplicated by MessageId and the outbox rows are committed atomically with the Inbox row.
/// </summary>
/// <param name="db">The Inventory database, one instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit so the snapshot leaves immediately.</param>
/// <param name="faults">Fault-injection rules (no fault point is defined for this consumer).</param>
/// <param name="logger">Standard logger.</param>
public sealed class SnapshotRequestedConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<SnapshotRequestedConsumer> logger)
    : IdempotentConsumer<ReadModelSnapshotRequested, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>Queues one ProductCatalogChanged per product; the base class saves and commits.</summary>
    /// <param name="context">The delivered request (who asks and why).</param>
    /// <returns>A human-readable detail for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReadModelSnapshotRequested> context)
    {
        // Add an outbox row per product to the current transaction (AnnounceCatalogAsync does not save).
        // Going through the Outbox (instead of publishing directly) means the snapshot is not lost if
        // RabbitMQ is briefly unavailable: the dispatcher keeps retrying until it is delivered.
        await CatalogSeed.AnnounceCatalogAsync(Db, context.CancellationToken);
        // Report who asked and why (e.g. "Web (read model empty on start-up)").
        return new HandleResult($"catalog re-announced for {context.Message.RequestedBy} ({context.Message.Reason})");
    }
}
