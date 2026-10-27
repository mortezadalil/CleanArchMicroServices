// ==================================================================================================
// FILE: src/MiniShop.Web/Projections/ReadModelRebuilder.cs
//
// WHAT IT IS
//   The "Reset / Rebuild Read Model" operation of the CQRS Explorer: throw the projection tables away
//   and rebuild them from scratch by replaying the business events stored in EventAudit.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   CQRS read side.  It demonstrates the key promise of a read model: it is DISPOSABLE.  The truth lives
//   in the owner services; the read model is just a view that can always be recomputed from events.
//
// WHY REPLAY FROM EventAudit AND NOT FROM RabbitMQ?
//   RabbitMQ is a message broker, not an event store.  A message disappears from the queue once it is
//   acknowledged, and there is no "give me all events since the beginning".  So the Web keeps its own
//   append-only copy of every business event (EventAudit) and replays that.  (A real system might use
//   an event store or a log such as Kafka for this.)
//
// Read next: Projector.cs (the same code is used here and live), then ReadModelBootstrapper.cs.
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Messaging;
using MiniShop.Web.Data;

namespace MiniShop.Web.Projections;

/// <summary>
/// "Reset / Rebuild Read Model": wipes the projection tables and replays every BUSINESS event from
/// EventAudit in the order it was received.  RabbitMQ queues are not a durable event store, so the
/// append-only audit table is the replay source.  Because the projector is idempotent, the result
/// equals the live projection.
/// Registered as SCOPED (it shares the scoped DbContext with the Projector it uses).
/// </summary>
public sealed class ReadModelRebuilder(ReadModelDbContext db, Projector projector, UiNotifier notifier, ILogger<ReadModelRebuilder> logger)
{
    /// <summary>Rebuilds the projection tables and returns how many events were replayed.</summary>
    public async Task<int> RebuildAsync(CancellationToken ct)
    {
        // One transaction for the whole rebuild: readers never see half-empty tables committed, and if
        // anything fails the old read model is kept intact (rollback on dispose).
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        // Delete the PROJECTION tables only.  ExecuteDelete runs a direct SQL DELETE (no loading of rows).
        // EventAudit, the ProjectionInbox and the scenario tables are NOT touched: EventAudit is the
        // source we are about to replay, and the inbox must keep remembering processed messages.
        await db.ProductCards.ExecuteDeleteAsync(ct);
        await db.WalletBalances.ExecuteDeleteAsync(ct);
        await db.Orders.ExecuteDeleteAsync(ct);

        // Load every business event in the order the Web received it (Seq).  Diagnostics are skipped:
        // they never change projections.  AsNoTracking + a narrow Select keep memory small.
        var events = await db.EventAudit.AsNoTracking()
            .Where(a => a.Category == "Business")
            .OrderBy(a => a.Seq)
            .Select(a => new { a.Kind, a.PayloadJson })
            .ToListAsync(ct);

        var applied = 0;
        foreach (var row in events)
        {
            // Defensive: a row without payload cannot be replayed.
            if (row.PayloadJson is null) continue;
            // Turn the stored JSON back into the concrete event class: Kind ("OrderPlaced") → Type.
            var message = (Message)Json.Deserialize(row.PayloadJson, ContractTypes.Resolve(row.Kind))!;
            // Exactly the same code as the live path, with the same ordering guards, so the result
            // matches what live traffic produced (even if events had arrived out of order).
            await projector.ApplyAsync(message, ct);
            applied++;
        }

        // Write all rebuilt rows at once, then make them visible atomically.
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        logger.LogInformation("Read model rebuilt from {Count} audited events", applied);
        // Tell every open page to re-query the (new) read model.
        notifier.Notify("readmodel");
        return applied;
    }
}
