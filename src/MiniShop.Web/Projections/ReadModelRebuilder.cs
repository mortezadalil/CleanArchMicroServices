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
/// </summary>
public sealed class ReadModelRebuilder(ReadModelDbContext db, Projector projector, UiNotifier notifier, ILogger<ReadModelRebuilder> logger)
{
    public async Task<int> RebuildAsync(CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        await db.ProductCards.ExecuteDeleteAsync(ct);
        await db.WalletBalances.ExecuteDeleteAsync(ct);
        await db.Orders.ExecuteDeleteAsync(ct);

        var events = await db.EventAudit.AsNoTracking()
            .Where(a => a.Category == "Business")
            .OrderBy(a => a.Seq)
            .Select(a => new { a.Kind, a.PayloadJson })
            .ToListAsync(ct);

        var applied = 0;
        foreach (var row in events)
        {
            if (row.PayloadJson is null) continue;
            var message = (Message)Json.Deserialize(row.PayloadJson, ContractTypes.Resolve(row.Kind))!;
            await projector.ApplyAsync(message, ct);
            applied++;
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        logger.LogInformation("Read model rebuilt from {Count} audited events", applied);
        notifier.Notify("readmodel");
        return applied;
    }
}
