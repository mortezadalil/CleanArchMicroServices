using Microsoft.EntityFrameworkCore.Diagnostics;

namespace MiniShop.Shared.Faults;

/// <summary>
/// Simulates a transient database failure (scenario S07): when a Db.SaveChanges fault rule matches
/// the current scenario, SaveChanges throws BEFORE anything is written, the transaction rolls back
/// and MassTransit retries the message.  SQLite rarely fails on its own, so this is how we test
/// "retry without partial writes".
/// </summary>
public sealed class FaultingSaveChangesInterceptor(FaultRegistry faults) : SaveChangesInterceptor
{
    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        await faults.ApplyAsync(FaultPoints.DbSaveChanges, FaultContext.ScenarioId, FaultContext.CorrelationId,
            subjectMessageId: null, subjectMessageType: null, ct);
        return result;
    }
}
