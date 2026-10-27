// ==========================================================================================
// FILE: shared/Faults/FaultingSaveChangesInterceptor.cs
//
// WHAT THIS FILE IS
//   An EF Core interceptor that runs just before every SaveChanges and can throw on purpose.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source), fault-injection building block.  Registered as a singleton in
//   ServiceHostingExtensions.cs and attached to each service's DbContext in Program.cs with
//   `.AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>())`.
//   It finds out WHICH scenario is running through the ambient FaultContext (FaultContext.cs).
//
// WHICH PATTERN IT IMPLEMENTS
//   Fault injection for "transient database failure" (Scenario S07): proves that a failed write
//   leaves no partial state, and that the retry (with the Inbox) makes the operation happen exactly once.
//
// Read next: shared/Faults/FaultRegistry.cs.
// ==========================================================================================

using Microsoft.EntityFrameworkCore.Diagnostics;

namespace MiniShop.Shared.Faults;

/// <summary>
/// Simulates a transient database failure (scenario S07): when a Db.SaveChanges fault rule matches
/// the current scenario, SaveChanges throws BEFORE anything is written, the transaction rolls back
/// and MassTransit retries the message.  SQLite rarely fails on its own, so this is how we test
/// "retry without partial writes".
/// </summary>
/// <remarks>
/// <c>SaveChangesInterceptor</c> is an EF Core base class with virtual hooks around SaveChanges; we only
/// override the "saving" (before) hook.  The interceptor is a singleton shared by all DbContexts, which is
/// fine because it holds no per-request state — the per-message ids come from <see cref="FaultContext"/>.
/// </remarks>
/// <param name="faults">The service's fault registry (singleton).</param>
public sealed class FaultingSaveChangesInterceptor(FaultRegistry faults) : SaveChangesInterceptor
{
    /// <summary>Called by EF Core before SaveChangesAsync writes anything.</summary>
    /// <param name="eventData">Information about the DbContext being saved (unused here).</param>
    /// <param name="result">EF's "continue normally" value; returning it unchanged lets SaveChanges proceed.</param>
    /// <param name="ct">Cancellation token of the SaveChanges call.</param>
    /// <returns><paramref name="result"/>, unless a Fail rule threw an <see cref="InjectedFaultException"/>.</returns>
    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken ct = default)
    {
        // Ask the registry whether a Db.SaveChanges rule matches the CURRENT scenario/order (read from the
        // ambient FaultContext, set by IdempotentConsumer).  Outside a scenario ScenarioId is null, so only
        // global rules could match — normal shoppers are never hit by an experiment's rule.
        // A Fail rule throws here: nothing has been written yet, the surrounding transaction rolls back.
        await faults.ApplyAsync(FaultPoints.DbSaveChanges, FaultContext.ScenarioId, FaultContext.CorrelationId,
            subjectMessageId: null, subjectMessageType: null, ct);
        // No fault (or a non-throwing one): let EF Core continue with the normal save.
        return result;
    }
}
