// ==================================================================================================
// MiniShop.Orders — Saga/SagaTimeoutWorker.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   A background service that turns overdue saga deadlines into "Timeout" decisions.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Messages can be lost in practice: a service may be down, a consumer paused, a message parked in an
//   error queue (S10). A saga that only reacts to messages would then wait forever. So every waiting
//   state has a deadline (OrderSagaState.DeadlineUtc, set by SagaRunner.Apply). This worker finds sagas
//   whose deadline passed and asks OrderSaga.OnTimeout what to do (compensate, ask Payments to cancel,
//   retry the compensation, or give up → RequiresAttention).
//
// SCENARIOS
//   S10 payment timeout → cancellation, S12 compensation keeps failing, S13 late payment after timeout,
//   S09 restart (deadlines are rows, not in-memory timers, so they survive).
//
// Read next: Saga/SagaOptions.cs (the timeout values), then shared/Outbox/OutboxDispatcher.cs.
// ==================================================================================================

// ToListAsync, BeginTransactionAsync, DbUpdateConcurrencyException.
using Microsoft.EntityFrameworkCore;
// OrdersDbContext.
using MiniShop.Orders.Data;
// FaultContext.
using MiniShop.Shared.Faults;
// OutboxSignal.
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// Polls the saga table once per second for instances whose DeadlineUtc has passed and lets the state
/// machine decide what to do (<see cref="OrderSaga.OnTimeout"/>).  Deadlines are stored in the database,
/// so a restart of the Orders service does not lose them (S09).
///
/// A simple poll is used instead of a message scheduler on purpose: it is easy to read and needs no
/// extra RabbitMQ plugin.
/// </summary>
/// <param name="scopes">
/// Hosted services are singletons, but OrdersDbContext and SagaRunner are scoped. We therefore create a
/// fresh DI scope per poll to get a fresh DbContext (never keep one DbContext alive forever).
/// </param>
/// <param name="signal">Wakes the outbox dispatcher after a timeout produced new outbox rows.</param>
/// <param name="logger">Logger.</param>
public sealed class SagaTimeoutWorker(IServiceScopeFactory scopes, OutboxSignal signal, ILogger<SagaTimeoutWorker> logger) : BackgroundService
{
    /// <summary>The worker loop: process expired sagas, sleep one second, repeat until the app stops.</summary>
    /// <param name="stoppingToken">Cancelled when the host shuts down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessExpiredAsync(stoppingToken);
            }
            // Never let one failure (e.g. "database is locked") kill the worker: an unhandled exception in
            // a BackgroundService stops it for the rest of the process lifetime, and then no saga would
            // ever time out again. Log and try again in a second. Cancellation is let through (shutdown).
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Saga timeout worker failed; will retry");
            }
            // One second granularity is plenty for timeouts measured in seconds.
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    /// <summary>Finds every saga whose deadline passed and applies its timeout decision, one saga per transaction.</summary>
    /// <param name="ct">Cancellation token.</param>
    private async Task ProcessExpiredAsync(CancellationToken ct)
    {
        // Fresh scope = fresh DbContext and SagaRunner (they share the DbContext inside this scope).
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
        var runner = scope.ServiceProvider.GetRequiredService<SagaRunner>();

        // Only the ids, cheaply via the DeadlineUtc index. We re-load each saga inside its own transaction
        // below, because the list may already be stale by the time we get to an item.
        var now = DateTime.UtcNow;
        var expiredIds = await db.SagaInstances
            .Where(s => s.DeadlineUtc != null && s.DeadlineUtc <= now)
            .Select(s => s.CorrelationId)
            .ToListAsync(ct);

        foreach (var orderId in expiredIds)
        {
            try
            {
                // One short transaction per saga: read → decide → write → commit. A problem with one order
                // does not block the others.
                await using var tx = await db.Database.BeginTransactionAsync(ct);
                var loaded = await runner.LoadAsync(orderId, ct);
                if (loaded is null) continue;
                var (saga, order) = loaded.Value;
                // Re-check inside the transaction: an answer may have arrived between the query above and now
                // (it cleared or moved the deadline). Acting anyway would e.g. cancel a payment that just succeeded.
                if (saga.DeadlineUtc is null || saga.DeadlineUtc > DateTime.UtcNow) continue; // answered meanwhile

                // Let fault rules know which scenario/order this save belongs to (e.g. S12 failing saves).
                using var _ = FaultContext.Enter(order.ScenarioId, order.Id);
                // Pure decision: what does a timeout mean in the CURRENT state?
                var decision = OrderSaga.OnTimeout(saga, order, runner.Options);
                // Stage saga/order/audit/outbox changes. No causation message: time itself is the trigger.
                var result = runner.Apply(saga, order, decision, trigger: "Timeout", causation: null);
                // Save (checks the Version token) and commit atomically.
                await db.SaveChangesAsync(ct);
                await tx.CommitAsync(ct);
                // Only after commit: telemetry, and wake the dispatcher so the new command leaves at once.
                result.AfterCommit?.Invoke();
                signal.Notify();
                // Warning level so timeouts stand out in the log during a demo.
                logger.LogWarning("Saga {OrderId} timed out: {Detail}", orderId, decision.Detail);
            }
            catch (DbUpdateConcurrencyException)
            {
                // A message for this order was processed at the same time; the next poll re-evaluates.
                // (The Version token detected it.) Clear the tracker so the stale entities are not reused
                // for the next saga in this loop; the transaction was disposed without commit = rolled back.
                db.ChangeTracker.Clear();
            }
        }
    }
}
