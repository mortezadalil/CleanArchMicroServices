// ==================================================================================================
// MiniShop.Orders — Saga/SagaRunner.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The "impure" half of the saga. OrderSaga.cs DECIDES (pure function, no I/O); SagaRunner APPLIES the
//   decision to the database: saga row, order row, audit row and outgoing messages as Outbox rows.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Three callers, one way of applying decisions:
//     - SagaEventConsumer (Saga/SagaConsumers.cs) → HandleMessageAsync   (an event arrived)
//     - SagaTimeoutWorker (Saga/SagaTimeoutWorker.cs) → Apply             (a deadline passed)
//     - retry-compensation endpoint (Api/OrdersEndpoints.cs) → Apply      (an operator acted, S12)
//   The runner NEVER calls SaveChanges or commits: the caller owns the transaction. That way the Inbox
//   row (for consumers), the saga change and the Outbox rows are committed together, or not at all.
//
// PATTERNS
//   - Transactional Outbox: commands/events are not sent directly; they become rows here.
//   - Optimistic concurrency: saga.Version++ on every transition.
//
// Read next: Saga/SagaTimeoutWorker.cs.
// ==================================================================================================

// FirstOrDefaultAsync, Include.
using Microsoft.EntityFrameworkCore;
// IOptions<T>.
using Microsoft.Extensions.Options;
// Message base record.
using MiniShop.Contracts;
// DiagnosticKind.
using MiniShop.Contracts.Diagnostics;
// OrdersDbContext.
using MiniShop.Orders.Data;
// Order, OrderAudit.
using MiniShop.Orders.Domain;
// DiagnosticsPublisher.
using MiniShop.Shared.Diagnostics;
// HandleResult.
using MiniShop.Shared.Messaging;
// PublishViaOutbox / SendViaOutbox.
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// Applies a <see cref="SagaDecision"/> to the database: updates the saga row, the order row, writes
/// an audit line and the outgoing messages to the Outbox.  Everything happens on the caller's
/// DbContext, inside the caller's transaction (the IdempotentConsumer or the timeout worker).
/// </summary>
/// <param name="db">
/// The scoped DbContext. Registered as scoped in Program.cs, so it is the SAME instance the consumer or
/// worker uses: that is how the runner's changes join the caller's transaction.
/// </param>
/// <param name="diagnostics">Telemetry for the developer console (emitted after commit only).</param>
/// <param name="options">Saga timeouts and limits from appsettings.json.</param>
public sealed class SagaRunner(OrdersDbContext db, DiagnosticsPublisher diagnostics, IOptions<SagaOptions> options)
{
    /// <summary>The current saga options; exposed so callers can pass them to the pure OrderSaga functions.</summary>
    public SagaOptions Options => options.Value;

    /// <summary>Loads saga + order for a correlation id (tracked, so later changes are saved by the caller).</summary>
    /// <param name="orderId">OrderId = saga CorrelationId.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Both entities, or null when either one is missing (unknown order).</returns>
    public async Task<(OrderSagaState Saga, Order Order)?> LoadAsync(Guid orderId, CancellationToken ct)
    {
        // Tracked queries (no AsNoTracking): EF remembers the loaded values, including Version, so the
        // caller's SaveChanges can detect a concurrent change.
        var saga = await db.SagaInstances.FirstOrDefaultAsync(s => s.CorrelationId == orderId, ct);
        // Items are included because the state machine may need the lines (e.g. for snapshots).
        var order = await db.Orders.Include(o => o.Items).FirstOrDefaultAsync(o => o.Id == orderId, ct);
        // Both must exist; the API always creates them together, so a missing one means an unknown order.
        return saga is null || order is null ? null : (saga, order);
    }

    /// <summary>React to an incoming message. Returns the HandleResult for the consumer base class.</summary>
    /// <param name="incoming">The event that was delivered; its CorrelationId is the OrderId.</param>
    /// <param name="ct">Cancellation token of the delivery.</param>
    public async Task<HandleResult> HandleMessageAsync(Message incoming, CancellationToken ct)
    {
        // 1. Load the persisted saga and order for this message's order.
        var loaded = await LoadAsync(incoming.CorrelationId, ct);
        // An event for an order we do not know (e.g. after a demo reset) is acknowledged and ignored.
        // Throwing would only send it through retries into the error queue for no benefit.
        if (loaded is null)
            return new HandleResult($"no saga for order {incoming.CorrelationId}; {incoming.Type} ignored");

        // 2. Ask the pure state machine what to do.
        var (saga, order) = loaded.Value;
        var decision = OrderSaga.Decide(saga, order, incoming, options.Value);
        // 3. Stage the decision on the DbContext; the consumer base class saves and commits.
        return Apply(saga, order, decision, trigger: incoming.Type, causation: incoming);
    }

    /// <summary>Apply a decision (shared by message handling, timeouts and manual retries).</summary>
    /// <param name="saga">The tracked saga row (will be modified).</param>
    /// <param name="order">The tracked order row (will be modified).</param>
    /// <param name="decision">What OrderSaga decided.</param>
    /// <param name="trigger">Label for the audit: the message type, "Timeout" or "ManualRetry".</param>
    /// <param name="causation">The message that caused this decision, or null for timeouts / manual actions.</param>
    /// <returns>Detail + snapshot for telemetry, plus an AfterCommit callback the caller must run after COMMIT.</returns>
    public HandleResult Apply(OrderSagaState saga, Order order, SagaDecision decision, string trigger, Message? causation)
    {
        // Remember the "before" values for the audit line and the telemetry text.
        var sagaFrom = saga.CurrentState;
        var orderFrom = order.State;
        // One clock reading for all rows of this transition.
        var now = DateTime.UtcNow;

        // An ignored decision (late / duplicate / unexpected message) changes NOTHING except the audit.
        if (!decision.Ignored)
        {
            // --- saga row ---
            // Move to the new state.
            saga.CurrentState = decision.SagaState;
            // Store the deadline in the DATABASE: now + WaitFor, or null when we are no longer waiting
            // (terminal states, RequiresAttention). The timeout worker polls this column, so the
            // deadline survives a restart (S09).
            saga.DeadlineUtc = decision.WaitFor is { } wait ? now.Add(wait) : null;
            // Keep the first failure reason unless the decision provides a new one.
            saga.FailureReason = decision.FailureReason ?? saga.FailureReason;
            // Only overwrite the counter when the decision says so (start = 1, retry = n+1).
            if (decision.CompensationAttempts is { } attempts)
                saga.CompensationAttempts = attempts;
            // Bump the optimistic-concurrency token. EF's UPDATE becomes "... WHERE Version = old"; if
            // another writer (a second message for the same order, or the timeout worker) committed first,
            // 0 rows match, DbUpdateConcurrencyException is thrown, our transaction rolls back and the
            // caller retries with fresh data. So two decisions can never both win.
            saga.Version++;                // concurrency token: a concurrent writer will fail and retry
            saga.UpdatedAtUtc = now;

            // --- order row (customer-facing state) ---
            order.State = decision.OrderState;
            order.FailureReason = decision.FailureReason ?? order.FailureReason;
            order.UpdatedAtUtc = now;

            // --- outgoing messages → Outbox rows (same transaction) ---
            // Nothing is sent to RabbitMQ here. If the transaction rolls back, these rows vanish too,
            // so we can never send a command for a transition that did not happen.
            foreach (var outgoing in decision.Outgoing)
            {
                // No destination = an EVENT (OrderConfirmed, OrderRejected, ...): published to anyone interested.
                if (outgoing.DestinationQueue is null)
                    db.PublishViaOutbox(outgoing.Message);
                // With a destination = a COMMAND (ReserveStock, ChargeWallet, ...): sent to exactly one queue.
                else
                    db.SendViaOutbox(outgoing.Message, outgoing.DestinationQueue);
            }
        }

        // Audit line for EVERY decision, ignored ones included: "why was this late message dropped?" is a
        // question students (and operators) ask, and the answer should be in the database.
        db.OrderAudit.Add(new OrderAudit
        {
            OrderId = order.Id,
            Trigger = trigger,
            OrderFrom = orderFrom.ToString(),
            OrderTo = order.State.ToString(),
            SagaFrom = sagaFrom,
            SagaTo = saga.CurrentState,
            Detail = decision.Detail,
            AtUtc = now,
        });

        // Take the snapshot now (values after the change) for the telemetry record.
        var snapshot = Snapshot(saga, order);
        return new HandleResult(decision.Detail, snapshot)
        {
            // Emitted only after the transaction committed, so the console never shows a transition that was rolled back.
            // It is a callback (not called here) because this method runs BEFORE SaveChanges/Commit; the
            // caller invokes it after a successful commit (IdempotentConsumer, timeout worker, endpoint).
            AfterCommit = () => diagnostics.Emit(DiagnosticKind.SagaTransitioned, order.Id, order.ScenarioId,
                subjectMessageId: causation?.MessageId, subjectMessageType: trigger,
                detail: decision.Ignored ? decision.Detail : $"{sagaFrom} → {saga.CurrentState}: {decision.Detail}",
                outcome: decision.Ignored ? "Ignored" : "Transitioned", snapshot: snapshot),
        };
    }

    /// <summary>
    /// A small anonymous object with the interesting fields of order + saga, shown in the developer
    /// console. Anonymous types serialise to JSON nicely and avoid exposing whole entities.
    /// </summary>
    /// <param name="saga">The saga row.</param>
    /// <param name="order">The order row.</param>
    public static object Snapshot(OrderSagaState saga, Order order) => new
    {
        order = new { order.Id, order.State, order.Amount, order.CustomerId, order.FailureReason },
        saga = new { saga.CurrentState, saga.DeadlineUtc, saga.CompensationAttempts, saga.Version },
    };
}
