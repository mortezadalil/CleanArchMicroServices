using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Orders.Data;
using MiniShop.Orders.Domain;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// Applies a <see cref="SagaDecision"/> to the database: updates the saga row, the order row, writes
/// an audit line and the outgoing messages to the Outbox.  Everything happens on the caller's
/// DbContext, inside the caller's transaction (the IdempotentConsumer or the timeout worker).
/// </summary>
public sealed class SagaRunner(OrdersDbContext db, DiagnosticsPublisher diagnostics, IOptions<SagaOptions> options)
{
    public SagaOptions Options => options.Value;

    /// <summary>Loads saga + order for a correlation id (tracked, so later changes are saved by the caller).</summary>
    public async Task<(OrderSagaState Saga, Order Order)?> LoadAsync(Guid orderId, CancellationToken ct)
    {
        var saga = await db.SagaInstances.FirstOrDefaultAsync(s => s.CorrelationId == orderId, ct);
        var order = await db.Orders.Include(o => o.Items).FirstOrDefaultAsync(o => o.Id == orderId, ct);
        return saga is null || order is null ? null : (saga, order);
    }

    /// <summary>React to an incoming message. Returns the HandleResult for the consumer base class.</summary>
    public async Task<HandleResult> HandleMessageAsync(Message incoming, CancellationToken ct)
    {
        var loaded = await LoadAsync(incoming.CorrelationId, ct);
        if (loaded is null)
            return new HandleResult($"no saga for order {incoming.CorrelationId}; {incoming.Type} ignored");

        var (saga, order) = loaded.Value;
        var decision = OrderSaga.Decide(saga, order, incoming, options.Value);
        return Apply(saga, order, decision, trigger: incoming.Type, causation: incoming);
    }

    /// <summary>Apply a decision (shared by message handling, timeouts and manual retries).</summary>
    public HandleResult Apply(OrderSagaState saga, Order order, SagaDecision decision, string trigger, Message? causation)
    {
        var sagaFrom = saga.CurrentState;
        var orderFrom = order.State;
        var now = DateTime.UtcNow;

        if (!decision.Ignored)
        {
            saga.CurrentState = decision.SagaState;
            saga.DeadlineUtc = decision.WaitFor is { } wait ? now.Add(wait) : null;
            saga.FailureReason = decision.FailureReason ?? saga.FailureReason;
            if (decision.CompensationAttempts is { } attempts)
                saga.CompensationAttempts = attempts;
            saga.Version++;                // concurrency token: a concurrent writer will fail and retry
            saga.UpdatedAtUtc = now;

            order.State = decision.OrderState;
            order.FailureReason = decision.FailureReason ?? order.FailureReason;
            order.UpdatedAtUtc = now;

            foreach (var outgoing in decision.Outgoing)
            {
                if (outgoing.DestinationQueue is null)
                    db.PublishViaOutbox(outgoing.Message);
                else
                    db.SendViaOutbox(outgoing.Message, outgoing.DestinationQueue);
            }
        }

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

        var snapshot = Snapshot(saga, order);
        return new HandleResult(decision.Detail, snapshot)
        {
            // Emitted only after the transaction committed, so the console never shows a transition that was rolled back.
            AfterCommit = () => diagnostics.Emit(DiagnosticKind.SagaTransitioned, order.Id, order.ScenarioId,
                subjectMessageId: causation?.MessageId, subjectMessageType: trigger,
                detail: decision.Ignored ? decision.Detail : $"{sagaFrom} → {saga.CurrentState}: {decision.Detail}",
                outcome: decision.Ignored ? "Ignored" : "Transitioned", snapshot: snapshot),
        };
    }

    public static object Snapshot(OrderSagaState saga, Order order) => new
    {
        order = new { order.Id, order.State, order.Amount, order.CustomerId, order.FailureReason },
        saga = new { saga.CurrentState, saga.DeadlineUtc, saga.CompensationAttempts, saga.Version },
    };
}
