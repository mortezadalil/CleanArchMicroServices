using MiniShop.Contracts;
using MiniShop.Orders.Domain;

namespace MiniShop.Orders.Saga;

/// <summary>A message the saga wants to send out, with its destination (null = publish as event).</summary>
public sealed record OutgoingMessage(Message Message, string? DestinationQueue);

/// <summary>Everything the saga decided for one incoming message or timeout. Pure data: nothing is persisted here.</summary>
public sealed record SagaDecision(
    string SagaState,
    OrderState OrderState,
    IReadOnlyList<OutgoingMessage> Outgoing,
    string Detail,
    TimeSpan? WaitFor = null,
    string? FailureReason = null,
    int? CompensationAttempts = null,
    bool Ignored = false)
{
    public static SagaDecision Ignore(OrderSagaState saga, Order order, string why) =>
        new(saga.CurrentState, order.State, [], why, Ignored: true);
}

/// <summary>
/// The order saga as a PURE state machine: (current state, incoming message) → decision.
/// No database, no bus, no clock — which makes every transition unit-testable.
/// <see cref="SagaRunner"/> applies decisions inside a transaction and <see cref="SagaTimeoutWorker"/>
/// calls <see cref="OnTimeout"/> when a deadline passes.
///
/// Orchestration rules:
///   1. Reserve stock first, then charge the wallet, then confirm.
///   2. Any failure after a reservation is compensated by ReleaseStock; the order is only "Rejected"
///      once Inventory CONFIRMED the release (StockReleased).
///   3. A payment timeout is NOT "no payment": the saga asks Payments to cancel; Payments answers
///      either PaymentCancelled (safe to compensate) or PaymentAlreadyCompleted (confirm the order).
///   4. A compensation that is never confirmed is re-sent a few times, then the order goes to
///      RequiresAttention instead of pretending it was rejected.
/// </summary>
public static class OrderSaga
{
    public static SagaDecision Decide(OrderSagaState saga, Order order, Message incoming, SagaOptions options)
    {
        if (SagaStates.IsTerminal(saga.CurrentState))
            return SagaDecision.Ignore(saga, order, $"late/duplicate {incoming.Type} ignored: saga already {saga.CurrentState}");

        switch (saga.CurrentState, incoming)
        {
            // ---- happy path -------------------------------------------------------------------------
            case (SagaStates.Started, OrderPlaced placed):
                return new SagaDecision(SagaStates.WaitingForStock, OrderState.Reserving,
                    [Send(new ReserveStock(order.Id, placed.Lines), Topology.InventoryQueue, order, incoming)],
                    "order placed → asking Inventory to reserve stock",
                    WaitFor: TimeSpan.FromSeconds(options.StockTimeoutSeconds));

            case (SagaStates.WaitingForStock, StockReserved):
                return new SagaDecision(SagaStates.WaitingForPayment, OrderState.AwaitingPayment,
                    [Send(new ChargeWallet(order.Id, order.CustomerId, order.Amount), Topology.PaymentsQueue, order, incoming)],
                    "stock reserved → asking Payments to charge the wallet",
                    WaitFor: TimeSpan.FromSeconds(options.PaymentTimeoutSeconds));

            case (SagaStates.WaitingForPayment, PaymentCompleted):
                return Confirm(order, incoming, "payment completed → order confirmed");

            // ---- rejections without anything to compensate -------------------------------------------
            case (SagaStates.WaitingForStock, StockReservationRejected rejected):
                return Reject(order, incoming, rejected.Reason, "stock rejected → order rejected (nothing to compensate)");

            // ---- failures after a reservation: compensate --------------------------------------------
            case (SagaStates.WaitingForPayment, PaymentFailed failed):
                return StartRelease(order, incoming, failed.Reason, options, "payment failed → releasing stock");

            case (SagaStates.WaitingForPaymentCancellation, PaymentCancelled cancelled):
                return StartRelease(order, incoming, cancelled.Reason, options, "payment cancelled → releasing stock");

            case (SagaStates.WaitingForPaymentCancellation, PaymentFailed failed):
                return StartRelease(order, incoming, failed.Reason, options, "payment failed (answer to cancel) → releasing stock");

            case (SagaStates.WaitingForPaymentCancellation, PaymentAlreadyCompleted):
            case (SagaStates.WaitingForPaymentCancellation, PaymentCompleted):
                return Confirm(order, incoming, $"LATE payment reconciled: money was taken ({incoming.Type}) → order confirmed, not rejected");

            case (SagaStates.WaitingForRelease, StockReleased):
                return Reject(order, incoming, saga.FailureReason ?? "compensated", "stock released → order rejected (compensation confirmed)");

            // ---- recovery from RequiresAttention (e.g. after a human re-sent the compensation) ---------
            case (SagaStates.RequiresAttention, StockReleased):
                return Reject(order, incoming, saga.FailureReason ?? "compensated", "stock released after manual retry → order rejected");

            case (SagaStates.RequiresAttention, PaymentCancelled cancelled):
                return StartRelease(order, incoming, cancelled.Reason, options, "payment cancelled after manual retry → releasing stock");

            default:
                return SagaDecision.Ignore(saga, order, $"{incoming.Type} is not expected in state {saga.CurrentState}; ignored (late or out-of-order)");
        }
    }

    /// <summary>Called by the timeout worker when DeadlineUtc passed without an answer.</summary>
    public static SagaDecision OnTimeout(OrderSagaState saga, Order order, SagaOptions options)
    {
        var causation = (Guid?)null;
        switch (saga.CurrentState)
        {
            case SagaStates.WaitingForStock:
                // Inventory never answered. Maybe it is still processing; a ReleaseStock makes the outcome
                // deterministic: Inventory either releases the reservation or stores a tombstone that
                // rejects a late ReserveStock.
                return new SagaDecision(SagaStates.WaitingForRelease, OrderState.Compensating,
                    [Send(new ReleaseStock(order.Id, "stock reservation timed out"), Topology.InventoryQueue, order, causation)],
                    "stock reservation timed out → releasing (tombstone if nothing was reserved)",
                    WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
                    FailureReason: "stock reservation timed out", CompensationAttempts: 1);

            case SagaStates.WaitingForPayment:
                // A timeout does NOT mean "not paid". Ask Payments for a definitive answer first.
                return new SagaDecision(SagaStates.WaitingForPaymentCancellation, OrderState.Compensating,
                    [Send(new CancelPayment(order.Id, "payment timed out"), Topology.PaymentsQueue, order, causation)],
                    "payment timed out → asking Payments to cancel (reconcile before compensating)",
                    WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
                    FailureReason: "payment timed out", CompensationAttempts: 1);

            case SagaStates.WaitingForPaymentCancellation:
                return RetryOrGiveUp(saga, order, options,
                    () => Send(new CancelPayment(order.Id, saga.FailureReason ?? "payment timed out"), Topology.PaymentsQueue, order, causation),
                    "CancelPayment");

            case SagaStates.WaitingForRelease:
                return RetryOrGiveUp(saga, order, options,
                    () => Send(new ReleaseStock(order.Id, saga.FailureReason ?? "compensation"), Topology.InventoryQueue, order, causation),
                    "ReleaseStock");

            default:
                return SagaDecision.Ignore(saga, order, $"timeout in state {saga.CurrentState} has no effect");
        }
    }

    /// <summary>Operator action: re-send the pending compensation of a saga that is in RequiresAttention.</summary>
    public static SagaDecision RetryCompensation(OrderSagaState saga, Order order, SagaOptions options)
    {
        if (saga.CurrentState != SagaStates.RequiresAttention)
            return SagaDecision.Ignore(saga, order, "retry is only allowed in RequiresAttention");

        return new SagaDecision(SagaStates.WaitingForRelease, OrderState.Compensating,
            [Send(new ReleaseStock(order.Id, saga.FailureReason ?? "manual retry"), Topology.InventoryQueue, order, (Guid?)null)],
            "manual retry → re-sending ReleaseStock",
            WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
            CompensationAttempts: 1);
    }

    // ---- helpers -------------------------------------------------------------------------------------

    private static SagaDecision Confirm(Order order, Message incoming, string detail) =>
        new(SagaStates.Completed, OrderState.Confirmed,
            [Publish(new OrderConfirmed(order.Id, order.CustomerId, order.Amount), order, incoming)], detail);

    private static SagaDecision Reject(Order order, Message incoming, string reason, string detail) =>
        new(SagaStates.Rejected, OrderState.Rejected,
            [Publish(new OrderRejected(order.Id, order.CustomerId, reason), order, incoming)], detail, FailureReason: reason);

    private static SagaDecision StartRelease(Order order, Message incoming, string reason, SagaOptions options, string detail) =>
        new(SagaStates.WaitingForRelease, OrderState.Compensating,
            [Send(new ReleaseStock(order.Id, reason), Topology.InventoryQueue, order, incoming)], detail,
            WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds), FailureReason: reason, CompensationAttempts: 1);

    private static SagaDecision RetryOrGiveUp(OrderSagaState saga, Order order, SagaOptions options, Func<OutgoingMessage> resend, string commandName)
    {
        if (saga.CompensationAttempts < options.MaxCompensationAttempts)
        {
            var attempt = saga.CompensationAttempts + 1;
            return new SagaDecision(saga.CurrentState, OrderState.Compensating, [resend()],
                $"no answer to {commandName} → re-sending (attempt {attempt} of {options.MaxCompensationAttempts})",
                WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds), CompensationAttempts: attempt);
        }

        var reason = $"{commandName} was not confirmed after {options.MaxCompensationAttempts} attempts";
        return new SagaDecision(SagaStates.RequiresAttention, OrderState.RequiresAttention,
            [Publish(new OrderRequiresAttention(order.Id, order.CustomerId, reason), order, (Guid?)null)],
            $"{reason} → RequiresAttention (NOT silently rejected)", FailureReason: saga.FailureReason);
    }

    private static OutgoingMessage Send(Message command, string queue, Order order, Message cause) => Send(command, queue, order, cause.MessageId);
    private static OutgoingMessage Send(Message command, string queue, Order order, Guid? causationId) =>
        new(Stamp(command, order, causationId), queue);

    private static OutgoingMessage Publish(Message @event, Order order, Message cause) => Publish(@event, order, cause.MessageId);
    private static OutgoingMessage Publish(Message @event, Order order, Guid? causationId) =>
        new(Stamp(@event, order, causationId), null);

    /// <summary>Fill the envelope: correlation = order, causation = the message we are reacting to.</summary>
    private static Message Stamp(Message message, Order order, Guid? causationId) => message with
    {
        CorrelationId = order.Id,
        CausationId = causationId,
        ScenarioId = order.ScenarioId,
        Producer = ServiceNames.Orders,
        OccurredAtUtc = DateTime.UtcNow,
    };
}
