// ==================================================================================================
// MiniShop.Orders — Saga/OrderSaga.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The brain of the order saga: every rule of the orchestration, written as PURE functions.
//   Input: the current saga row, the order and an incoming message (or "a timeout happened").
//   Output: a SagaDecision record (next states + messages to send + deadline). Nothing else.
//
// WHY A PURE FUNCTION?
//   No database, no RabbitMQ, no clock (except stamping OccurredAtUtc on outgoing envelopes). That means:
//     - every arrow of the state chart can be unit-tested with plain objects, in microseconds;
//     - the rules are readable in one place, top to bottom, without plumbing in the way;
//     - crash safety is handled elsewhere: SagaRunner writes the decision in ONE transaction with the
//       Inbox row and the Outbox rows, so a decision is either fully applied or not at all.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   SagaConsumers → SagaRunner.HandleMessageAsync → OrderSaga.Decide → SagaRunner.Apply
//   SagaTimeoutWorker → OrderSaga.OnTimeout → SagaRunner.Apply
//   retry-compensation endpoint → OrderSaga.RetryCompensation → SagaRunner.Apply
//   This is an ORCHESTRATED saga: Orders tells the others what to do (commands) and they answer with
//   events. (The alternative, choreography, lets services react to each other's events with no centre.)
//
// Read next: Saga/SagaRunner.cs (how a decision is persisted).
// ==================================================================================================

// Message base record, all commands/events, Topology (queue names), ServiceNames.
using MiniShop.Contracts;
// Order, OrderState.
using MiniShop.Orders.Domain;

namespace MiniShop.Orders.Saga;

/// <summary>A message the saga wants to send out, with its destination (null = publish as event).</summary>
/// <param name="Message">The fully stamped message (correlation, causation, scenario, producer).</param>
/// <param name="DestinationQueue">
/// A queue name for a COMMAND (sent to exactly one service, e.g. Topology.InventoryQueue), or null for an
/// EVENT (published; whoever subscribes, e.g. the Web's read model, receives a copy).
/// </param>
public sealed record OutgoingMessage(Message Message, string? DestinationQueue);

/// <summary>Everything the saga decided for one incoming message or timeout. Pure data: nothing is persisted here.</summary>
/// <param name="SagaState">The next saga state (one of <see cref="SagaStates"/>).</param>
/// <param name="OrderState">The next customer-facing order state.</param>
/// <param name="Outgoing">Commands/events to write to the Outbox in the same transaction.</param>
/// <param name="Detail">Plain-language explanation, stored in the audit and shown in the console.</param>
/// <param name="WaitFor">
/// How long to wait for the next answer. SagaRunner turns it into DeadlineUtc = now + WaitFor.
/// Null means "not waiting" (terminal or RequiresAttention): the deadline is cleared.
/// </param>
/// <param name="FailureReason">Why the order is failing; null keeps the reason already stored.</param>
/// <param name="CompensationAttempts">New value of the compensation counter; null leaves it unchanged.</param>
/// <param name="Ignored">
/// True when the message does not fit the current state (late, duplicate, out of order). SagaRunner then
/// changes nothing but still writes an audit line explaining why.
/// </param>
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
    /// <summary>Builds an "ignore" decision: same states as now, nothing to send, and the reason.</summary>
    /// <param name="saga">Current saga (its state is kept).</param>
    /// <param name="order">Current order (its state is kept).</param>
    /// <param name="why">Explanation for the audit.</param>
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
    /// <summary>Decides how the saga reacts to an incoming event.</summary>
    /// <param name="saga">Current persisted saga state.</param>
    /// <param name="order">The order (ids, amount, customer, scenario).</param>
    /// <param name="incoming">The event that was delivered.</param>
    /// <param name="options">Timeouts used to compute the next deadline.</param>
    public static SagaDecision Decide(OrderSagaState saga, Order order, Message incoming, SagaOptions options)
    {
        // Once Completed or Rejected, the story is over. Any message now is a late answer or a duplicate
        // (RabbitMQ is at-least-once, S06/S11). Ignoring it keeps the order from flipping back and forth.
        if (SagaStates.IsTerminal(saga.CurrentState))
            return SagaDecision.Ignore(saga, order, $"late/duplicate {incoming.Type} ignored: saga already {saga.CurrentState}");

        // C# tuple pattern matching: match on (current state, message type) at the same time. Each case is
        // exactly one arrow of the state chart (see OrderSagaState.cs). A type pattern like
        // "OrderPlaced placed" both checks the type and gives us a typed variable.
        switch (saga.CurrentState, incoming)
        {
            // ---- happy path -------------------------------------------------------------------------
            // Arrow: Started --OrderPlaced--> WaitingForStock.
            // Our own OrderPlaced came back through RabbitMQ (published from the outbox by the API).
            // Step 1 of the orchestration: send the ReserveStock COMMAND to Inventory's queue, and start
            // the stock deadline in case Inventory never answers.
            case (SagaStates.Started, OrderPlaced placed):
                return new SagaDecision(SagaStates.WaitingForStock, OrderState.Reserving,
                    [Send(new ReserveStock(order.Id, placed.Lines), Topology.InventoryQueue, order, incoming)],
                    "order placed → asking Inventory to reserve stock",
                    WaitFor: TimeSpan.FromSeconds(options.StockTimeoutSeconds));

            // Arrow: WaitingForStock --StockReserved--> WaitingForPayment.
            // Step 2: stock is held for us, now ask Payments to charge the wallet; start the payment deadline.
            case (SagaStates.WaitingForStock, StockReserved):
                return new SagaDecision(SagaStates.WaitingForPayment, OrderState.AwaitingPayment,
                    [Send(new ChargeWallet(order.Id, order.CustomerId, order.Amount), Topology.PaymentsQueue, order, incoming)],
                    "stock reserved → asking Payments to charge the wallet",
                    WaitFor: TimeSpan.FromSeconds(options.PaymentTimeoutSeconds));

            // Arrow: WaitingForPayment --PaymentCompleted--> Completed (S01 happy path).
            // Step 3: money taken → publish OrderConfirmed. No deadline: nothing left to wait for.
            case (SagaStates.WaitingForPayment, PaymentCompleted):
                return Confirm(order, incoming, "payment completed → order confirmed");

            // ---- rejections without anything to compensate -------------------------------------------
            // Arrow: WaitingForStock --StockReservationRejected--> Rejected (S02 insufficient stock).
            // Inventory reserved nothing and no money moved, so there is nothing to undo.
            case (SagaStates.WaitingForStock, StockReservationRejected rejected):
                return Reject(order, incoming, rejected.Reason, "stock rejected → order rejected (nothing to compensate)");

            // ---- failures after a reservation: compensate --------------------------------------------
            // Arrow: WaitingForPayment --PaymentFailed--> WaitingForRelease (S03 insufficient wallet).
            // Stock IS reserved, so we must give it back: send the compensating command ReleaseStock.
            // We do NOT reject yet; we wait for Inventory to confirm the release.
            case (SagaStates.WaitingForPayment, PaymentFailed failed):
                return StartRelease(order, incoming, failed.Reason, options, "payment failed → releasing stock");

            // Arrow: WaitingForPaymentCancellation --PaymentCancelled--> WaitingForRelease (S10).
            // After our timeout we asked Payments to cancel; it confirmed that no money was (or will be)
            // taken. Only NOW is it safe to release the stock.
            case (SagaStates.WaitingForPaymentCancellation, PaymentCancelled cancelled):
                return StartRelease(order, incoming, cancelled.Reason, options, "payment cancelled → releasing stock");

            // Arrow: WaitingForPaymentCancellation --PaymentFailed--> WaitingForRelease.
            // The original ChargeWallet finally answered "failed" (it crossed our CancelPayment on the wire).
            // Same meaning as "cancelled": no money was taken, so release the stock.
            case (SagaStates.WaitingForPaymentCancellation, PaymentFailed failed):
                return StartRelease(order, incoming, failed.Reason, options, "payment failed (answer to cancel) → releasing stock");

            // Arrow: WaitingForPaymentCancellation --PaymentAlreadyCompleted / PaymentCompleted--> Completed (S13).
            // RECONCILIATION: the payment was slow, not missing. The customer's money WAS taken, so rejecting
            // the order (and releasing their stock) would be wrong. We confirm the order instead.
            // Two stacked case labels share one body: either answer means "money was taken".
            case (SagaStates.WaitingForPaymentCancellation, PaymentAlreadyCompleted):
            case (SagaStates.WaitingForPaymentCancellation, PaymentCompleted):
                return Confirm(order, incoming, $"LATE payment reconciled: money was taken ({incoming.Type}) → order confirmed, not rejected");

            // Arrow: WaitingForRelease --StockReleased--> Rejected.
            // Inventory confirmed the compensation; the order can honestly be called Rejected now.
            // The reason stored when compensation started is carried into OrderRejected.
            case (SagaStates.WaitingForRelease, StockReleased):
                return Reject(order, incoming, saga.FailureReason ?? "compensated", "stock released → order rejected (compensation confirmed)");

            // ---- recovery from RequiresAttention (e.g. after a human re-sent the compensation) ---------
            // Arrow: RequiresAttention --StockReleased--> Rejected (S12 after the fault is removed).
            // A late or manually triggered StockReleased finally arrived: the compensation is confirmed.
            case (SagaStates.RequiresAttention, StockReleased):
                return Reject(order, incoming, saga.FailureReason ?? "compensated", "stock released after manual retry → order rejected");

            // Arrow: RequiresAttention --PaymentCancelled--> WaitingForRelease.
            // We gave up waiting for the cancellation, but it finally came: continue with the stock release.
            case (SagaStates.RequiresAttention, PaymentCancelled cancelled):
                return StartRelease(order, incoming, cancelled.Reason, options, "payment cancelled after manual retry → releasing stock");

            // Everything else: a message that makes no sense in this state (duplicate, very late, or out of
            // order). Ignore it rather than throw: throwing would only retry it and park it in the error
            // queue, and it would never become valid anyway.
            default:
                return SagaDecision.Ignore(saga, order, $"{incoming.Type} is not expected in state {saga.CurrentState}; ignored (late or out-of-order)");
        }
    }

    /// <summary>Called by the timeout worker when DeadlineUtc passed without an answer.</summary>
    /// <param name="saga">Current persisted saga state (its deadline has passed).</param>
    /// <param name="order">The order.</param>
    /// <param name="options">Timeouts and the maximum number of compensation attempts.</param>
    public static SagaDecision OnTimeout(OrderSagaState saga, Order order, SagaOptions options)
    {
        // A timeout is not caused by any message, so outgoing messages have no CausationId.
        var causation = (Guid?)null;
        switch (saga.CurrentState)
        {
            // Arrow: WaitingForStock --timeout--> WaitingForRelease.
            case SagaStates.WaitingForStock:
                // Inventory never answered. Maybe it is still processing; a ReleaseStock makes the outcome
                // deterministic: Inventory either releases the reservation or stores a tombstone that
                // rejects a late ReserveStock.
                // (Without this, a slow ReserveStock could still reserve stock AFTER we rejected the order,
                // and that stock would be locked forever.) CompensationAttempts starts counting at 1.
                return new SagaDecision(SagaStates.WaitingForRelease, OrderState.Compensating,
                    [Send(new ReleaseStock(order.Id, "stock reservation timed out"), Topology.InventoryQueue, order, causation)],
                    "stock reservation timed out → releasing (tombstone if nothing was reserved)",
                    WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
                    FailureReason: "stock reservation timed out", CompensationAttempts: 1);

            // Arrow: WaitingForPayment --timeout--> WaitingForPaymentCancellation (S10, S13).
            case SagaStates.WaitingForPayment:
                // A timeout does NOT mean "not paid". Ask Payments for a definitive answer first.
                // If we released the stock right away and the payment then succeeded (S13), the customer
                // would have paid for an order we rejected. CancelPayment forces Payments to decide:
                // either "cancelled, nothing taken" or "already completed".
                return new SagaDecision(SagaStates.WaitingForPaymentCancellation, OrderState.Compensating,
                    [Send(new CancelPayment(order.Id, "payment timed out"), Topology.PaymentsQueue, order, causation)],
                    "payment timed out → asking Payments to cancel (reconcile before compensating)",
                    WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
                    FailureReason: "payment timed out", CompensationAttempts: 1);

            // Arrow: WaitingForPaymentCancellation --timeout--> same state (re-send) or RequiresAttention.
            // The lambda builds the message to re-send only if RetryOrGiveUp decides to retry.
            case SagaStates.WaitingForPaymentCancellation:
                return RetryOrGiveUp(saga, order, options,
                    () => Send(new CancelPayment(order.Id, saga.FailureReason ?? "payment timed out"), Topology.PaymentsQueue, order, causation),
                    "CancelPayment");

            // Arrow: WaitingForRelease --timeout--> same state (re-send) or RequiresAttention (S12).
            // Re-sending is safe because Inventory handles ReleaseStock idempotently per order.
            case SagaStates.WaitingForRelease:
                return RetryOrGiveUp(saga, order, options,
                    () => Send(new ReleaseStock(order.Id, saga.FailureReason ?? "compensation"), Topology.InventoryQueue, order, causation),
                    "ReleaseStock");

            // Any other state has no deadline that could expire (defensive: should not happen).
            default:
                return SagaDecision.Ignore(saga, order, $"timeout in state {saga.CurrentState} has no effect");
        }
    }

    /// <summary>Operator action: re-send the pending compensation of a saga that is in RequiresAttention.</summary>
    /// <param name="saga">Current saga; must be in RequiresAttention.</param>
    /// <param name="order">The order.</param>
    /// <param name="options">Used for the new compensation deadline.</param>
    public static SagaDecision RetryCompensation(OrderSagaState saga, Order order, SagaOptions options)
    {
        // Guard: a manual retry on a healthy or finished saga would send a bogus ReleaseStock.
        if (saga.CurrentState != SagaStates.RequiresAttention)
            return SagaDecision.Ignore(saga, order, "retry is only allowed in RequiresAttention");

        // Arrow: RequiresAttention --manual retry--> WaitingForRelease, with a fresh attempt counter (1),
        // so the automatic retry/give-up logic gets a full new round.
        return new SagaDecision(SagaStates.WaitingForRelease, OrderState.Compensating,
            [Send(new ReleaseStock(order.Id, saga.FailureReason ?? "manual retry"), Topology.InventoryQueue, order, (Guid?)null)],
            "manual retry → re-sending ReleaseStock",
            WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds),
            CompensationAttempts: 1);
    }

    // ---- helpers -------------------------------------------------------------------------------------

    /// <summary>Terminal success: saga Completed, order Confirmed, publish the OrderConfirmed event. No deadline.</summary>
    /// <param name="order">The order.</param>
    /// <param name="incoming">The message that caused the confirmation (becomes the CausationId).</param>
    /// <param name="detail">Explanation for the audit.</param>
    private static SagaDecision Confirm(Order order, Message incoming, string detail) =>
        new(SagaStates.Completed, OrderState.Confirmed,
            [Publish(new OrderConfirmed(order.Id, order.CustomerId, order.Amount), order, incoming)], detail);

    /// <summary>Terminal failure: saga Rejected, order Rejected, publish the OrderRejected event with the reason.</summary>
    /// <param name="order">The order.</param>
    /// <param name="incoming">The causing message.</param>
    /// <param name="reason">Business reason shown to the customer (e.g. "insufficient stock").</param>
    /// <param name="detail">Explanation for the audit.</param>
    private static SagaDecision Reject(Order order, Message incoming, string reason, string detail) =>
        new(SagaStates.Rejected, OrderState.Rejected,
            [Publish(new OrderRejected(order.Id, order.CustomerId, reason), order, incoming)], detail, FailureReason: reason);

    /// <summary>
    /// Start compensating: send ReleaseStock to Inventory, wait CompensationTimeoutSeconds for StockReleased,
    /// remember the reason and set the attempt counter to 1.
    /// </summary>
    /// <param name="order">The order.</param>
    /// <param name="incoming">The causing message (e.g. PaymentFailed).</param>
    /// <param name="reason">Why we compensate; later carried into OrderRejected.</param>
    /// <param name="options">For the compensation deadline.</param>
    /// <param name="detail">Explanation for the audit.</param>
    private static SagaDecision StartRelease(Order order, Message incoming, string reason, SagaOptions options, string detail) =>
        new(SagaStates.WaitingForRelease, OrderState.Compensating,
            [Send(new ReleaseStock(order.Id, reason), Topology.InventoryQueue, order, incoming)], detail,
            WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds), FailureReason: reason, CompensationAttempts: 1);

    /// <summary>
    /// A compensation command got no answer in time. Re-send it while attempts remain; otherwise stop and
    /// flag the order as RequiresAttention (S12).
    /// </summary>
    /// <param name="saga">Current saga (its CompensationAttempts is the number of sends so far).</param>
    /// <param name="order">The order.</param>
    /// <param name="options">MaxCompensationAttempts and the compensation timeout.</param>
    /// <param name="resend">Builds the command to re-send (called only when we retry).</param>
    /// <param name="commandName">Name for the audit text ("ReleaseStock" / "CancelPayment").</param>
    private static SagaDecision RetryOrGiveUp(OrderSagaState saga, Order order, SagaOptions options, Func<OutgoingMessage> resend, string commandName)
    {
        // Attempts left: stay in the same state, send the command again (a NEW MessageId, so the receiver's
        // Inbox does not drop it as a duplicate) and start a new deadline.
        if (saga.CompensationAttempts < options.MaxCompensationAttempts)
        {
            var attempt = saga.CompensationAttempts + 1;
            return new SagaDecision(saga.CurrentState, OrderState.Compensating, [resend()],
                $"no answer to {commandName} → re-sending (attempt {attempt} of {options.MaxCompensationAttempts})",
                WaitFor: TimeSpan.FromSeconds(options.CompensationTimeoutSeconds), CompensationAttempts: attempt);
        }

        // No attempts left: be honest. We cannot prove the stock was released / the payment cancelled, so
        // calling the order "Rejected" would be a lie. Publish OrderRequiresAttention so a human looks at it.
        // WaitFor stays null → the deadline is cleared and the timeout worker stops retrying.
        var reason = $"{commandName} was not confirmed after {options.MaxCompensationAttempts} attempts";
        return new SagaDecision(SagaStates.RequiresAttention, OrderState.RequiresAttention,
            [Publish(new OrderRequiresAttention(order.Id, order.CustomerId, reason), order, (Guid?)null)],
            $"{reason} → RequiresAttention (NOT silently rejected)", FailureReason: saga.FailureReason);
    }

    // Send = COMMAND to one queue; Publish = EVENT to all subscribers. Each has two overloads: one takes the
    // causing message (and uses its MessageId as CausationId), the other a raw causation id (null for
    // timeouts and manual actions).

    /// <summary>Wraps a command for a specific queue; the causing message's id becomes the CausationId.</summary>
    private static OutgoingMessage Send(Message command, string queue, Order order, Message cause) => Send(command, queue, order, cause.MessageId);
    /// <summary>Wraps a command for a specific queue with an explicit (possibly null) CausationId.</summary>
    private static OutgoingMessage Send(Message command, string queue, Order order, Guid? causationId) =>
        new(Stamp(command, order, causationId), queue);

    /// <summary>Wraps an event to publish; the causing message's id becomes the CausationId.</summary>
    private static OutgoingMessage Publish(Message @event, Order order, Message cause) => Publish(@event, order, cause.MessageId);
    /// <summary>Wraps an event to publish with an explicit (possibly null) CausationId. Destination null = publish.</summary>
    private static OutgoingMessage Publish(Message @event, Order order, Guid? causationId) =>
        new(Stamp(@event, order, causationId), null);

    /// <summary>Fill the envelope: correlation = order, causation = the message we are reacting to.</summary>
    /// <remarks>
    /// Messages are immutable records, so "with { ... }" creates a COPY with these fields set.
    /// CorrelationId lets anyone trace every message of one order; CausationId builds the parent/child
    /// chain shown in the developer console; ScenarioId keeps Scenario Lab fault rules scoped to one run.
    /// </remarks>
    private static Message Stamp(Message message, Order order, Guid? causationId) => message with
    {
        CorrelationId = order.Id,
        CausationId = causationId,
        ScenarioId = order.ScenarioId,
        Producer = ServiceNames.Orders,
        OccurredAtUtc = DateTime.UtcNow,
    };
}
