// ==================================================================================================
// MiniShop.Orders — Saga/SagaConsumers.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The MassTransit consumers through which events enter the saga: one tiny class per event type.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   RabbitMQ → orders queue (Topology.OrdersQueue) → MassTransit picks the consumer by message type →
//   IdempotentConsumer.Consume (shared/Messaging: Inbox check + transaction) → HandleAsync below →
//   SagaRunner.HandleMessageAsync → OrderSaga.Decide (pure decision) → SagaRunner.Apply (writes) → COMMIT.
//
// WHY ONE CLASS PER EVENT?
//   MassTransit routes by CLR type: an IConsumer<StockReserved> receives StockReserved messages. A
//   generic base class (SagaEventConsumer<TEvent>) holds the shared code, and each sealed subclass only
//   closes the generic type. That keeps every consumer to one line while still giving MassTransit a
//   concrete type to register (see the AddConsumer<...> calls in Program.cs).
//
// PATTERN
//   Inbox / idempotent consumer: RabbitMQ delivers "at least once", so the same message can arrive twice
//   (S06 duplicate delivery, S11 crash after publish). The base class records every MessageId in the
//   Inbox in the same transaction as the saga change, so a duplicate is detected and ignored.
//
// Read next: Saga/SagaRunner.cs.
// ==================================================================================================

// ConsumeContext<T> (the incoming message plus its delivery details).
using MassTransit;
// ILogger.
using Microsoft.Extensions.Logging;
// Message base record and all event contracts.
using MiniShop.Contracts;
// OrdersDbContext.
using MiniShop.Orders.Data;
// DiagnosticsPublisher.
using MiniShop.Shared.Diagnostics;
// FaultRegistry and FaultPoints.
using MiniShop.Shared.Faults;
// IdempotentConsumer and HandleResult.
using MiniShop.Shared.Messaging;
// OutboxSignal.
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// One consumer class per event the saga listens to.  They are deliberately tiny: the Inbox check,
/// transaction and telemetry live in <see cref="IdempotentConsumer{TMessage,TDbContext}"/>, and the
/// decision lives in <see cref="OrderSaga"/>.
/// </summary>
/// <typeparam name="TEvent">The event type this consumer handles (must derive from the common <see cref="Message"/> envelope).</typeparam>
/// <param name="db">The scoped DbContext of this delivery; shared with <paramref name="runner"/>, so both write into ONE transaction.</param>
/// <param name="runner">Loads the saga, asks OrderSaga for a decision and stages the changes.</param>
/// <param name="diagnostics">Telemetry for the developer console.</param>
/// <param name="signal">Wakes the outbox dispatcher after commit.</param>
/// <param name="faults">Scenario Lab fault rules (e.g. crash while the orchestrator handles an event).</param>
/// <param name="logger">Logger of the concrete consumer type.</param>
public abstract class SagaEventConsumer<TEvent>(OrdersDbContext db, SagaRunner runner, DiagnosticsPublisher diagnostics,
    OutboxSignal signal, FaultRegistry faults, ILogger logger) : IdempotentConsumer<TEvent, OrdersDbContext>(db, diagnostics, signal, faults, logger)
    where TEvent : Message
{
    /// <summary>Fault point for experiments on the orchestrator itself (e.g. crash while handling an event).</summary>
    protected override string FaultPoint => FaultPoints.OrdersSaga;

    /// <summary>
    /// Called by the base class INSIDE its transaction, after the Inbox check passed. It only stages
    /// changes on the DbContext; the base class adds the Inbox row, saves and commits.
    /// </summary>
    /// <param name="context">The delivery; we only need the message and the cancellation token.</param>
    protected override Task<HandleResult> HandleAsync(ConsumeContext<TEvent> context) =>
        runner.HandleMessageAsync(context.Message, context.CancellationToken);
}

// ---- concrete consumers ------------------------------------------------------------------------------
// Each one only fixes TEvent and asks DI for a typed ILogger<T> (so log lines show the consumer name).
// The short parameter names (r, d, s, f, l) are just to keep each declaration on two lines.

/// <summary>Receives our own OrderPlaced (published via the outbox by the API) and starts the saga: Started → WaitingForStock.</summary>
public sealed class OrderPlacedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<OrderPlacedConsumer> l)
    : SagaEventConsumer<OrderPlaced>(db, r, d, s, f, l);

/// <summary>Inventory reserved the stock: WaitingForStock → WaitingForPayment.</summary>
public sealed class StockReservedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReservedConsumer> l)
    : SagaEventConsumer<StockReserved>(db, r, d, s, f, l);

/// <summary>Inventory refused (not enough stock, S02): WaitingForStock → Rejected.</summary>
public sealed class StockReservationRejectedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReservationRejectedConsumer> l)
    : SagaEventConsumer<StockReservationRejected>(db, r, d, s, f, l);

/// <summary>Inventory confirmed the compensation: WaitingForRelease (or RequiresAttention) → Rejected.</summary>
public sealed class StockReleasedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReleasedConsumer> l)
    : SagaEventConsumer<StockReleased>(db, r, d, s, f, l);

/// <summary>Payments charged the wallet: WaitingForPayment → Completed (or the late-payment case of S13).</summary>
public sealed class PaymentCompletedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentCompletedConsumer> l)
    : SagaEventConsumer<PaymentCompleted>(db, r, d, s, f, l);

/// <summary>Payments could not charge (e.g. insufficient funds, S03): start compensating with ReleaseStock.</summary>
public sealed class PaymentFailedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentFailedConsumer> l)
    : SagaEventConsumer<PaymentFailed>(db, r, d, s, f, l);

/// <summary>Payments confirmed that no money was taken after CancelPayment (S10): safe to release the stock.</summary>
public sealed class PaymentCancelledConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentCancelledConsumer> l)
    : SagaEventConsumer<PaymentCancelled>(db, r, d, s, f, l);

/// <summary>Payments answered CancelPayment with "too late, the money was already taken" (S13): confirm the order.</summary>
public sealed class PaymentAlreadyCompletedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentAlreadyCompletedConsumer> l)
    : SagaEventConsumer<PaymentAlreadyCompleted>(db, r, d, s, f, l);
