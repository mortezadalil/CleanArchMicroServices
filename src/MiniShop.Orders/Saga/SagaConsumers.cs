using MassTransit;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Orders.Data;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// One consumer class per event the saga listens to.  They are deliberately tiny: the Inbox check,
/// transaction and telemetry live in <see cref="IdempotentConsumer{TMessage,TDbContext}"/>, and the
/// decision lives in <see cref="OrderSaga"/>.
/// </summary>
public abstract class SagaEventConsumer<TEvent>(OrdersDbContext db, SagaRunner runner, DiagnosticsPublisher diagnostics,
    OutboxSignal signal, FaultRegistry faults, ILogger logger) : IdempotentConsumer<TEvent, OrdersDbContext>(db, diagnostics, signal, faults, logger)
    where TEvent : Message
{
    /// <summary>Fault point for experiments on the orchestrator itself (e.g. crash while handling an event).</summary>
    protected override string FaultPoint => FaultPoints.OrdersSaga;

    protected override Task<HandleResult> HandleAsync(ConsumeContext<TEvent> context) =>
        runner.HandleMessageAsync(context.Message, context.CancellationToken);
}

public sealed class OrderPlacedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<OrderPlacedConsumer> l)
    : SagaEventConsumer<OrderPlaced>(db, r, d, s, f, l);

public sealed class StockReservedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReservedConsumer> l)
    : SagaEventConsumer<StockReserved>(db, r, d, s, f, l);

public sealed class StockReservationRejectedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReservationRejectedConsumer> l)
    : SagaEventConsumer<StockReservationRejected>(db, r, d, s, f, l);

public sealed class StockReleasedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<StockReleasedConsumer> l)
    : SagaEventConsumer<StockReleased>(db, r, d, s, f, l);

public sealed class PaymentCompletedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentCompletedConsumer> l)
    : SagaEventConsumer<PaymentCompleted>(db, r, d, s, f, l);

public sealed class PaymentFailedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentFailedConsumer> l)
    : SagaEventConsumer<PaymentFailed>(db, r, d, s, f, l);

public sealed class PaymentCancelledConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentCancelledConsumer> l)
    : SagaEventConsumer<PaymentCancelled>(db, r, d, s, f, l);

public sealed class PaymentAlreadyCompletedConsumer(OrdersDbContext db, SagaRunner r, DiagnosticsPublisher d, OutboxSignal s, FaultRegistry f, ILogger<PaymentAlreadyCompletedConsumer> l)
    : SagaEventConsumer<PaymentAlreadyCompleted>(db, r, d, s, f, l);
