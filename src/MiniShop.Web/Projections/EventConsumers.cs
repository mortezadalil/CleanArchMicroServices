using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;
using MiniShop.Web.Data;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Projections;

/// <summary>
/// Consumes one BUSINESS event: appends it to EventAudit, applies the projection and advances the
/// checkpoint — in one transaction, de-duplicated by the ProjectionInbox (the inherited Inbox check).
/// Fault point Web.Projection: Delay (S08 projection lag) and Fail (S16 projection crash + recovery).
/// </summary>
public abstract class ProjectionConsumer<TEvent>(ReadModelDbContext db, Projector projector, FaultRegistry faults, UiNotifier notifier,
    DiagnosticsPublisher diagnostics, OutboxSignal signal, ILogger logger)
    : IdempotentConsumer<TEvent, ReadModelDbContext>(db, diagnostics, signal, faults, logger)
    where TEvent : Message
{
    protected override string FaultPoint => FaultPoints.WebProjection;

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<TEvent> context)
    {
        var @event = context.Message;
        var ct = context.CancellationToken;

        var detail = await projector.ApplyAsync(@event, ct);
        var now = DateTime.UtcNow;

        Db.EventAudit.Add(new EventAudit
        {
            Category = "Business",
            Kind = @event.Type,
            Service = @event.Producer,
            MessageId = @event.MessageId,
            CorrelationId = @event.CorrelationId,
            CausationId = @event.CausationId,
            ScenarioId = @event.ScenarioId,
            OccurredAtUtc = @event.OccurredAtUtc,
            RecordedAtUtc = now,
            Attempt = context.GetRetryAttempt() + 1,
            Detail = detail,
            PayloadJson = Json.Serialize(@event, @event.GetType()),
        });

        var checkpoint = await Db.Checkpoints.FirstOrDefaultAsync(c => c.Projection == "ReadModel", ct)
                         ?? Db.Checkpoints.Add(new ProjectionCheckpoint { Projection = "ReadModel" }).Entity;
        checkpoint.LastEventOccurredAtUtc = @event.OccurredAtUtc;
        checkpoint.LastAppliedAtUtc = now;
        checkpoint.AppliedCount++;
        checkpoint.LastLagMs = (now - @event.OccurredAtUtc).TotalMilliseconds;

        return new HandleResult(detail)
        {
            AfterCommit = () =>
            {
                Diagnostics.Emit(DiagnosticKind.ProjectionUpdated, @event, detail: detail, durationMs: checkpoint.LastLagMs);
                notifier.Notify("readmodel");
            },
        };
    }
}

// One tiny class per event type so MassTransit can bind each exchange to the Web queue.
public sealed class ProductCatalogChangedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<ProductCatalogChangedProjection> l) : ProjectionConsumer<ProductCatalogChanged>(db, p, f, n, d, s, l);
public sealed class WalletBalanceChangedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<WalletBalanceChangedProjection> l) : ProjectionConsumer<WalletBalanceChanged>(db, p, f, n, d, s, l);
public sealed class OrderPlacedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderPlacedProjection> l) : ProjectionConsumer<OrderPlaced>(db, p, f, n, d, s, l);
public sealed class StockReservedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReservedProjection> l) : ProjectionConsumer<StockReserved>(db, p, f, n, d, s, l);
public sealed class StockReservationRejectedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReservationRejectedProjection> l) : ProjectionConsumer<StockReservationRejected>(db, p, f, n, d, s, l);
public sealed class StockReleasedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReleasedProjection> l) : ProjectionConsumer<StockReleased>(db, p, f, n, d, s, l);
public sealed class PaymentCompletedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentCompletedProjection> l) : ProjectionConsumer<PaymentCompleted>(db, p, f, n, d, s, l);
public sealed class PaymentFailedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentFailedProjection> l) : ProjectionConsumer<PaymentFailed>(db, p, f, n, d, s, l);
public sealed class PaymentCancelledProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentCancelledProjection> l) : ProjectionConsumer<PaymentCancelled>(db, p, f, n, d, s, l);
public sealed class PaymentAlreadyCompletedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentAlreadyCompletedProjection> l) : ProjectionConsumer<PaymentAlreadyCompleted>(db, p, f, n, d, s, l);
public sealed class OrderConfirmedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderConfirmedProjection> l) : ProjectionConsumer<OrderConfirmed>(db, p, f, n, d, s, l);
public sealed class OrderRejectedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderRejectedProjection> l) : ProjectionConsumer<OrderRejected>(db, p, f, n, d, s, l);
public sealed class OrderRequiresAttentionProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderRequiresAttentionProjection> l) : ProjectionConsumer<OrderRequiresAttention>(db, p, f, n, d, s, l);

/// <summary>
/// Consumes DIAGNOSTIC records on the telemetry queue: appends them to EventAudit and keeps the
/// SagaSnapshot table current.  Diagnostics never change the business projections.
/// </summary>
public sealed class DiagnosticEventConsumer(ReadModelDbContext db, UiNotifier notifier, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<DiagnosticEventConsumer> logger)
    : IdempotentConsumer<DiagnosticEvent, ReadModelDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override bool EmitTelemetry => false;   // never report telemetry about telemetry

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<DiagnosticEvent> context)
    {
        var d = context.Message;
        var ct = context.CancellationToken;

        Db.EventAudit.Add(new EventAudit
        {
            Category = "Diagnostic",
            Kind = d.Kind,
            Service = d.Service,
            MessageId = d.MessageId,
            SubjectMessageId = d.SubjectMessageId,
            SubjectMessageType = d.SubjectMessageType,
            CorrelationId = d.CorrelationId,
            CausationId = d.CausationId,
            ScenarioId = d.ScenarioId,
            OccurredAtUtc = d.OccurredAtUtc,
            RecordedAtUtc = DateTime.UtcNow,
            Attempt = d.Attempt,
            DurationMs = d.DurationMs,
            Outcome = d.Outcome,
            Error = d.Error,
            Detail = d.Detail,
            PayloadJson = d.PayloadJson,
            SnapshotJson = d.SnapshotJson,
        });

        if (d.Kind == DiagnosticKind.SagaTransitioned && d.Outcome == "Transitioned")
        {
            var snapshot = await Db.SagaSnapshots.FirstOrDefaultAsync(s => s.CorrelationId == d.CorrelationId, ct)
                           ?? Db.SagaSnapshots.Add(new SagaSnapshot { CorrelationId = d.CorrelationId }).Entity;
            if (d.OccurredAtUtc >= snapshot.UpdatedAtUtc)
            {
                snapshot.CurrentState = ExtractState(d.SnapshotJson) ?? snapshot.CurrentState;
                snapshot.LastTransition = d.Detail;
                snapshot.SnapshotJson = d.SnapshotJson;
                snapshot.UpdatedAtUtc = d.OccurredAtUtc;
            }
            snapshot.Transitions++;
        }

        return new HandleResult($"audit: {d.Kind}") { AfterCommit = () => notifier.Notify("audit") };
    }

    private static string? ExtractState(string? snapshotJson)
    {
        if (snapshotJson is null) return null;
        using var doc = System.Text.Json.JsonDocument.Parse(snapshotJson);
        return doc.RootElement.TryGetProperty("saga", out var saga) && saga.TryGetProperty("currentState", out var state)
            ? state.GetString()
            : null;
    }
}
