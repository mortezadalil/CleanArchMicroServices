// ==================================================================================================
// FILE: src/MiniShop.Web/Projections/EventConsumers.cs
//
// WHAT IT IS
//   The MassTransit consumers of MiniShop.Web: the entry point of every message the Web receives.
//     - ProjectionConsumer<TEvent> + 13 tiny subclasses: BUSINESS events (queue minishop.web).
//     - DiagnosticEventConsumer: TELEMETRY records (queue minishop.web.diagnostics).
//
// WHERE IT SITS IN THE ARCHITECTURE
//   CQRS read side.  Orders / Inventory / Payments publish events through their transactional outboxes;
//   RabbitMQ copies each event to every subscribed queue; these consumers turn them into rows of
//   readmodel.db.  The Web never asks another service's database; this is its only source of data.
//
// PATTERNS
//   - Idempotent consumer + Inbox: both classes inherit shared/Messaging/IdempotentConsumer.cs, which
//     wraps HandleAsync in ONE transaction: "already in the ProjectionInbox? skip" → HandleAsync →
//     insert MessageId into the inbox → commit.  Read that file first if you have not yet.
//   - Append-only audit log (EventAudit) written in the same transaction as the projection.
//   - Checkpoint / lag measurement (eventual consistency made visible).
//
// SCENARIOS
//   S08 slow projection: a Delay fault on point Web.Projection makes each event wait → the minishop.web
//       queue builds a backlog and LastLagMs grows, while the Developer Console (other queue) stays live.
//   S16 projection crash: a Fail fault makes HandleAsync throw → nothing is committed, MassTransit
//       retries, and once the fault is removed the read model catches up with nothing lost or doubled.
//
// Read next: Projector.cs (what ApplyAsync does), then UiNotifier.cs.
// ==================================================================================================

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
///
/// Why one transaction?  If the projection were saved but the audit row or the inbox row were not
/// (or the other way round), a redelivery would apply the event twice, or a rebuild would miss it.
/// All-or-nothing keeps the three in step.
/// The Web has no outbox table, so the inherited OutboxSignal is passed along but never woken here.
/// </summary>
/// <typeparam name="TEvent">The business event type this consumer subscribes to.</typeparam>
public abstract class ProjectionConsumer<TEvent>(ReadModelDbContext db, Projector projector, FaultRegistry faults, UiNotifier notifier,
    DiagnosticsPublisher diagnostics, OutboxSignal signal, ILogger logger)
    : IdempotentConsumer<TEvent, ReadModelDbContext>(db, diagnostics, signal, faults, logger)
    where TEvent : Message
{
    /// <summary>
    /// Fault-injection point of every projection.  The base class applies a matching rule BEFORE the
    /// transaction starts, so an injected delay (S08) never holds the SQLite write lock.
    /// </summary>
    protected override string FaultPoint => FaultPoints.WebProjection;

    /// <summary>
    /// The business work for one event.  Runs INSIDE the base class transaction, after the inbox check;
    /// it only adds/changes tracked entities and never calls SaveChanges itself.
    /// </summary>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<TEvent> context)
    {
        // "event" is a C# keyword, hence the @ prefix.
        var @event = context.Message;
        var ct = context.CancellationToken;

        // 1. Update the read-model tables (upsert + ordering guards).  Returns a description for the timeline.
        var detail = await projector.ApplyAsync(@event, ct);
        // One "now" for both the audit row and the checkpoint, so their timestamps agree.
        var now = DateTime.UtcNow;

        // 2. Append the event to the audit log WITH its full JSON payload.  RabbitMQ forgets a message
        //    once it is acknowledged, so this row is what the timeline shows and what a rebuild replays.
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
            // GetRetryAttempt() is 0 on the first try; +1 makes it human-friendly ("attempt 1").
            Attempt = context.GetRetryAttempt() + 1,
            Detail = detail,
            // Serialise with the RUNTIME type so every property of the concrete event is kept; the
            // rebuild deserialises it back using Kind (see ContractTypes.Resolve).
            PayloadJson = Json.Serialize(@event, @event.GetType()),
        });

        // 3. Advance the checkpoint: load the single "ReadModel" row or create it on the very first event.
        var checkpoint = await Db.Checkpoints.FirstOrDefaultAsync(c => c.Projection == "ReadModel", ct)
                         ?? Db.Checkpoints.Add(new ProjectionCheckpoint { Projection = "ReadModel" }).Entity;
        checkpoint.LastEventOccurredAtUtc = @event.OccurredAtUtc;
        checkpoint.LastAppliedAtUtc = now;
        checkpoint.AppliedCount++;
        // Lag = how long after it happened did the read side apply it?  The CQRS Explorer shows this;
        // in S08 you watch it climb.
        checkpoint.LastLagMs = (now - @event.OccurredAtUtc).TotalMilliseconds;

        // 4. Side effects that must happen only AFTER the commit succeeded.  If they ran inside the
        //    transaction and the commit then failed, the UI would show data that does not exist.
        return new HandleResult(detail)
        {
            AfterCommit = () =>
            {
                // Telemetry: "the Web projected this event" (appears in the timeline as ProjectionUpdated).
                Diagnostics.Emit(DiagnosticKind.ProjectionUpdated, @event, detail: detail, durationMs: checkpoint.LastLagMs);
                // Wake up every open Blazor page that shows read-model data, so it re-queries and re-renders.
                notifier.Notify("readmodel");
            },
        };
    }
}

// One tiny class per event type so MassTransit can bind each exchange to the Web queue.
// MassTransit consumers are registered per CLOSED type: it needs a concrete IConsumer<OrderPlaced>, etc.
// to know which exchange (one per message type) must be bound to minishop.web.  The generic base class
// holds all the logic; these subclasses only fix TEvent and forward the constructor dependencies.
// The ILogger<...> of each gives log lines a precise category.  Every class here must also appear in
// the ProjectionConsumers list in Program.cs, or it never receives anything.

/// <summary>Projects ProductCatalogChanged (Inventory: full current state of one product) into ProductCards.</summary>
public sealed class ProductCatalogChangedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<ProductCatalogChangedProjection> l) : ProjectionConsumer<ProductCatalogChanged>(db, p, f, n, d, s, l);
/// <summary>Projects WalletBalanceChanged (Payments: the FINAL balance, not a delta) into WalletBalances.</summary>
public sealed class WalletBalanceChangedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<WalletBalanceChangedProjection> l) : ProjectionConsumer<WalletBalanceChanged>(db, p, f, n, d, s, l);
/// <summary>Projects OrderPlaced: creates (or completes) the OrderSummary row in state Pending.</summary>
public sealed class OrderPlacedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderPlacedProjection> l) : ProjectionConsumer<OrderPlaced>(db, p, f, n, d, s, l);
/// <summary>Projects StockReserved: order moves to AwaitingPayment.</summary>
public sealed class StockReservedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReservedProjection> l) : ProjectionConsumer<StockReserved>(db, p, f, n, d, s, l);
/// <summary>Projects StockReservationRejected: order moves to Rejecting, with the reason.</summary>
public sealed class StockReservationRejectedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReservationRejectedProjection> l) : ProjectionConsumer<StockReservationRejected>(db, p, f, n, d, s, l);
/// <summary>Audits StockReleased (compensation step).  No table change: it only feeds the timeline.</summary>
public sealed class StockReleasedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<StockReleasedProjection> l) : ProjectionConsumer<StockReleased>(db, p, f, n, d, s, l);
/// <summary>Audits PaymentCompleted.  No table change (the wallet is updated by WalletBalanceChanged).</summary>
public sealed class PaymentCompletedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentCompletedProjection> l) : ProjectionConsumer<PaymentCompleted>(db, p, f, n, d, s, l);
/// <summary>Projects PaymentFailed: order moves to Compensating, with the reason.</summary>
public sealed class PaymentFailedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentFailedProjection> l) : ProjectionConsumer<PaymentFailed>(db, p, f, n, d, s, l);
/// <summary>Projects PaymentCancelled: order moves to Compensating, with the reason.</summary>
public sealed class PaymentCancelledProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentCancelledProjection> l) : ProjectionConsumer<PaymentCancelled>(db, p, f, n, d, s, l);
/// <summary>Audits PaymentAlreadyCompleted (Payments refused a second debit for the same order).  Timeline only.</summary>
public sealed class PaymentAlreadyCompletedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<PaymentAlreadyCompletedProjection> l) : ProjectionConsumer<PaymentAlreadyCompleted>(db, p, f, n, d, s, l);
/// <summary>Projects OrderConfirmed: terminal state Confirmed.</summary>
public sealed class OrderConfirmedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderConfirmedProjection> l) : ProjectionConsumer<OrderConfirmed>(db, p, f, n, d, s, l);
/// <summary>Projects OrderRejected: terminal state Rejected, with the reason.</summary>
public sealed class OrderRejectedProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderRejectedProjection> l) : ProjectionConsumer<OrderRejected>(db, p, f, n, d, s, l);
/// <summary>Projects OrderRequiresAttention: terminal state RequiresAttention (a human must look).</summary>
public sealed class OrderRequiresAttentionProjection(ReadModelDbContext db, Projector p, FaultRegistry f, UiNotifier n, DiagnosticsPublisher d, OutboxSignal s, ILogger<OrderRequiresAttentionProjection> l) : ProjectionConsumer<OrderRequiresAttention>(db, p, f, n, d, s, l);

/// <summary>
/// Consumes DIAGNOSTIC records on the telemetry queue: appends them to EventAudit and keeps the
/// SagaSnapshot table current.  Diagnostics never change the business projections.
///
/// Every service (the Web included) publishes DiagnosticEvent records: "message published", "consumed",
/// "duplicate ignored", "saga transitioned"...  They live on their own queue (minishop.web.diagnostics,
/// see Program.cs) so a slow or crashing projection (S08, S16) never stops the Developer Console.
/// It still inherits IdempotentConsumer, so a redelivered record is not appended twice.
/// </summary>
public sealed class DiagnosticEventConsumer(ReadModelDbContext db, UiNotifier notifier, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<DiagnosticEventConsumer> logger)
    : IdempotentConsumer<DiagnosticEvent, ReadModelDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>
    /// False: consuming a telemetry record must NOT emit a new telemetry record.  Otherwise "I consumed
    /// record X" becomes record Y, which is consumed and becomes record Z... an endless feedback loop on
    /// the same queue.  This feedback loop really happened during development and once grew a single
    /// message to 87 MB.  No FaultPoint either: telemetry is not part of any fault scenario.
    /// </summary>
    protected override bool EmitTelemetry => false;   // never report telemetry about telemetry

    /// <summary>Appends the record to EventAudit and, for saga transitions, updates the SagaSnapshot.</summary>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<DiagnosticEvent> context)
    {
        var d = context.Message;
        var ct = context.CancellationToken;

        // Copy the record as-is into the append-only audit log, Category "Diagnostic".  The timeline, the
        // Message Explorer, the Outbox/Inbox counters and the Scenario Lab evidence all read these rows.
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

        // The Web cannot read the saga table in orders.db, so the saga inspector is built from the
        // SagaTransitioned records Orders publishes.  Only real transitions update the snapshot.
        if (d.Kind == DiagnosticKind.SagaTransitioned && d.Outcome == "Transitioned")
        {
            // Upsert the snapshot row for this saga instance.
            var snapshot = await Db.SagaSnapshots.FirstOrDefaultAsync(s => s.CorrelationId == d.CorrelationId, ct)
                           ?? Db.SagaSnapshots.Add(new SagaSnapshot { CorrelationId = d.CorrelationId }).Entity;
            // Ordering guard: telemetry arrives out of order too (prefetch 16, concurrent consumers), so an
            // older transition must not overwrite the state of a newer one.
            if (d.OccurredAtUtc >= snapshot.UpdatedAtUtc)
            {
                // Keep the old state if the snapshot JSON has no readable state.
                snapshot.CurrentState = ExtractState(d.SnapshotJson) ?? snapshot.CurrentState;
                snapshot.LastTransition = d.Detail;
                snapshot.SnapshotJson = d.SnapshotJson;
                snapshot.UpdatedAtUtc = d.OccurredAtUtc;
            }
            // Count every transition, even an out-of-order one: it still happened.
            snapshot.Transitions++;
        }

        // After commit, tell the pages that show the audit (timeline, explorer, console) to refresh.
        return new HandleResult($"audit: {d.Kind}") { AfterCommit = () => notifier.Notify("audit") };
    }

    /// <summary>
    /// Reads <c>saga.currentState</c> from the snapshot JSON published by Orders, or returns null when
    /// there is no snapshot or the property is missing.
    /// </summary>
    private static string? ExtractState(string? snapshotJson)
    {
        if (snapshotJson is null) return null;
        // JsonDocument is read-only and pooled; "using" returns its buffers when done.
        using var doc = System.Text.Json.JsonDocument.Parse(snapshotJson);
        return doc.RootElement.TryGetProperty("saga", out var saga) && saga.TryGetProperty("currentState", out var state)
            ? state.GetString()
            : null;
    }
}
