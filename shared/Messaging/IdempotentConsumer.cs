// ==========================================================================================
// FILE: shared/Messaging/IdempotentConsumer.cs
//
// WHAT THIS FILE IS
//   The base class of EVERY message consumer in MiniShop (Inventory, Payments, Web projections,
//   and the Orders saga consumers).  A concrete consumer only writes its business logic in
//   HandleAsync; everything that makes message processing SAFE lives here, in one place.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  It glues together:
//     - the Inbox            (shared/Inbox/InboxMessage.cs)        → de-duplication
//     - the Outbox           (shared/Outbox/OutboxMessage.cs)      → reliable outgoing messages
//     - fault injection      (shared/Faults/FaultRegistry.cs)      → Scenario Lab experiments
//     - telemetry            (shared/Diagnostics/DiagnosticsPublisher.cs) → Developer Console
//
// WHICH PATTERN IT IMPLEMENTS
//   "Idempotent consumer" / Inbox pattern, combined with the Transactional Outbox:
//     inbox check + business change + outbox rows + inbox insert  =  ONE database transaction.
//
// WHY IT IS NEEDED
//   RabbitMQ + MassTransit give AT-LEAST-ONCE delivery: a message can arrive twice (the outbox
//   re-publishes after a crash S11, a duplicate is injected S06, the broker redelivers after a
//   consumer crash).  Processing "reserve 1 unit" twice would reserve 2 units.  Recording the
//   MessageId in the SAME transaction as the business write means: either both happened, or
//   neither — so a retry or a duplicate can always tell whether the work is already done.
//
// Read next: src/MiniShop.Inventory/Consumers/ReserveStockConsumer.cs (a concrete subclass),
//            then shared/Outbox/OutboxDispatcher.cs.
// ==========================================================================================

using System.Diagnostics;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Shared.Messaging;

/// <summary>What a handler reports back after doing its business work.</summary>
/// <param name="Detail">Human-readable summary of what the handler did ("reserved 2 units of Mouse"), shown in the console timeline.</param>
/// <param name="Snapshot">Optional object describing the service's state after the step (stock, wallet...), stored as JSON in telemetry.</param>
public sealed record HandleResult(string Detail, object? Snapshot = null)
{
    /// <summary>Optional callback run only after the transaction committed (e.g. emit a saga-transition record).</summary>
    /// <remarks>
    /// Why "after commit": if we announced "saga moved to AwaitingPayment" before the commit and the commit
    /// then failed, the console would show a transition that never happened.  <c>Action</c> is a delegate
    /// type for a method with no parameters and no return value.
    /// </remarks>
    public Action? AfterCommit { get; init; }
}

/// <summary>
/// Base class for every message consumer in MiniShop.  It implements the pattern that makes
/// at-least-once delivery safe, in ONE database transaction:
///
///   0. Apply the fault rule of this consumer, if any (OUTSIDE the transaction, so a delay holds no lock).
///   BEGIN TRANSACTION
///     1. Is MessageId already in the Inbox?  → yes: ignore (InboxDuplicateIgnored), commit nothing.
///     2. Run the business handler (changes aggregates, adds Outbox rows).
///     3. Insert the MessageId into the Inbox.
///   COMMIT
///   4. Wake the outbox dispatcher, emit telemetry.
///
/// If anything throws, nothing is written (no partial state), MassTransit retries, and after the
/// retries are exhausted the message is parked in the error queue.  The handler itself must still be
/// idempotent on BUSINESS keys (e.g. "one debit per order") because two DIFFERENT messages may ask
/// for the same operation.
/// </summary>
/// <remarks>
/// C# NOTE — primary constructor (C# 12): the parameters in parentheses after the class name
/// (<c>(TDbContext db, DiagnosticsPublisher diagnostics, ...)</c>) are constructor parameters that are
/// in scope in the whole class body.  DI fills them in; subclasses pass them on with
/// <c>: IdempotentConsumer&lt;X, Y&gt;(db, diagnostics, ...)</c>.
///
/// C# NOTE — generic constraints: <c>where TMessage : Message</c> means "TMessage can be any type, as long
/// as it derives from Message" — that is what lets us read <c>message.MessageId</c> below.
/// <c>where TDbContext : DbContext, IInboxDbContext</c> means the service's DbContext must be an EF Core
/// context AND expose an Inbox table, so <c>Db.Inbox</c> and <c>Db.Database</c> both compile.
/// </remarks>
/// <typeparam name="TMessage">The message type this consumer handles (e.g. ReserveStock).</typeparam>
/// <typeparam name="TDbContext">The service's EF Core DbContext, which owns the business tables, the Inbox and the Outbox.</typeparam>
public abstract class IdempotentConsumer<TMessage, TDbContext>(
    TDbContext db,
    DiagnosticsPublisher diagnostics,
    OutboxSignal outboxSignal,
    FaultRegistry faults,
    ILogger logger) : IConsumer<TMessage>
    where TMessage : Message
    where TDbContext : DbContext, IInboxDbContext
{
    /// <summary>The service's DbContext (scoped: one instance per message).  Subclasses add their changes to it.</summary>
    protected TDbContext Db { get; } = db;
    /// <summary>Telemetry publisher, exposed so subclasses can emit extra records (e.g. saga transitions).</summary>
    protected DiagnosticsPublisher Diagnostics { get; } = diagnostics;
    /// <summary>Logger for the concrete consumer.</summary>
    protected ILogger Logger { get; } = logger;

    /// <summary>Business logic. Add aggregate changes and outbox rows to <see cref="Db"/>; do NOT call SaveChanges.</summary>
    /// <remarks>
    /// Why no SaveChanges in the handler: the base class saves ONCE, together with the Inbox row, inside
    /// the transaction.  A handler that saved by itself would still be inside the transaction, but
    /// would split the work into several writes and make the code harder to reason about.
    /// </remarks>
    /// <param name="context">MassTransit's consume context: the message plus headers, retry info and cancellation token.</param>
    protected abstract Task<HandleResult> HandleAsync(ConsumeContext<TMessage> context);

    /// <summary>
    /// Name of the fault-injection point of this consumer (one of <see cref="FaultPoints"/>), or null.
    /// The fault is applied BEFORE the transaction starts, so an injected Delay/Pause never holds a
    /// database lock; the handler reads the outcome (Reject / Duplicate) from <see cref="Fault"/>.
    /// </summary>
    protected virtual string? FaultPoint => null;

    /// <summary>Outcome of the fault applied for the current message (None when no rule matched).</summary>
    /// <remarks><c>private set</c>: only this base class may change it; subclasses can only read it.</remarks>
    protected FaultOutcome Fault { get; private set; } = FaultOutcome.None;

    /// <summary>
    /// False for consumers of telemetry itself: recording "I consumed a diagnostic record" as a new
    /// diagnostic record would feed back into the same queue forever.
    /// </summary>
    /// <remarks>
    /// The telemetry feedback loop: DiagnosticEventConsumer (Web) consumes DiagnosticEvents.  If it emitted
    /// a MessageConsumed record for each one, that record would itself be consumed, which would emit
    /// another record, and so on — an infinite stream generated by a single event.  That consumer
    /// overrides this property with <c>false</c>.
    /// </remarks>
    protected virtual bool EmitTelemetry => true;

    /// <summary>
    /// Entry point called by MassTransit for every delivery (including each retry).  Implements the
    /// numbered steps described on the class.
    /// </summary>
    /// <param name="context">The delivery: message, headers, retry attempt and cancellation token.</param>
    public async Task Consume(ConsumeContext<TMessage> context)
    {
        // The deserialized message (OrderPlaced, ReserveStock, ...), typed thanks to the generic parameter.
        var message = context.Message;
        // GetRetryAttempt() is 0 on the first delivery, 1 on the first retry...; +1 makes it human ("attempt 1").
        var attempt = context.GetRetryAttempt() + 1;
        // Cancelled when the service shuts down; passed to every async call so shutdown is not blocked.
        var ct = context.CancellationToken;
        // Measures how long this attempt takes; shown as DurationMs in the console (S08 slow projection).
        var stopwatch = Stopwatch.StartNew();

        // Make ScenarioId/CorrelationId available as "ambient" values for deep code that cannot receive them
        // as parameters — in particular the EF SaveChanges interceptor that injects S07 failures.
        // `using var _` disposes the scope at the end of the method, restoring the previous values.
        using var _ = FaultContext.Enter(message.ScenarioId, message.CorrelationId);
        try
        {
            // STEP 0 — fault injection, deliberately BEFORE the transaction: a Delay or Pause rule (S05, S08)
            // would otherwise sit on SQLite's write lock and freeze every other writer of this service.
            // Fail throws right here (→ retry); Reject/Duplicate are returned for the handler to honour.
            Fault = FaultPoint is null ? FaultOutcome.None : await faults.ApplyAsync(FaultPoint, message, ct);

            // BEGIN IMMEDIATE on SQLite: from here on we hold the write lock, so keep this block short.
            // `await using` disposes the transaction asynchronously at the end of the try block; if we never
            // reached CommitAsync (an exception, or the duplicate early return), disposing ROLLS BACK.
            await using var transaction = await Db.Database.BeginTransactionAsync(ct);

            // STEP 1 — Inbox check.  Done INSIDE the transaction, so two deliveries of the same message
            // cannot both pass the check: the second one waits for the write lock and then sees the row.
            if (await Db.Inbox.AnyAsync(i => i.MessageId == message.MessageId, ct))
            {
                // Duplicate (S06, S11): the work was already committed earlier.  Record it for the console
                // (unless this is the telemetry consumer, see EmitTelemetry)...
                if (EmitTelemetry)
                    Diagnostics.Emit(DiagnosticKind.InboxDuplicateIgnored, message,
                        detail: $"{message.Type} {message.MessageId} was already processed; delivery ignored", attempt: attempt);
                Logger.LogInformation("Duplicate {Type} {MessageId} ignored", message.Type, message.MessageId);
                // ...and return normally: MassTransit ACKs the message, so it is removed from the queue and NOT
                // retried.  Throwing here would be wrong — the message is not broken, just repeated.
                return;
            }

            // STEP 2 — the business logic of the concrete consumer.  It only stages changes on Db
            // (aggregates + Outbox rows); nothing is written to disk yet.
            var result = await HandleAsync(context);

            // STEP 3 — remember that this MessageId is done.  It is saved in the SAME SaveChanges/transaction as
            // the business changes: if the commit fails, the inbox row disappears too and a retry is allowed.
            Db.Inbox.Add(new InboxMessage
            {
                MessageId = message.MessageId,
                MessageType = message.Type,
                CorrelationId = message.CorrelationId,
                ReceivedAtUtc = DateTime.UtcNow,
            });

            // Before saving, collect the Outbox rows the handler added (EF's change tracker knows every entity
            // in state Added).  We need them after the commit for the OutboxStored telemetry and to decide
            // whether to wake the dispatcher.  ToList() makes a copy, because after SaveChanges their state
            // becomes Unchanged and this query would return nothing.
            var outboxRows = Db.ChangeTracker.Entries<OutboxMessage>()
                .Where(e => e.State == EntityState.Added)
                .Select(e => e.Entity)
                .ToList();

            // Write everything (business rows + outbox rows + inbox row) in one go, then COMMIT.
            // Optimistic-concurrency conflicts (S15: two orders racing for the last unit) surface here as a
            // DbUpdateConcurrencyException, which lands in the catch block below and is retried with fresh data.
            await Db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);

            // --- after commit: side effects that must not influence the transaction ---
            // Wake the outbox dispatcher now instead of waiting for its 1-second poll (latency only).
            if (outboxRows.Count > 0)
                outboxSignal.Notify();
            // Run the handler's post-commit callback (e.g. saga-transition telemetry), now that it is certain.
            result.AfterCommit?.Invoke();
            // The diagnostics consumer stops here: no telemetry about telemetry (feedback loop).
            if (!EmitTelemetry)
                return;

            // One OutboxStored record per outgoing message, so the console can draw "stored, waiting for dispatch".
            foreach (var row in outboxRows)
                Diagnostics.Emit(DiagnosticKind.OutboxStored, correlationId: row.CorrelationId, scenarioId: row.ScenarioId,
                    subjectMessageId: row.MessageId, subjectMessageType: row.Type,
                    detail: row.DestinationQueue is null ? "event stored in outbox" : $"command stored in outbox → {row.DestinationQueue}");
            // And one MessageConsumed record for the message we just processed, with timing and state snapshot.
            Diagnostics.Emit(DiagnosticKind.MessageConsumed, message, detail: result.Detail, attempt: attempt,
                durationMs: stopwatch.Elapsed.TotalMilliseconds, outcome: "Success", snapshot: result.Snapshot);
        }
        // Exception FILTER (`when`): catch everything EXCEPT cancellation.  On shutdown we do not want to log a
        // scary "failed" record; the message simply stays in the queue and is delivered after the restart (S09).
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Is there another attempt left?  This only decides which telemetry kind to emit; the actual retry
            // is done by MassTransit's retry middleware (UseMiniShopDefaults) when we rethrow.
            var willRetry = attempt < RetryPolicy.MaxAttempts;
            if (EmitTelemetry)
                Diagnostics.Emit(willRetry ? DiagnosticKind.ConsumerFailed : DiagnosticKind.MessageDeadLettered, message,
                detail: willRetry
                    ? $"attempt {attempt} of {RetryPolicy.MaxAttempts} failed; retry scheduled"
                    : $"attempt {attempt} of {RetryPolicy.MaxAttempts} failed; message moved to error queue",
                attempt: attempt, durationMs: stopwatch.Elapsed.TotalMilliseconds, outcome: "Failed", error: ex.Message);
            Logger.LogWarning(ex, "{Type} {MessageId} failed on attempt {Attempt}", message.Type, message.MessageId, attempt);
            // Rethrow (`throw;` keeps the original stack trace).  The transaction was never committed, so it
            // rolls back on dispose: no partial state.  MassTransit now retries, or after the last attempt
            // moves the poison message to "{queue}_error" (S10, S12).
            throw;
        }
    }
}
