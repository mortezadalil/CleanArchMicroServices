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
public sealed record HandleResult(string Detail, object? Snapshot = null)
{
    /// <summary>Optional callback run only after the transaction committed (e.g. emit a saga-transition record).</summary>
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
public abstract class IdempotentConsumer<TMessage, TDbContext>(
    TDbContext db,
    DiagnosticsPublisher diagnostics,
    OutboxSignal outboxSignal,
    FaultRegistry faults,
    ILogger logger) : IConsumer<TMessage>
    where TMessage : Message
    where TDbContext : DbContext, IInboxDbContext
{
    protected TDbContext Db { get; } = db;
    protected DiagnosticsPublisher Diagnostics { get; } = diagnostics;
    protected ILogger Logger { get; } = logger;

    /// <summary>Business logic. Add aggregate changes and outbox rows to <see cref="Db"/>; do NOT call SaveChanges.</summary>
    protected abstract Task<HandleResult> HandleAsync(ConsumeContext<TMessage> context);

    /// <summary>
    /// Name of the fault-injection point of this consumer (one of <see cref="FaultPoints"/>), or null.
    /// The fault is applied BEFORE the transaction starts, so an injected Delay/Pause never holds a
    /// database lock; the handler reads the outcome (Reject / Duplicate) from <see cref="Fault"/>.
    /// </summary>
    protected virtual string? FaultPoint => null;

    /// <summary>Outcome of the fault applied for the current message (None when no rule matched).</summary>
    protected FaultOutcome Fault { get; private set; } = FaultOutcome.None;

    /// <summary>
    /// False for consumers of telemetry itself: recording "I consumed a diagnostic record" as a new
    /// diagnostic record would feed back into the same queue forever.
    /// </summary>
    protected virtual bool EmitTelemetry => true;

    public async Task Consume(ConsumeContext<TMessage> context)
    {
        var message = context.Message;
        var attempt = context.GetRetryAttempt() + 1;
        var ct = context.CancellationToken;
        var stopwatch = Stopwatch.StartNew();

        using var _ = FaultContext.Enter(message.ScenarioId, message.CorrelationId);
        try
        {
            Fault = FaultPoint is null ? FaultOutcome.None : await faults.ApplyAsync(FaultPoint, message, ct);

            // BEGIN IMMEDIATE on SQLite: from here on we hold the write lock, so keep this block short.
            await using var transaction = await Db.Database.BeginTransactionAsync(ct);

            if (await Db.Inbox.AnyAsync(i => i.MessageId == message.MessageId, ct))
            {
                if (EmitTelemetry)
                    Diagnostics.Emit(DiagnosticKind.InboxDuplicateIgnored, message,
                        detail: $"{message.Type} {message.MessageId} was already processed; delivery ignored", attempt: attempt);
                Logger.LogInformation("Duplicate {Type} {MessageId} ignored", message.Type, message.MessageId);
                return;
            }

            var result = await HandleAsync(context);

            Db.Inbox.Add(new InboxMessage
            {
                MessageId = message.MessageId,
                MessageType = message.Type,
                CorrelationId = message.CorrelationId,
                ReceivedAtUtc = DateTime.UtcNow,
            });

            var outboxRows = Db.ChangeTracker.Entries<OutboxMessage>()
                .Where(e => e.State == EntityState.Added)
                .Select(e => e.Entity)
                .ToList();

            await Db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);

            // --- after commit: side effects that must not influence the transaction ---
            if (outboxRows.Count > 0)
                outboxSignal.Notify();
            result.AfterCommit?.Invoke();
            if (!EmitTelemetry)
                return;

            foreach (var row in outboxRows)
                Diagnostics.Emit(DiagnosticKind.OutboxStored, correlationId: row.CorrelationId, scenarioId: row.ScenarioId,
                    subjectMessageId: row.MessageId, subjectMessageType: row.Type,
                    detail: row.DestinationQueue is null ? "event stored in outbox" : $"command stored in outbox → {row.DestinationQueue}");
            Diagnostics.Emit(DiagnosticKind.MessageConsumed, message, detail: result.Detail, attempt: attempt,
                durationMs: stopwatch.Elapsed.TotalMilliseconds, outcome: "Success", snapshot: result.Snapshot);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var willRetry = attempt < RetryPolicy.MaxAttempts;
            if (EmitTelemetry)
                Diagnostics.Emit(willRetry ? DiagnosticKind.ConsumerFailed : DiagnosticKind.MessageDeadLettered, message,
                detail: willRetry
                    ? $"attempt {attempt} of {RetryPolicy.MaxAttempts} failed; retry scheduled"
                    : $"attempt {attempt} of {RetryPolicy.MaxAttempts} failed; message moved to error queue",
                attempt: attempt, durationMs: stopwatch.Elapsed.TotalMilliseconds, outcome: "Failed", error: ex.Message);
            Logger.LogWarning(ex, "{Type} {MessageId} failed on attempt {Attempt}", message.Type, message.MessageId, attempt);
            throw;
        }
    }
}
