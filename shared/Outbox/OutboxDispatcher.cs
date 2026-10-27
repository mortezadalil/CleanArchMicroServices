// ==========================================================================================
// FILE: shared/Outbox/OutboxDispatcher.cs
//
// WHAT THIS FILE IS
//   The background service that reads pending rows from a service's Outbox table and delivers
//   them to RabbitMQ through MassTransit.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Each producing service registers one instance for its own DbContext:
//     builder.Services.AddHostedService<OutboxDispatcher<OrdersDbContext>>();   (Orders/Program.cs)
//   It is the second half of the Transactional Outbox: OutboxMessage.cs explains the first half
//   (writing the row in the business transaction).
//
// WHICH PATTERN IT IMPLEMENTS
//   Transactional Outbox — "message relay" / polling publisher, with retry + exponential backoff.
//   Delivery guarantee: AT-LEAST-ONCE.  A message is never lost (the row stays until it is marked),
//   but it can be sent twice — the "outbox gap": the process may crash AFTER RabbitMQ accepted the
//   message and BEFORE the row is marked as published.  After a restart the row is still pending and
//   is sent again.  No local trick can close that gap (the broker and the database cannot share a
//   transaction), so the consumers' Inbox removes the duplicate instead.  Scenario S11 demonstrates it.
//
// Read next: shared/Messaging/IdempotentConsumer.cs (the receiving side that de-duplicates),
//            then shared/Faults/FaultRegistry.cs (the fault points used below).
// ==========================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;

namespace MiniShop.Shared.Outbox;

/// <summary>
/// Background service that moves rows from the Outbox table to RabbitMQ.
///
/// Loop: wait for a signal or 1 second → load pending rows whose backoff expired → for each row:
///   1. deserialize the payload to its contract type,
///   2. publish (event) or send (command) through MassTransit with the ORIGINAL MessageId,
///   3. mark the row as published.
/// If step 2 fails (broker down), the row keeps Attempts/LastError/NextAttemptUtc and is retried
/// with exponential backoff.  If the process dies between 2 and 3, the row is sent again later:
/// at-least-once delivery, de-duplicated by the consumers' Inbox.
///
/// Fault points (for the Scenario Lab):
///   - Outbox.Dispatch     : Pause (hold all rows), Delay, Duplicate (publish twice), Fail (before publish)
///   - Outbox.AfterPublish : Fail (crash AFTER publish and BEFORE mark-as-published → redelivery, S11)
/// </summary>
/// <remarks>
/// <c>BackgroundService</c> is the .NET base class for long-running hosted services: the host calls
/// <c>ExecuteAsync</c> once at startup and cancels <c>stoppingToken</c> at shutdown.
///
/// It is a SINGLETON (hosted services live as long as the app), while a DbContext is SCOPED (short-lived,
/// not thread-safe).  That is why we receive an <see cref="IServiceScopeFactory"/> and create a fresh
/// scope + DbContext for every batch, instead of injecting the DbContext directly.
/// </remarks>
/// <typeparam name="TDbContext">The service's DbContext; must be an EF Core context that owns an Outbox table.</typeparam>
public sealed class OutboxDispatcher<TDbContext>(
    IServiceScopeFactory scopes,
    IBus bus,
    OutboxSignal signal,
    FaultRegistry faults,
    DiagnosticsPublisher diagnostics,
    ILogger<OutboxDispatcher<TDbContext>> logger) : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    /// <summary>Max rows per round; keeps one round short and memory bounded when thousands of rows piled up (S04).</summary>
    private const int BatchSize = 50;
    /// <summary>Fallback poll: even without a signal, look for work every second (rows from before a restart, expired backoffs).</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    /// <summary>
    /// Upper bound for one publish.  When RabbitMQ is down MassTransit may wait a long time for a connection;
    /// without a timeout the whole dispatcher would hang on one row instead of recording the failure and backing off.
    /// </summary>
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The main loop: dispatch a batch; if there was nothing to do, sleep until signalled or the poll interval passes.</summary>
    /// <param name="stoppingToken">Cancelled when the host shuts down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox dispatcher started");
        // Run until the application stops.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dispatchedSomething = await DispatchBatchAsync(stoppingToken);
                // If we did work there may be more (more than BatchSize rows): loop again at once.
                // Otherwise sleep until a writer calls OutboxSignal.Notify() or the poll interval passes.
                if (!dispatchedSomething)
                    await signal.WaitAsync(PollInterval, stoppingToken);
            }
            // Normal shutdown: leave the loop quietly.  The `when` filter makes sure we only swallow a
            // cancellation that WE asked for, not some unrelated timeout.
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            // Anything else (database locked, bug...) must NOT kill the background service — an unhandled
            // exception would stop the dispatcher forever and every future message would be stuck.
            // Log, wait a moment so we do not spin at 100% CPU, and try again.
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox dispatcher loop failed; will retry");
                await Task.Delay(PollInterval, stoppingToken);
            }
        }
    }

    /// <summary>Loads one batch of due rows and dispatches them in order.</summary>
    /// <param name="ct">Shutdown token.</param>
    /// <returns>true when at least one row was processed (so the loop runs again immediately).</returns>
    private async Task<bool> DispatchBatchAsync(CancellationToken ct)
    {
        // New DI scope = new DbContext for this batch (see class remarks).  `using` disposes both at the end.
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        // Pending = not yet published AND (never failed OR its backoff time has passed).
        // Ordered by Id so messages leave in the order they were written (e.g. OrderPlaced before later events).
        var now = DateTime.UtcNow;
        var pending = await db.Outbox
            .Where(m => m.PublishedAtUtc == null && (m.NextAttemptUtc == null || m.NextAttemptUtc <= now))
            .OrderBy(m => m.Id)
            .Take(BatchSize)
            .ToListAsync(ct);

        // Nothing to do: tell the loop to sleep.
        if (pending.Count == 0)
            return false;

        var processedAny = false;
        foreach (var row in pending)
        {
            // Pause is checked per row so that a scenario-scoped pause only holds its own messages.
            // (A global Pause, without ScenarioId, holds every row: "the dispatcher is stopped".)
            if (faults.IsPaused(FaultPoints.OutboxDispatch, row.ScenarioId))
                continue;

            processedAny = true;
            // Rows are dispatched one by one, in order.  A failure on one row is handled inside
            // DispatchRowAsync (backoff), so it does not stop the others.
            await DispatchRowAsync(db, row, ct);
        }

        // If every row was paused we report "nothing processed", so the loop sleeps instead of busy-looping.
        return processedAny;
    }

    /// <summary>Delivers one row: deserialize → (faults) → publish/send → mark published, or record the failure and back off.</summary>
    /// <param name="db">The batch's DbContext (tracks <paramref name="row"/>, so changing the row and saving updates it).</param>
    /// <param name="row">The outbox row to deliver.</param>
    /// <param name="ct">Shutdown token.</param>
    private async Task DispatchRowAsync(TDbContext db, OutboxMessage row, CancellationToken ct)
    {
        // Count the attempt up front, so it is recorded whether the attempt succeeds or fails.
        row.Attempts++;
        try
        {
            // STEP 1 — turn the stored JSON back into the real contract type (OrderPlaced, ReserveStock...).
            // The cast to Message is safe: ContractTypes only knows Message subclasses.  `!` tells the compiler
            // "this is not null" (our own payloads are never the JSON literal null).
            var message = (Message)Json.Deserialize(row.Payload, ContractTypes.Resolve(row.Type))!;

            // Fault injection BEFORE publish (Fail / Delay / Duplicate).
            // Fail → throws → handled by the catch below exactly like a real broker error (backoff + retry).
            var fault = await faults.ApplyAsync(FaultPoints.OutboxDispatch, message, ct);

            // STEP 2 — hand the message to RabbitMQ.
            await PublishAsync(message, row, ct);
            if (fault.Duplicate)
                await PublishAsync(message, row, ct); // deliberately send it twice (S06)

            // Fault injection AFTER publish, BEFORE mark-as-published (S11: crash in the gap).
            // If this throws, the message is already in RabbitMQ but the row stays pending, so it will be
            // published AGAIN with the same MessageId — the consumers' Inbox must then ignore the copy.
            await faults.ApplyAsync(FaultPoints.OutboxAfterPublish, message, ct);

            // STEP 3 — mark as published and clear the failure bookkeeping, then save.  From now on the row is
            // never selected again.  (If the process died right before this SaveChanges, see the class summary.)
            row.PublishedAtUtc = DateTime.UtcNow;
            row.LastError = null;
            row.NextAttemptUtc = null;
            await db.SaveChangesAsync(ct);

            // Telemetry: the row has completed its journey (the console shows it leave the outbox).
            diagnostics.Emit(DiagnosticKind.OutboxDispatched, message,
                detail: fault.Duplicate ? "published twice (injected duplicate)" : $"attempt {row.Attempts}",
                attempt: row.Attempts);
        }
        // Do not treat shutdown as a failed attempt; let the cancellation flow up to ExecuteAsync.
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Exponential backoff: 1s, 2s, 4s ... capped at 30s.
            // Waiting longer after each failure avoids hammering a broker that is down (S04), while the cap
            // keeps recovery quick once it is back.
            var delay = TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, row.Attempts - 1)));
            row.LastError = ex.Message;
            row.NextAttemptUtc = DateTime.UtcNow.Add(delay);
            logger.LogWarning(ex, "Outbox row {Id} ({Type}) failed on attempt {Attempt}; next try in {Delay}",
                row.Id, row.Type, row.Attempts, delay);

            // Save the bookkeeping even though publish failed (this does not touch the broker).
            // The row stays pending (PublishedAtUtc is still null): nothing is lost, it will be retried.
            await db.SaveChangesAsync(ct);

            // Telemetry: makes the outage visible in the console ("retry in 4s").  This record is itself queued
            // in memory by DiagnosticsPublisher and delivered once the broker is reachable again.
            diagnostics.Emit(DiagnosticKind.RetryScheduled, correlationId: row.CorrelationId, scenarioId: row.ScenarioId,
                subjectMessageId: row.MessageId, subjectMessageType: row.Type,
                detail: $"outbox attempt {row.Attempts} failed: {ex.Message}; retry in {delay.TotalSeconds:0}s",
                attempt: row.Attempts, error: ex.Message);
        }
    }

    /// <summary>Publishes an event or sends a command through MassTransit, with a timeout.</summary>
    /// <param name="message">The deserialized message.</param>
    /// <param name="row">Its outbox row (tells us event vs command and the attempt number).</param>
    /// <param name="ct">Shutdown token.</param>
    private async Task PublishAsync(Message message, OutboxMessage row, CancellationToken ct)
    {
        // A linked token source is cancelled when EITHER the shutdown token fires OR our own timer expires.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(PublishTimeout);
        // We hold the message as the base type Message; passing its runtime type tells MassTransit to route it
        // as e.g. OrderPlaced (exchange "MiniShop.Contracts:OrderPlaced"), not as an abstract Message.
        var messageType = message.GetType();

        if (row.DestinationQueue is null)
        {
            // EVENT: publish to the type's exchange; RabbitMQ fans it out to every subscribed queue.
            await bus.Publish(message, messageType, ctx => Stamp(ctx, message), timeout.Token);
        }
        else
        {
            // COMMAND: look up the endpoint of the one destination queue and send it there directly.
            var endpoint = await bus.GetSendEndpoint(Topology.QueueAddress(row.DestinationQueue));
            await endpoint.Send(message, messageType, ctx => Stamp(ctx, message), timeout.Token);
        }

        // Telemetry for each physical publish (so an injected duplicate shows up as two MessagePublished records).
        diagnostics.Emit(DiagnosticKind.MessagePublished, message,
            detail: row.DestinationQueue is null ? "event published" : $"command sent to {row.DestinationQueue}",
            attempt: row.Attempts);
    }

    /// <summary>Copy envelope ids onto the transport headers so RabbitMQ / MassTransit see the same ids as the body.</summary>
    /// <remarks>
    /// Without this, MassTransit would generate a NEW transport MessageId on every publish, and a re-published row
    /// would look like a different message in the broker and in MassTransit's own logs.
    /// </remarks>
    /// <param name="ctx">MassTransit's send context (headers of the outgoing message).</param>
    /// <param name="message">The message whose envelope ids we copy.</param>
    private static void Stamp(SendContext ctx, Message message)
    {
        // Same id on the wire as in the body and the outbox row (stable across re-publishes).
        ctx.MessageId = message.MessageId;
        // The order id: MassTransit and the RabbitMQ UI can correlate by it.
        ctx.CorrelationId = message.CorrelationId;
        // C# NOTE — property pattern `is { } x`: "if the nullable value is not null, put it in x".
        // MassTransit's InitiatorId is its name for "the message that caused this one".
        if (message.CausationId is { } causation)
            ctx.InitiatorId = causation;
        // Custom headers make the scenario and producer visible when inspecting a message in the RabbitMQ UI.
        if (message.ScenarioId is { } scenario)
            ctx.Headers.Set("x-scenario-id", scenario.ToString());
        ctx.Headers.Set("x-producer", message.Producer);
    }
}
