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
public sealed class OutboxDispatcher<TDbContext>(
    IServiceScopeFactory scopes,
    IBus bus,
    OutboxSignal signal,
    FaultRegistry faults,
    DiagnosticsPublisher diagnostics,
    ILogger<OutboxDispatcher<TDbContext>> logger) : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    private const int BatchSize = 50;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox dispatcher started");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var dispatchedSomething = await DispatchBatchAsync(stoppingToken);
                if (!dispatchedSomething)
                    await signal.WaitAsync(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Outbox dispatcher loop failed; will retry");
                await Task.Delay(PollInterval, stoppingToken);
            }
        }
    }

    /// <returns>true when at least one row was processed (so the loop runs again immediately).</returns>
    private async Task<bool> DispatchBatchAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var now = DateTime.UtcNow;
        var pending = await db.Outbox
            .Where(m => m.PublishedAtUtc == null && (m.NextAttemptUtc == null || m.NextAttemptUtc <= now))
            .OrderBy(m => m.Id)
            .Take(BatchSize)
            .ToListAsync(ct);

        if (pending.Count == 0)
            return false;

        var processedAny = false;
        foreach (var row in pending)
        {
            // Pause is checked per row so that a scenario-scoped pause only holds its own messages.
            if (faults.IsPaused(FaultPoints.OutboxDispatch, row.ScenarioId))
                continue;

            processedAny = true;
            await DispatchRowAsync(db, row, ct);
        }

        return processedAny;
    }

    private async Task DispatchRowAsync(TDbContext db, OutboxMessage row, CancellationToken ct)
    {
        row.Attempts++;
        try
        {
            var message = (Message)Json.Deserialize(row.Payload, ContractTypes.Resolve(row.Type))!;

            // Fault injection BEFORE publish (Fail / Delay / Duplicate).
            var fault = await faults.ApplyAsync(FaultPoints.OutboxDispatch, message, ct);

            await PublishAsync(message, row, ct);
            if (fault.Duplicate)
                await PublishAsync(message, row, ct); // deliberately send it twice (S06)

            // Fault injection AFTER publish, BEFORE mark-as-published (S11: crash in the gap).
            await faults.ApplyAsync(FaultPoints.OutboxAfterPublish, message, ct);

            row.PublishedAtUtc = DateTime.UtcNow;
            row.LastError = null;
            row.NextAttemptUtc = null;
            await db.SaveChangesAsync(ct);

            diagnostics.Emit(DiagnosticKind.OutboxDispatched, message,
                detail: fault.Duplicate ? "published twice (injected duplicate)" : $"attempt {row.Attempts}",
                attempt: row.Attempts);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Exponential backoff: 1s, 2s, 4s ... capped at 30s.
            var delay = TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, row.Attempts - 1)));
            row.LastError = ex.Message;
            row.NextAttemptUtc = DateTime.UtcNow.Add(delay);
            logger.LogWarning(ex, "Outbox row {Id} ({Type}) failed on attempt {Attempt}; next try in {Delay}",
                row.Id, row.Type, row.Attempts, delay);

            // Save the bookkeeping even though publish failed (this does not touch the broker).
            await db.SaveChangesAsync(ct);

            diagnostics.Emit(DiagnosticKind.RetryScheduled, correlationId: row.CorrelationId, scenarioId: row.ScenarioId,
                subjectMessageId: row.MessageId, subjectMessageType: row.Type,
                detail: $"outbox attempt {row.Attempts} failed: {ex.Message}; retry in {delay.TotalSeconds:0}s",
                attempt: row.Attempts, error: ex.Message);
        }
    }

    private async Task PublishAsync(Message message, OutboxMessage row, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(PublishTimeout);
        var messageType = message.GetType();

        if (row.DestinationQueue is null)
        {
            await bus.Publish(message, messageType, ctx => Stamp(ctx, message), timeout.Token);
        }
        else
        {
            var endpoint = await bus.GetSendEndpoint(Topology.QueueAddress(row.DestinationQueue));
            await endpoint.Send(message, messageType, ctx => Stamp(ctx, message), timeout.Token);
        }

        diagnostics.Emit(DiagnosticKind.MessagePublished, message,
            detail: row.DestinationQueue is null ? "event published" : $"command sent to {row.DestinationQueue}",
            attempt: row.Attempts);
    }

    /// <summary>Copy envelope ids onto the transport headers so RabbitMQ / MassTransit see the same ids as the body.</summary>
    private static void Stamp(SendContext ctx, Message message)
    {
        ctx.MessageId = message.MessageId;
        ctx.CorrelationId = message.CorrelationId;
        if (message.CausationId is { } causation)
            ctx.InitiatorId = causation;
        if (message.ScenarioId is { } scenario)
            ctx.Headers.Set("x-scenario-id", scenario.ToString());
        ctx.Headers.Set("x-producer", message.Producer);
    }
}
