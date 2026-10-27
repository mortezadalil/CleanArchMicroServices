// ==========================================================================================
// FILE: shared/Diagnostics/DiagnosticsPublisher.cs
//
// WHAT THIS FILE IS
//   The producer side of the telemetry channel: an in-memory queue of DiagnosticEvent records
//   (DiagnosticsPublisher) and the background worker that ships them to RabbitMQ
//   (DiagnosticsPublisherWorker).
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Both classes are registered in ServiceHostingExtensions.cs.
//   Emit(...) is called from IdempotentConsumer, OutboxDispatcher, FaultRegistry, the Orders saga, etc.
//   The records are published like any event, but only the Web's "minishop.web.diagnostics" queue
//   subscribes; DiagnosticEventConsumer (src/MiniShop.Web/Projections/EventConsumers.cs) stores them in
//   the audit table that feeds the Developer Console.
//
// WHICH PATTERN IT IMPLEMENTS
//   Fire-and-forget, best-effort telemetry with a bounded buffer.  Notice the DIFFERENCE with business
//   messages: those go through the Transactional Outbox because losing one would corrupt an order;
//   telemetry may be dropped under extreme load, and in exchange business code never waits for it and
//   never fails because of it.
//
// Read next: contracts/Diagnostics.cs (the record) and src/MiniShop.Web/Projections/EventConsumers.cs.
// ==========================================================================================

using System.Threading.Channels;
using MassTransit;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Messaging;

namespace MiniShop.Shared.Diagnostics;

/// <summary>
/// Collects telemetry records and publishes them to RabbitMQ in the background, on a channel that is
/// separate from business messaging.  Telemetry is best-effort: if the broker is down the records wait
/// in memory (bounded) and are flushed when it comes back, so the Developer Console still shows what
/// happened during an outage.  Business code never waits for telemetry.
/// </summary>
/// <remarks>
/// C# NOTE — <c>Channel&lt;T&gt;</c> (System.Threading.Channels): a thread-safe, async producer/consumer
/// queue.  Many threads can <c>TryWrite</c> into it at the same time; one reader awaits items with
/// <c>ReadAllAsync</c> without blocking a thread.  Think of it as an in-process mini message queue.
/// </remarks>
/// <param name="serviceName">This service's name; stamped as Service and Producer on every record.</param>
/// <param name="logger">Used to warn when a record had to be dropped.</param>
public sealed class DiagnosticsPublisher(string serviceName, ILogger<DiagnosticsPublisher> logger)
{
    /// <summary>
    /// The buffer between Emit (many writers) and the worker (one reader).  BOUNDED to 10 000 records so a
    /// long broker outage cannot eat all memory; when full, the OLDEST record is dropped to make room — the
    /// most recent events are the most useful ones when the broker comes back.
    /// </summary>
    private readonly Channel<DiagnosticEvent> _queue = Channel.CreateBounded<DiagnosticEvent>(
        new BoundedChannelOptions(10_000) { FullMode = BoundedChannelFullMode.DropOldest });

    /// <summary>This service's name.</summary>
    public string ServiceName => serviceName;
    /// <summary>The read end of the buffer, used only by <see cref="DiagnosticsPublisherWorker"/>.</summary>
    public ChannelReader<DiagnosticEvent> Reader => _queue.Reader;

    /// <summary>Record something about a business message (most common overload).</summary>
    /// <remarks>
    /// The record copies the subject's CorrelationId/ScenarioId (so the console can filter it per order and per
    /// experiment) and uses the subject's MessageId as its CausationId (this record was caused by that message).
    /// Optional parameters with defaults (<c>string? detail = null</c>) let callers name only what they need:
    /// <c>Emit(kind, message, detail: "...", attempt: 2)</c>.
    /// </remarks>
    /// <param name="kind">One of <see cref="DiagnosticKind"/>.</param>
    /// <param name="subject">The business message the record is about; its JSON is stored as PayloadJson.</param>
    /// <param name="detail">Human-readable text for the timeline.</param>
    /// <param name="attempt">Delivery/dispatch attempt number (1 = first).</param>
    /// <param name="durationMs">How long the step took.</param>
    /// <param name="outcome">"Success" / "Failed".</param>
    /// <param name="error">Exception message, if any.</param>
    /// <param name="snapshot">Optional state object, stored as JSON for the Before/After views.</param>
    public void Emit(string kind, Message subject, string? detail = null, int attempt = 0,
        double? durationMs = null, string? outcome = null, string? error = null, object? snapshot = null)
    {
        Emit(new DiagnosticEvent
        {
            Kind = kind,
            Service = serviceName,
            Producer = serviceName,
            CorrelationId = subject.CorrelationId,
            ScenarioId = subject.ScenarioId,
            CausationId = subject.MessageId,
            SubjectMessageId = subject.MessageId,
            SubjectMessageType = subject.Type,
            Attempt = attempt,
            DurationMs = durationMs,
            Outcome = outcome,
            Error = error,
            Detail = detail,
            // Serialize with the RUNTIME type so the payload includes every field of e.g. OrderPlaced.
            PayloadJson = Json.Serialize(subject, subject.GetType()),
            SnapshotJson = snapshot is null ? null : Json.Serialize(snapshot),
        });
    }

    /// <summary>Record something that is not tied to one message object (saga transitions, snapshots, retries).</summary>
    /// <remarks>Used where we only have ids at hand — e.g. an outbox row that could not even be deserialized, or a saga step.</remarks>
    /// <param name="kind">One of <see cref="DiagnosticKind"/>.</param>
    /// <param name="correlationId">The order the record is about.</param>
    /// <param name="scenarioId">Scenario, if any.</param>
    /// <param name="subjectMessageId">Id of the related message, if any (also used as CausationId).</param>
    /// <param name="subjectMessageType">Type name of the related message.</param>
    /// <param name="detail">Human-readable text.</param>
    /// <param name="attempt">Attempt number.</param>
    /// <param name="outcome">"Success" / "Failed".</param>
    /// <param name="error">Exception message, if any.</param>
    /// <param name="snapshot">Optional state object, stored as JSON.</param>
    public void Emit(string kind, Guid correlationId, Guid? scenarioId = null, Guid? subjectMessageId = null,
        string? subjectMessageType = null, string? detail = null, int attempt = 0, string? outcome = null,
        string? error = null, object? snapshot = null)
    {
        Emit(new DiagnosticEvent
        {
            Kind = kind,
            Service = serviceName,
            Producer = serviceName,
            CorrelationId = correlationId,
            ScenarioId = scenarioId,
            CausationId = subjectMessageId,
            SubjectMessageId = subjectMessageId,
            SubjectMessageType = subjectMessageType,
            Attempt = attempt,
            Outcome = outcome,
            Error = error,
            Detail = detail,
            SnapshotJson = snapshot is null ? null : Json.Serialize(snapshot),
        });
    }

    /// <summary>Queues a ready-made record.  Never blocks and never throws: the caller is business code.</summary>
    /// <param name="record">The record to ship.</param>
    public void Emit(DiagnosticEvent record)
    {
        // TryWrite returns immediately.  With DropOldest it practically always succeeds (it drops an old record
        // instead); the false branch only happens if the channel was completed.  We log rather than throw,
        // because a telemetry problem must never fail an order.
        if (!_queue.Writer.TryWrite(record))
            logger.LogWarning("Diagnostic record dropped (queue full): {Kind}", record.Kind);
    }
}

/// <summary>Drains <see cref="DiagnosticsPublisher"/> to the bus; retries forever on broker errors.</summary>
/// <remarks>
/// A hosted <c>BackgroundService</c> started with the application.  Records are published one by one and in
/// order: a record is only taken off the buffer's head after the previous one was accepted by the broker, so
/// during an outage (S04) everything waits in the buffer and is flushed in the original order afterwards.
/// </remarks>
/// <param name="publisher">The singleton buffer to drain.</param>
/// <param name="bus">MassTransit bus used to publish the records (as DiagnosticEvent events).</param>
/// <param name="logger">Logger (Debug level only: an outage is expected in demos and must not flood logs).</param>
public sealed class DiagnosticsPublisherWorker(DiagnosticsPublisher publisher, IBus bus, ILogger<DiagnosticsPublisherWorker> logger)
    : BackgroundService
{
    /// <summary>Reads records as they arrive and publishes each one, retrying until it succeeds or the app stops.</summary>
    /// <param name="stoppingToken">Cancelled at shutdown.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // `await foreach` over ReadAllAsync: waits (without blocking a thread) for the next record, forever,
        // until the token is cancelled.
        await foreach (var record in publisher.Reader.ReadAllAsync(stoppingToken))
        {
            // Retry loop for THIS record.
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // 5-second cap per attempt, so a dead broker does not hang the worker indefinitely.
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    timeout.CancelAfter(TimeSpan.FromSeconds(5));
                    // Publish the record; set the transport MessageId to the record's own id (consistent with the
                    // outbox's Stamp).  The record is NOT written to an outbox: telemetry is best-effort.
                    await bus.Publish(record, ctx => ctx.MessageId = record.MessageId, timeout.Token);
                    // Success: leave the retry loop and take the next record.
                    break;
                }
                // App is shutting down: stop quietly (remaining in-memory records are lost, which is acceptable).
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                // Broker unreachable or the 5-second timeout fired: wait 2 s and retry the same record.
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Diagnostics publish failed; broker probably down, retrying in 2s");
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
            }
        }
    }
}
