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
public sealed class DiagnosticsPublisher(string serviceName, ILogger<DiagnosticsPublisher> logger)
{
    private readonly Channel<DiagnosticEvent> _queue = Channel.CreateBounded<DiagnosticEvent>(
        new BoundedChannelOptions(10_000) { FullMode = BoundedChannelFullMode.DropOldest });

    public string ServiceName => serviceName;
    public ChannelReader<DiagnosticEvent> Reader => _queue.Reader;

    /// <summary>Record something about a business message (most common overload).</summary>
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
            PayloadJson = Json.Serialize(subject, subject.GetType()),
            SnapshotJson = snapshot is null ? null : Json.Serialize(snapshot),
        });
    }

    /// <summary>Record something that is not tied to one message object (saga transitions, snapshots, retries).</summary>
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

    public void Emit(DiagnosticEvent record)
    {
        if (!_queue.Writer.TryWrite(record))
            logger.LogWarning("Diagnostic record dropped (queue full): {Kind}", record.Kind);
    }
}

/// <summary>Drains <see cref="DiagnosticsPublisher"/> to the bus; retries forever on broker errors.</summary>
public sealed class DiagnosticsPublisherWorker(DiagnosticsPublisher publisher, IBus bus, ILogger<DiagnosticsPublisherWorker> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var record in publisher.Reader.ReadAllAsync(stoppingToken))
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    timeout.CancelAfter(TimeSpan.FromSeconds(5));
                    await bus.Publish(record, ctx => ctx.MessageId = record.MessageId, timeout.Token);
                    break;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Diagnostics publish failed; broker probably down, retrying in 2s");
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
            }
        }
    }
}
