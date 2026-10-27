// ==========================================================================================
// FILE: contracts/Diagnostics.cs
//
// WHAT THIS FILE IS
//   The contract of the TELEMETRY channel: the DiagnosticEvent message and the list of its kinds.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Part of contracts/ (linked as source into every service), but in its own namespace
//   (MiniShop.Contracts.Diagnostics) to make clear it is NOT a business message.
//   Every service produces DiagnosticEvents through shared/Diagnostics/DiagnosticsPublisher.cs;
//   only the Web consumes them (queue "minishop.web.diagnostics", DiagnosticEventConsumer in
//   src/MiniShop.Web/Projections/EventConsumers.cs) and stores them in an append-only audit table.
//
// WHICH PATTERN IT IMPLEMENTS
//   Observability / audit log: the Developer Console is built ONLY from these records, so what
//   students see on screen is what really happened inside the services.
//
// Read next: shared/Diagnostics/DiagnosticsPublisher.cs.
// ==========================================================================================

namespace MiniShop.Contracts.Diagnostics;

/// <summary>
/// Kinds of diagnostic (telemetry) events. They travel on a SEPARATE channel from the business
/// messages and are never used to make business decisions: they only feed the Developer Console.
/// </summary>
/// <remarks>Plain string constants (not an enum) so they read naturally in JSON, SQL queries and the UI.</remarks>
public static class DiagnosticKind
{
    /// <summary>A message left the service (outbox dispatcher published/sent it to RabbitMQ).</summary>
    public const string MessagePublished = "MessagePublished";
    /// <summary>A consumer finished processing a message successfully (its transaction committed).</summary>
    public const string MessageConsumed = "MessageConsumed";
    /// <summary>A consumer attempt threw; MassTransit will retry it (S07, S16).</summary>
    public const string ConsumerFailed = "ConsumerFailed";
    /// <summary>A message was written to the Outbox table in the business transaction.</summary>
    public const string OutboxStored = "OutboxStored";
    /// <summary>An Outbox row was published AND marked as published.</summary>
    public const string OutboxDispatched = "OutboxDispatched";
    /// <summary>The Inbox recognised a MessageId it had already processed and ignored the delivery (S06, S11).</summary>
    public const string InboxDuplicateIgnored = "InboxDuplicateIgnored";
    /// <summary>The Orders saga moved from one state to another.</summary>
    public const string SagaTransitioned = "SagaTransitioned";
    /// <summary>The Web read model applied an event (CQRS query side).</summary>
    public const string ProjectionUpdated = "ProjectionUpdated";
    /// <summary>An Outbox publish failed and the row was rescheduled with backoff (S04 broker outage).</summary>
    public const string RetryScheduled = "RetryScheduled";
    /// <summary>All retries failed: the message was moved to the "{queue}_error" queue (poison message, S10).</summary>
    public const string MessageDeadLettered = "MessageDeadLettered";
    /// <summary>A Scenario Lab fault rule fired (so the console shows that a failure was deliberate).</summary>
    public const string FaultInjected = "FaultInjected";
    /// <summary>A copy of a service's state (stock, wallet, order) for the Before/During/After views.</summary>
    public const string StateSnapshot = "StateSnapshot";
}

/// <summary>
/// One telemetry record. Everything the Developer Console shows (timeline, graph animation,
/// saga inspector, outbox/inbox counters, DB snapshots) is built from these records, which the
/// Web stores in an append-only audit table.  Nothing in the UI is faked.
/// </summary>
/// <remarks>
/// It derives from <see cref="Message"/> so it gets the same envelope (CorrelationId = OrderId,
/// ScenarioId...), which lets the console filter telemetry per order and per experiment.
///
/// C# NOTE — <c>required</c> (C# 11): the compiler forces whoever creates the object to set these
/// properties in the object initializer; forgetting <c>Kind</c> or <c>Service</c> is a compile error
/// instead of a record with an empty kind at runtime.
/// </remarks>
public sealed record DiagnosticEvent : Message
{
    /// <summary>One of <see cref="DiagnosticKind"/>.</summary>
    public required string Kind { get; init; }

    /// <summary>Service that emitted the record (same as Producer, repeated for clarity in queries).</summary>
    public required string Service { get; init; }

    /// <summary>MessageId / type of the business message this record is about (if any).</summary>
    public Guid? SubjectMessageId { get; init; }
    /// <summary>Short type name of that business message ("OrderPlaced"), or null.</summary>
    public string? SubjectMessageType { get; init; }

    /// <summary>For consumer records: which attempt (1 = first delivery), how long, and the result.</summary>
    public int Attempt { get; init; }
    /// <summary>How long the attempt took, in milliseconds (null when not measured).</summary>
    public double? DurationMs { get; init; }
    /// <summary>"Success" / "Failed" for consumer records, null otherwise.</summary>
    public string? Outcome { get; init; }
    /// <summary>Exception message when something failed (the stack trace stays in the service log).</summary>
    public string? Error { get; init; }

    /// <summary>Free-form, human-readable detail ("Reserving -> AwaitingPayment", "attempt 2 of 3"...).</summary>
    public string? Detail { get; init; }

    /// <summary>JSON of the business payload (demo data only, nothing sensitive).</summary>
    public string? PayloadJson { get; init; }

    /// <summary>JSON snapshot of the owning service's state after the step (stock levels, wallet, order, saga).</summary>
    public string? SnapshotJson { get; init; }
}
