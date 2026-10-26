namespace MiniShop.Contracts.Diagnostics;

/// <summary>
/// Kinds of diagnostic (telemetry) events. They travel on a SEPARATE channel from the business
/// messages and are never used to make business decisions: they only feed the Developer Console.
/// </summary>
public static class DiagnosticKind
{
    public const string MessagePublished = "MessagePublished";
    public const string MessageConsumed = "MessageConsumed";
    public const string ConsumerFailed = "ConsumerFailed";
    public const string OutboxStored = "OutboxStored";
    public const string OutboxDispatched = "OutboxDispatched";
    public const string InboxDuplicateIgnored = "InboxDuplicateIgnored";
    public const string SagaTransitioned = "SagaTransitioned";
    public const string ProjectionUpdated = "ProjectionUpdated";
    public const string RetryScheduled = "RetryScheduled";
    public const string MessageDeadLettered = "MessageDeadLettered";
    public const string FaultInjected = "FaultInjected";
    public const string StateSnapshot = "StateSnapshot";
}

/// <summary>
/// One telemetry record. Everything the Developer Console shows (timeline, graph animation,
/// saga inspector, outbox/inbox counters, DB snapshots) is built from these records, which the
/// Web stores in an append-only audit table.  Nothing in the UI is faked.
/// </summary>
public sealed record DiagnosticEvent : Message
{
    /// <summary>One of <see cref="DiagnosticKind"/>.</summary>
    public required string Kind { get; init; }

    /// <summary>Service that emitted the record (same as Producer, repeated for clarity in queries).</summary>
    public required string Service { get; init; }

    /// <summary>MessageId / type of the business message this record is about (if any).</summary>
    public Guid? SubjectMessageId { get; init; }
    public string? SubjectMessageType { get; init; }

    /// <summary>For consumer records: which attempt (1 = first delivery), how long, and the result.</summary>
    public int Attempt { get; init; }
    public double? DurationMs { get; init; }
    public string? Outcome { get; init; }
    public string? Error { get; init; }

    /// <summary>Free-form, human-readable detail ("Reserving -> AwaitingPayment", "attempt 2 of 3"...).</summary>
    public string? Detail { get; init; }

    /// <summary>JSON of the business payload (demo data only, nothing sensitive).</summary>
    public string? PayloadJson { get; init; }

    /// <summary>JSON snapshot of the owning service's state after the step (stock levels, wallet, order, saga).</summary>
    public string? SnapshotJson { get; init; }
}
