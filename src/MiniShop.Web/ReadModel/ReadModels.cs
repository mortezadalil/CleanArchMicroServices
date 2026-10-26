namespace MiniShop.Web.ReadModel;

// ==================================================================================================
// READ MODEL (CQRS query side).  Every table here is built ONLY from events the Web consumed.
// The Web never opens another service's database.  All tables are rebuildable from EventAudit.
// ==================================================================================================

/// <summary>Projection of ProductCatalogChanged: what the storefront shows.</summary>
public sealed class ProductCard
{
    public Guid ProductId { get; set; }
    public string Sku { get; set; } = "";
    public string Name { get; set; } = "";
    public long Price { get; set; }
    public int OnHand { get; set; }
    public int Reserved { get; set; }
    public int Available => OnHand - Reserved;
    public int Version { get; set; }
    public DateTime UpdatedAtUtc { get; set; }   // OccurredAtUtc of the last applied event (ordering guard)
}

/// <summary>Projection of WalletBalanceChanged.</summary>
public sealed class WalletBalance
{
    public string CustomerId { get; set; } = "";
    public long Balance { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>Projection of the order events (list + details share one row; lines are stored as JSON).</summary>
public sealed class OrderSummary
{
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = "";
    public long Amount { get; set; }
    /// <summary>Pending / AwaitingPayment / Compensating / Confirmed / Rejected / RequiresAttention — derived from events.</summary>
    public string State { get; set; } = "Pending";
    public string? FailureReason { get; set; }
    public string LinesJson { get; set; } = "[]";
    public Guid? ScenarioId { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    /// <summary>Which event produced the current state (shown in the UI as "updated by ...").</summary>
    public string LastEvent { get; set; } = "";
}

/// <summary>
/// Append-only audit of EVERYTHING the Web received: business events and diagnostic records.
/// It is the source for the timeline, the message explorer, the replay and the read-model rebuild.
/// </summary>
public sealed class EventAudit
{
    public long Seq { get; set; }
    /// <summary>"Business" for integration events, "Diagnostic" for telemetry.</summary>
    public string Category { get; set; } = "";
    /// <summary>Business: the message type (OrderPlaced). Diagnostic: the kind (MessageConsumed).</summary>
    public string Kind { get; set; } = "";
    public string Service { get; set; } = "";
    public Guid MessageId { get; set; }
    public Guid? SubjectMessageId { get; set; }
    public string? SubjectMessageType { get; set; }
    public Guid CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
    public Guid? ScenarioId { get; set; }
    public DateTime OccurredAtUtc { get; set; }
    public DateTime RecordedAtUtc { get; set; }
    public int Attempt { get; set; }
    public double? DurationMs { get; set; }
    public string? Outcome { get; set; }
    public string? Error { get; set; }
    public string? Detail { get; set; }
    public string? PayloadJson { get; set; }
    public string? SnapshotJson { get; set; }
}

/// <summary>Latest known saga state per order, projected from SagaTransitioned diagnostics.</summary>
public sealed class SagaSnapshot
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = "";
    public string? LastTransition { get; set; }
    public string? SnapshotJson { get; set; }
    public int Transitions { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>How far behind the read side is (eventual consistency made visible).</summary>
public sealed class ProjectionCheckpoint
{
    public string Projection { get; set; } = "";
    public long LastSeq { get; set; }
    public DateTime? LastEventOccurredAtUtc { get; set; }
    public DateTime? LastAppliedAtUtc { get; set; }
    public long AppliedCount { get; set; }
    /// <summary>Applied - Occurred of the last event, in milliseconds.</summary>
    public double LastLagMs { get; set; }
}

/// <summary>One execution of a Scenario Lab scenario.</summary>
public sealed class ScenarioRun
{
    public Guid Id { get; set; }                       // = ScenarioId stamped on every message of the run
    public string Code { get; set; } = "";             // S01..S16
    public string Title { get; set; } = "";
    public string Status { get; set; } = "Running";    // Running / PASSED / FAILED / INCONCLUSIVE
    public string? CustomerId { get; set; }
    public Guid? OrderId { get; set; }
    public string OrderIdsJson { get; set; } = "[]";
    public string ExpectedTraceJson { get; set; } = "[]";
    public string ActualTraceJson { get; set; } = "[]";
    public string? Summary { get; set; }
    public string Log { get; set; } = "";
    public int DeliveryAttempts { get; set; }
    public int DedupHits { get; set; }
    public string? FinalSagaState { get; set; }
    public DateTime StartedAtUtc { get; set; }
    public DateTime? FinishedAtUtc { get; set; }
}

/// <summary>One invariant check of a run, with the evidence that was read from the owner service.</summary>
public sealed class ScenarioAssertion
{
    public long Id { get; set; }
    public Guid RunId { get; set; }
    public string Name { get; set; } = "";
    public bool Passed { get; set; }
    public string Evidence { get; set; } = "";
    public DateTime AtUtc { get; set; }
}
