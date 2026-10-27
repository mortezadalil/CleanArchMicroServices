// ==================================================================================================
// FILE: src/MiniShop.Web/ReadModel/ReadModels.cs
//
// WHAT IT IS
//   The row classes (plain EF Core entities) stored in readmodel.db.  No behaviour, just data shaped
//   for the screens: the storefront, "my orders", the order timeline, the Developer Console and the
//   Scenario Lab.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   CQRS QUERY side.  On the write side, Orders/Inventory/Payments keep rich domain objects with rules.
//   Here we keep flat, denormalised copies that are cheap to read.  They are written ONLY by the
//   projection consumers (Projections/) and read ONLY through Queries/ReadModelQueries.cs.
//
// IMPORTANT IDEAS
//   - Eventual consistency: these rows lag a little behind the owners (see ProjectionCheckpoint).
//   - UpdatedAtUtc columns hold the OccurredAtUtc of the last APPLIED event.  They are the "ordering
//     guard": an event that occurred earlier than this value is older news and must not overwrite.
//   - Everything except EventAudit (and the Scenario Lab tables) is disposable: it can be rebuilt by
//     replaying EventAudit.
//
// Read next: Data/ReadModelDbContext.cs (keys and indexes), then Projections/Projector.cs.
// ==================================================================================================

namespace MiniShop.Web.ReadModel;

// ==================================================================================================
// READ MODEL (CQRS query side).  Every table here is built ONLY from events the Web consumed.
// The Web never opens another service's database.  All tables are rebuildable from EventAudit.
// ==================================================================================================

/// <summary>
/// Projection of ProductCatalogChanged: what the storefront shows.
/// Inventory is the owner of stock; this card is only a copy that may be slightly out of date.  The real
/// check ("is there enough stock?") happens in Inventory when the order is processed, never here.
/// </summary>
public sealed class ProductCard
{
    /// <summary>Primary key: the product id assigned by Inventory.</summary>
    public Guid ProductId { get; set; }
    /// <summary>Stock-keeping unit code, shown in the catalog.</summary>
    public string Sku { get; set; } = "";
    /// <summary>Display name.</summary>
    public string Name { get; set; } = "";
    /// <summary>Unit price in the smallest currency unit (a long, never a floating-point number, to avoid rounding errors).</summary>
    public long Price { get; set; }
    /// <summary>Physical units in the warehouse, as last reported by Inventory.</summary>
    public int OnHand { get; set; }
    /// <summary>Units held for orders that are still in progress (reserved but not yet shipped/released).</summary>
    public int Reserved { get; set; }
    /// <summary>What can still be sold.  Computed in C#, not stored (the DbContext ignores it).</summary>
    public int Available => OnHand - Reserved;
    /// <summary>Version number of the product in Inventory (shown for teaching: it grows with every change).</summary>
    public int Version { get; set; }
    /// <summary>OccurredAtUtc of the last applied event; an event older than this is ignored.</summary>
    public DateTime UpdatedAtUtc { get; set; }   // OccurredAtUtc of the last applied event (ordering guard)
}

/// <summary>
/// Projection of WalletBalanceChanged.
/// The event carries the FINAL balance, not a delta ("+500").  That is what makes the projection safe:
/// applying the same event twice still gives the same balance, and a late, older event is simply
/// rejected by the timestamp guard.  With deltas, a duplicate or a re-ordering would corrupt the number.
/// </summary>
public sealed class WalletBalance
{
    /// <summary>Primary key: the customer (e.g. "demo-customer").</summary>
    public string CustomerId { get; set; } = "";
    /// <summary>Balance as reported by Payments, the owner of wallets.</summary>
    public long Balance { get; set; }
    /// <summary>OccurredAtUtc of the last applied WalletBalanceChanged (ordering guard).</summary>
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>
/// Projection of the order events (list + details share one row; lines are stored as JSON).
/// Several events, from several services, update the same row: OrderPlaced creates it, StockReserved,
/// PaymentFailed, OrderConfirmed... move its State forward.
/// </summary>
public sealed class OrderSummary
{
    /// <summary>Primary key; also the saga CorrelationId used on every message of this order.</summary>
    public Guid OrderId { get; set; }
    /// <summary>Who placed the order.</summary>
    public string CustomerId { get; set; } = "";
    /// <summary>Order total in the smallest currency unit.</summary>
    public long Amount { get; set; }
    /// <summary>
    /// Pending / AwaitingPayment / Compensating / Confirmed / Rejected / RequiresAttention — derived from events.
    /// (The projector also uses the intermediate "Rejecting" after StockReservationRejected.)  This is
    /// the READ model's view: the authoritative state lives in the Orders saga.
    /// </summary>
    public string State { get; set; } = "Pending";
    /// <summary>Why the order failed (out of stock, insufficient funds...), if it did.</summary>
    public string? FailureReason { get; set; }
    /// <summary>The order lines serialised as JSON: a read model may denormalise freely, one row is enough for the UI.</summary>
    public string LinesJson { get; set; } = "[]";
    /// <summary>Scenario Lab run that created this order, or null for a normal storefront order.</summary>
    public Guid? ScenarioId { get; set; }
    /// <summary>When the order was placed (OccurredAtUtc of OrderPlaced).</summary>
    public DateTime CreatedAtUtc { get; set; }
    /// <summary>OccurredAtUtc of the last applied event (ordering guard: older events are ignored).</summary>
    public DateTime UpdatedAtUtc { get; set; }
    /// <summary>Which event produced the current state (shown in the UI as "updated by ...").</summary>
    public string LastEvent { get; set; } = "";
}

/// <summary>
/// Append-only audit of EVERYTHING the Web received: business events and diagnostic records.
/// It is the source for the timeline, the message explorer, the replay and the read-model rebuild.
///
/// Why do we need our own log?  RabbitMQ is a message BROKER, not an event STORE: once a message is
/// acknowledged it is gone from the queue, and events published before our queue existed were never
/// delivered at all.  So the Web keeps the full payload of every business event it applied here.
/// Rows are only ever INSERTED (never updated or deleted, except by "Reset demo data").
/// </summary>
public sealed class EventAudit
{
    /// <summary>Auto-increment sequence number = the order in which the Web recorded the rows (replay order).</summary>
    public long Seq { get; set; }
    /// <summary>"Business" for integration events, "Diagnostic" for telemetry.</summary>
    public string Category { get; set; } = "";
    /// <summary>Business: the message type (OrderPlaced). Diagnostic: the kind (MessageConsumed).</summary>
    public string Kind { get; set; } = "";
    /// <summary>The service that produced the record (Orders, Inventory, Payments, Web).</summary>
    public string Service { get; set; } = "";
    /// <summary>Unique id of the received message (the Inbox de-duplicates on this).</summary>
    public Guid MessageId { get; set; }
    /// <summary>Diagnostic rows only: the id of the business message the record talks about.</summary>
    public Guid? SubjectMessageId { get; set; }
    /// <summary>Diagnostic rows only: the type of the business message the record talks about.</summary>
    public string? SubjectMessageType { get; set; }
    /// <summary>Groups every message of one business flow; for orders it equals the OrderId.</summary>
    public Guid CorrelationId { get; set; }
    /// <summary>The message that caused this one (lets the UI draw "A led to B").</summary>
    public Guid? CausationId { get; set; }
    /// <summary>Scenario Lab run id, when the message belongs to a scenario.</summary>
    public Guid? ScenarioId { get; set; }
    /// <summary>When it happened at the producer (producer's clock).</summary>
    public DateTime OccurredAtUtc { get; set; }
    /// <summary>When the Web stored it.  RecordedAtUtc - OccurredAtUtc is the delivery/projection lag.</summary>
    public DateTime RecordedAtUtc { get; set; }
    /// <summary>Delivery attempt number (1 = first try; higher means retries happened).</summary>
    public int Attempt { get; set; }
    /// <summary>Diagnostics: how long the reported operation took, in milliseconds.</summary>
    public double? DurationMs { get; set; }
    /// <summary>Diagnostics: "Success", "Failed", "Transitioned"...</summary>
    public string? Outcome { get; set; }
    /// <summary>Diagnostics: error message when something failed.</summary>
    public string? Error { get; set; }
    /// <summary>Human-readable explanation shown in the timeline.</summary>
    public string? Detail { get; set; }
    /// <summary>The full message as JSON.  For business events this is what a rebuild deserialises and replays.</summary>
    public string? PayloadJson { get; set; }
    /// <summary>Diagnostics: optional state snapshot (e.g. the saga instance after a transition).</summary>
    public string? SnapshotJson { get; set; }
}

/// <summary>
/// Latest known saga state per order, projected from SagaTransitioned diagnostics.
/// The Web cannot read the Orders database, so it learns about the saga only through the telemetry
/// the Orders service publishes.
/// </summary>
public sealed class SagaSnapshot
{
    /// <summary>Primary key: the saga's CorrelationId (= OrderId).</summary>
    public Guid CorrelationId { get; set; }
    /// <summary>Saga state name as reported by Orders (e.g. AwaitingPayment).</summary>
    public string CurrentState { get; set; } = "";
    /// <summary>Description of the last transition ("AwaitingStock → AwaitingPayment on StockReserved").</summary>
    public string? LastTransition { get; set; }
    /// <summary>The full saga instance as JSON, for the saga inspector.</summary>
    public string? SnapshotJson { get; set; }
    /// <summary>How many transitions were received for this saga.</summary>
    public int Transitions { get; set; }
    /// <summary>OccurredAtUtc of the newest applied transition (ordering guard).</summary>
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>
/// How far behind the read side is (eventual consistency made visible).
/// Updated in the same transaction as each projection, so it is always consistent with the data.
/// </summary>
public sealed class ProjectionCheckpoint
{
    /// <summary>Primary key: name of the projection (today a single one, "ReadModel").</summary>
    public string Projection { get; set; } = "";
    /// <summary>Last EventAudit sequence applied (reserved for a per-projection replay; not filled today).</summary>
    public long LastSeq { get; set; }
    /// <summary>OccurredAtUtc of the last applied event.</summary>
    public DateTime? LastEventOccurredAtUtc { get; set; }
    /// <summary>When the Web applied it.</summary>
    public DateTime? LastAppliedAtUtc { get; set; }
    /// <summary>Total number of events applied since the last reset.</summary>
    public long AppliedCount { get; set; }
    /// <summary>Applied - Occurred of the last event, in milliseconds.  Grows visibly during S08 (slow projection).</summary>
    public double LastLagMs { get; set; }
}

/// <summary>One execution of a Scenario Lab scenario.</summary>
public sealed class ScenarioRun
{
    /// <summary>Primary key; the same id is stamped as ScenarioId on every message of the run.</summary>
    public Guid Id { get; set; }                       // = ScenarioId stamped on every message of the run
    /// <summary>Scenario code, S01..S16.</summary>
    public string Code { get; set; } = "";             // S01..S16
    /// <summary>Scenario title shown in the lab.</summary>
    public string Title { get; set; } = "";
    /// <summary>Running / PASSED / FAILED / INCONCLUSIVE.</summary>
    public string Status { get; set; } = "Running";    // Running / PASSED / FAILED / INCONCLUSIVE
    /// <summary>Customer used by the run (scenarios often use their own customer to stay isolated).</summary>
    public string? CustomerId { get; set; }
    /// <summary>Main order of the run, if any.</summary>
    public Guid? OrderId { get; set; }
    /// <summary>All orders created by the run, as a JSON array.</summary>
    public string OrderIdsJson { get; set; } = "[]";
    /// <summary>The message sequence the scenario expects (JSON array of type names).</summary>
    public string ExpectedTraceJson { get; set; } = "[]";
    /// <summary>The message sequence actually observed (see ReadModelQueries.GetActualTraceAsync).</summary>
    public string ActualTraceJson { get; set; } = "[]";
    /// <summary>One-line verdict.</summary>
    public string? Summary { get; set; }
    /// <summary>Step-by-step log of the run, shown in the lab.</summary>
    public string Log { get; set; } = "";
    /// <summary>Number of MessageConsumed diagnostics recorded for the run (successful consumptions in every service).</summary>
    public int DeliveryAttempts { get; set; }
    /// <summary>How many InboxDuplicateIgnored diagnostics were recorded during the run (duplicates safely skipped).</summary>
    public int DedupHits { get; set; }
    /// <summary>Saga state at the end of the run.</summary>
    public string? FinalSagaState { get; set; }
    /// <summary>When the run started.</summary>
    public DateTime StartedAtUtc { get; set; }
    /// <summary>When the run finished (null while running).</summary>
    public DateTime? FinishedAtUtc { get; set; }
}

/// <summary>One invariant check of a run, with the evidence that was read from the owner service.</summary>
public sealed class ScenarioAssertion
{
    /// <summary>Auto-increment primary key (also gives the display order).</summary>
    public long Id { get; set; }
    /// <summary>The ScenarioRun this check belongs to.</summary>
    public Guid RunId { get; set; }
    /// <summary>The invariant, e.g. "exactly one debit for the order".</summary>
    public string Name { get; set; } = "";
    /// <summary>Whether the invariant held.</summary>
    public bool Passed { get; set; }
    /// <summary>What was read (from the owner's read-only /diagnostics endpoints) to decide.</summary>
    public string Evidence { get; set; } = "";
    /// <summary>When the check ran.</summary>
    public DateTime AtUtc { get; set; }
}
