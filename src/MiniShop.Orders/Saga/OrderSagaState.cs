// ==================================================================================================
// MiniShop.Orders — Saga/OrderSagaState.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The list of saga states (SagaStates) and the persisted saga instance (OrderSagaState): one row per
//   order in the SagaInstances table of orders.db.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   A saga is a long-running business process made of several local transactions in different services
//   (reserve stock in Inventory, charge wallet in Payments, ...). There is no distributed transaction,
//   so the orchestrator must REMEMBER where each order is. That memory is this row. Because it lives in
//   the database (not in RAM), the saga survives a crash or restart of Orders (S09).
//
//   OrderSaga.cs reads it to decide; SagaRunner.cs writes the decision back; SagaTimeoutWorker.cs
//   watches DeadlineUtc.
//
// STATE CHART (technical saga states; every arrow is one case in OrderSaga.cs)
//
//   Started                      --OrderPlaced-------------------> WaitingForStock
//   WaitingForStock              --StockReserved-----------------> WaitingForPayment
//   WaitingForStock              --StockReservationRejected------> Rejected            (S02)
//   WaitingForStock              --timeout-----------------------> WaitingForRelease
//   WaitingForPayment            --PaymentCompleted--------------> Completed           (S01)
//   WaitingForPayment            --PaymentFailed-----------------> WaitingForRelease   (S03)
//   WaitingForPayment            --timeout-----------------------> WaitingForPaymentCancellation (S10)
//   WaitingForPaymentCancellation--PaymentCancelled / Failed-----> WaitingForRelease   (S10)
//   WaitingForPaymentCancellation--PaymentAlreadyCompleted/Completed-> Completed       (S13)
//   WaitingForRelease            --StockReleased-----------------> Rejected
//   WaitingFor{PaymentCancellation,Release} --timeout, attempts left--> same state (re-send)
//   WaitingFor{PaymentCancellation,Release} --timeout, no attempts left--> RequiresAttention (S12)
//   RequiresAttention            --manual retry------------------> WaitingForRelease
//   RequiresAttention            --StockReleased-----------------> Rejected
//   RequiresAttention            --PaymentCancelled--------------> WaitingForRelease
//
// Read next: Saga/OrderSaga.cs (the transitions themselves).
// ==================================================================================================

namespace MiniShop.Orders.Saga;

/// <summary>Saga states (see the state chart in README.md).</summary>
/// <remarks>
/// Plain string constants rather than an enum: they are stored as readable text in the database and
/// appear unchanged in logs, the audit table and the developer console.
/// </remarks>
public static class SagaStates
{
    /// <summary>Created by the API together with the order; waiting for our own OrderPlaced to come back.</summary>
    public const string Started = "Started";
    /// <summary>ReserveStock was sent to Inventory; waiting for StockReserved / StockReservationRejected.</summary>
    public const string WaitingForStock = "WaitingForStock";
    /// <summary>ChargeWallet was sent to Payments; waiting for PaymentCompleted / PaymentFailed.</summary>
    public const string WaitingForPayment = "WaitingForPayment";
    /// <summary>
    /// The payment timed out and CancelPayment was sent. We wait for a DEFINITIVE answer before
    /// compensating, because a timeout does not prove that no money was taken (S10, S13).
    /// </summary>
    public const string WaitingForPaymentCancellation = "WaitingForPaymentCancellation";
    /// <summary>ReleaseStock (the compensation) was sent; waiting for Inventory to confirm with StockReleased.</summary>
    public const string WaitingForRelease = "WaitingForRelease";
    /// <summary>Terminal: the order is confirmed.</summary>
    public const string Completed = "Completed";
    /// <summary>Terminal: the order is rejected and every compensation was confirmed.</summary>
    public const string Rejected = "Rejected";
    /// <summary>
    /// NOT terminal: the compensation was never confirmed (S12). The saga stops retrying on its own but
    /// can still move on after a manual retry or a late StockReleased.
    /// </summary>
    public const string RequiresAttention = "RequiresAttention";

    /// <summary>
    /// True for states that accept no more messages. Any message arriving afterwards is a late or
    /// duplicate one and is ignored (but still audited). RequiresAttention is deliberately NOT terminal.
    /// </summary>
    /// <param name="state">A value from this class.</param>
    public static bool IsTerminal(string state) => state is Completed or Rejected;
}

/// <summary>
/// Persisted saga instance (one per order, CorrelationId = OrderId).
/// The saga survives restarts because EVERY decision is stored here and in the Outbox in the same
/// transaction as the triggering message's Inbox row.  Version is an optimistic-concurrency token:
/// two messages for the same order processed at the same time cannot both win.
/// </summary>
public sealed class OrderSagaState
{
    /// <summary>Primary key; equal to the OrderId and to the CorrelationId of every message of this order.</summary>
    public Guid CorrelationId { get; set; }
    /// <summary>Current state, one of <see cref="SagaStates"/>. A new instance starts in Started.</summary>
    public string CurrentState { get; set; } = SagaStates.Started;
    /// <summary>Why the order is being compensated (carried into OrderRejected).</summary>
    public string? FailureReason { get; set; }
    /// <summary>How many times the current compensation command was (re)sent.</summary>
    public int CompensationAttempts { get; set; }
    /// <summary>When the timeout worker should act if no answer arrived. Null = not waiting.</summary>
    /// <remarks>
    /// Storing the deadline as DATA (not as an in-memory timer) is what makes timeouts survive a restart:
    /// after S09 the worker simply finds the overdue rows again.
    /// </remarks>
    public DateTime? DeadlineUtc { get; set; }
    /// <summary>
    /// Optimistic-concurrency token (configured in OrdersDbContext). Incremented on every transition;
    /// a writer that read an older Version gets DbUpdateConcurrencyException and must retry.
    /// </summary>
    public int Version { get; set; }
    /// <summary>When the saga was created (same moment as the order).</summary>
    public DateTime CreatedAtUtc { get; set; }
    /// <summary>When the saga last changed state.</summary>
    public DateTime UpdatedAtUtc { get; set; }
}
