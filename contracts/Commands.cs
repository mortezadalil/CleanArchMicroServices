namespace MiniShop.Contracts;

// ------------------------------------------------------------------------------------------
// COMMANDS
// A command is an instruction sent to exactly ONE service ("do this").  It is delivered to
// the destination service's queue (see Topology.cs).  The receiver may refuse it, and must
// always answer with an event so the Saga can move on.
// ------------------------------------------------------------------------------------------

/// <summary>Orders → Inventory: reserve the items of an order. Prices are the checkout snapshot; Inventory re-validates them.</summary>
public sealed record ReserveStock(Guid OrderId, IReadOnlyList<OrderLine> Lines) : Message;

/// <summary>
/// Orders → Inventory: give a reservation back (compensation). Idempotent: releasing twice is harmless.
/// If no reservation exists yet (e.g. a ReserveStock that is still in flight), Inventory records a
/// "cancelled" tombstone so that a late ReserveStock is rejected instead of locking stock forever.
/// </summary>
public sealed record ReleaseStock(Guid OrderId, string Reason) : Message;

/// <summary>Orders → Payments: debit the customer's wallet for the order amount.</summary>
public sealed record ChargeWallet(Guid OrderId, string CustomerId, long Amount) : Message;

/// <summary>
/// Orders → Payments: the Saga waited too long for a payment answer and wants a definitive decision.
/// Payments answers with PaymentAlreadyCompleted (money was taken, so the order should be confirmed)
/// or PaymentCancelled (a tombstone is stored so that a late ChargeWallet can never debit).
/// </summary>
public sealed record CancelPayment(Guid OrderId, string Reason) : Message;
