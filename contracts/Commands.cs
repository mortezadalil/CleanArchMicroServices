// ==========================================================================================
// FILE: contracts/Commands.cs
//
// WHAT THIS FILE IS
//   The COMMAND messages of MiniShop: instructions that the Orders Saga sends to Inventory and
//   Payments.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Part of contracts/ (linked as source into every service, see contracts/Message.cs).
//   Commands are produced by src/MiniShop.Orders/Saga (always through the Transactional Outbox,
//   see shared/Outbox/OutboxMessage.cs → SendViaOutbox) and consumed by:
//     - src/MiniShop.Inventory/Consumers (ReserveStock, ReleaseStock)
//     - src/MiniShop.Payments/Consumers  (ChargeWallet, CancelPayment)
//
// WHICH PATTERN IT IMPLEMENTS
//   Orchestrated Saga: one coordinator (the Orders Saga) tells each participant what to do with a
//   command, and the participant answers with an event (see contracts/Events.cs).  Compensating
//   commands (ReleaseStock, CancelPayment) undo or close earlier steps when the order fails.
//
// Read next: contracts/Events.cs (the answers), then src/MiniShop.Orders/Saga/OrderSaga.cs.
// ==========================================================================================

namespace MiniShop.Contracts;

// ------------------------------------------------------------------------------------------
// COMMANDS
// A command is an instruction sent to exactly ONE service ("do this").  It is delivered to
// the destination service's queue (see Topology.cs).  The receiver may refuse it, and must
// always answer with an event so the Saga can move on.
//
// Naming convention: commands are verbs in the imperative ("ReserveStock"), events are verbs in
// the past tense ("StockReserved").  Reading a message name tells you which kind it is.
//
// Every command can be DELIVERED MORE THAN ONCE (at-least-once delivery: outbox re-publish after a
// crash S11, injected duplicates S06).  The receivers therefore de-duplicate by MessageId (Inbox)
// AND are idempotent on the business key (OrderId), because the Saga may also send a second,
// different command for the same order (for example a retried timeout).
// ------------------------------------------------------------------------------------------

/// <summary>Orders → Inventory: reserve the items of an order. Prices are the checkout snapshot; Inventory re-validates them.</summary>
/// <remarks>
/// Answered with <see cref="StockReserved"/> or <see cref="StockReservationRejected"/>.
/// Scenario Lab: S15 (two orders race for the last unit; optimistic concurrency in Inventory makes
/// exactly one of them win) and S06 (duplicate delivery ignored by the Inbox).
/// </remarks>
/// <param name="OrderId">The order (also the CorrelationId of the message).</param>
/// <param name="Lines">The products, quantities and checkout prices to reserve.  IReadOnlyList so a consumer cannot modify the message.</param>
public sealed record ReserveStock(Guid OrderId, IReadOnlyList<OrderLine> Lines) : Message;

/// <summary>
/// Orders → Inventory: give a reservation back (compensation). Idempotent: releasing twice is harmless.
/// If no reservation exists yet (e.g. a ReserveStock that is still in flight), Inventory records a
/// "cancelled" tombstone so that a late ReserveStock is rejected instead of locking stock forever.
/// </summary>
/// <remarks>
/// Answered with <see cref="StockReleased"/>.  Scenario Lab: S12 (compensation fails repeatedly and the
/// order ends in "requires attention").
/// </remarks>
/// <param name="OrderId">The order whose reservation must be released.</param>
/// <param name="Reason">Human-readable reason (e.g. "payment failed"), shown in the UI and logs.</param>
public sealed record ReleaseStock(Guid OrderId, string Reason) : Message;

/// <summary>Orders → Payments: debit the customer's wallet for the order amount.</summary>
/// <remarks>
/// Answered with <see cref="PaymentCompleted"/> or <see cref="PaymentFailed"/>.  Payments guarantees
/// "at most one debit per OrderId" on its own ledger, independent of the Inbox, so even two different
/// ChargeWallet messages for the same order can never take money twice.
/// Scenario Lab: S10 (payment timeout), S13 (late payment arrives after the timeout).
/// </remarks>
/// <param name="OrderId">The order being paid.</param>
/// <param name="CustomerId">Whose wallet to debit.</param>
/// <param name="Amount">Amount in the smallest currency unit (cents).</param>
public sealed record ChargeWallet(Guid OrderId, string CustomerId, long Amount) : Message;

/// <summary>
/// Orders → Payments: the Saga waited too long for a payment answer and wants a definitive decision.
/// Payments answers with PaymentAlreadyCompleted (money was taken, so the order should be confirmed)
/// or PaymentCancelled (a tombstone is stored so that a late ChargeWallet can never debit).
/// </summary>
/// <remarks>
/// Why not just "give up"?  In a distributed system a timeout does not tell you whether the other side
/// did the work.  Asking the owner of the money for a final answer avoids both "charged but rejected"
/// and "confirmed but never charged".  Scenario Lab: S10 and S13.
/// </remarks>
/// <param name="OrderId">The order whose payment window is closed.</param>
/// <param name="Reason">Why the saga cancels (usually "payment timeout").</param>
public sealed record CancelPayment(Guid OrderId, string Reason) : Message;
