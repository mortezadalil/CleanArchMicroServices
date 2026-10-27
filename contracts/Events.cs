// ==========================================================================================
// FILE: contracts/Events.cs
//
// WHAT THIS FILE IS
//   The INTEGRATION EVENTS of MiniShop: facts that one service announces to the others.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Part of contracts/ (linked as source into every service, see contracts/Message.cs).
//   Events are always written to the producer's Outbox table in the same transaction as the
//   business change (shared/Outbox/OutboxMessage.cs → PublishViaOutbox) and then published by the
//   OutboxDispatcher.  They are consumed by:
//     - the Orders Saga (src/MiniShop.Orders/Saga) — to decide the next step of an order;
//     - the Web read model (src/MiniShop.Web/Projections) — the "Q" side of CQRS, which builds
//       product cards, order lists and wallet summaries from these events;
//     - Inventory (OrderConfirmed, ReadModelSnapshotRequested) and Payments (ReadModelSnapshotRequested).
//
// WHICH PATTERN IT IMPLEMENTS
//   Event-driven integration / publish-subscribe.  The producer does not know who listens; RabbitMQ
//   fans each event out to every queue that subscribed to its type (see contracts/Topology.cs).
//
// Read next: contracts/Topology.cs, then src/MiniShop.Orders/Saga/OrderSaga.cs (who reacts to what)
//            and src/MiniShop.Web/Projections/Projector.cs (how events become read models).
// ==========================================================================================

namespace MiniShop.Contracts;

// ------------------------------------------------------------------------------------------
// INTEGRATION EVENTS
// An event is a fact that already happened ("this was done").  It is PUBLISHED, and any number
// of services may subscribe: the Orders saga reacts to them and the Web read model projects them.
//
// Because delivery is at-least-once, every subscriber must tolerate receiving the same event
// twice (S06 duplicate delivery, S11 crash after publish).  The Inbox (MessageId) handles exact
// duplicates; projections are additionally written to be idempotent (they overwrite with the
// latest version instead of adding).
//
// Many Inventory/Payments events carry a "StockAfter" / "BalanceAfter" snapshot.  This is
// EVIDENCE for the Developer Console (Before/During/After views), not something the saga decides on.
// ------------------------------------------------------------------------------------------

/// <summary>Orders: a new order was accepted (state Pending) and the saga must start.</summary>
/// <remarks>
/// Written to the Orders outbox in the SAME transaction as the Order row, so "order saved but event lost"
/// (the dual-write problem) cannot happen.  Scenario Lab: S04 (broker down: the event waits in the outbox),
/// S11 (published twice because of a crash before mark-as-published), S14 (same Idempotency-Key → only one OrderPlaced).
/// </remarks>
/// <param name="OrderId">The new order (also the CorrelationId of the whole flow).</param>
/// <param name="CustomerId">Who placed it (the wallet that will be charged).</param>
/// <param name="Amount">Total to pay in cents (sum of the line totals).</param>
/// <param name="Lines">What was ordered, with checkout prices.</param>
public sealed record OrderPlaced(Guid OrderId, string CustomerId, long Amount, IReadOnlyList<OrderLine> Lines) : Message;

/// <summary>Inventory: stock was reserved for the order. Carries the stock snapshot as evidence for the UI.</summary>
/// <param name="OrderId">The order whose items are now reserved.</param>
/// <param name="StockAfter">Stock levels of the affected products right after the reservation.</param>
public sealed record StockReserved(Guid OrderId, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Inventory: the order could not be reserved (out of stock, price mismatch, cancelled...).</summary>
/// <remarks>A business "no" is a normal event, not an exception: it is not retried and never goes to an error queue (S02, S15).</remarks>
/// <param name="OrderId">The order that was refused.</param>
/// <param name="Reason">Why (shown to the customer and in the console).</param>
/// <param name="StockAfter">Stock levels at the moment of the decision (unchanged by this order).</param>
public sealed record StockReservationRejected(Guid OrderId, string Reason, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Inventory: the reservation was given back (or confirmed as never existing).</summary>
/// <remarks>Answer to the compensating command <see cref="ReleaseStock"/> (S03, S10).</remarks>
/// <param name="OrderId">The order whose reservation is gone.</param>
/// <param name="Reason">The reason copied from the ReleaseStock command.</param>
/// <param name="StockAfter">Stock levels after the units became available again.</param>
public sealed record StockReleased(Guid OrderId, string Reason, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Payments: the wallet was debited exactly once for the order.</summary>
/// <param name="OrderId">The order that was paid.</param>
/// <param name="CustomerId">The wallet owner.</param>
/// <param name="Amount">Amount debited, in cents.</param>
/// <param name="BalanceAfter">Wallet balance after the debit (evidence for the UI).</param>
public sealed record PaymentCompleted(Guid OrderId, string CustomerId, long Amount, long BalanceAfter) : Message;

/// <summary>Payments: the debit was refused (insufficient funds, injected failure, cancelled window...).</summary>
/// <remarks>Triggers compensation in the saga: it sends ReleaseStock (S03).</remarks>
/// <param name="OrderId">The order that could not be paid.</param>
/// <param name="CustomerId">The wallet owner.</param>
/// <param name="Amount">Amount that was requested, in cents.</param>
/// <param name="Reason">Why the debit was refused.</param>
/// <param name="BalanceAfter">Wallet balance (unchanged) at the moment of the decision.</param>
public sealed record PaymentFailed(Guid OrderId, string CustomerId, long Amount, string Reason, long BalanceAfter) : Message;

/// <summary>Payments → Saga (answer to CancelPayment): no debit happened and none can happen anymore.</summary>
/// <param name="OrderId">The order whose payment window is now closed for good.</param>
/// <param name="Reason">Reason copied from the CancelPayment command.</param>
public sealed record PaymentCancelled(Guid OrderId, string Reason) : Message;

/// <summary>Payments → Saga (answer to CancelPayment): the debit already happened, so the order must be confirmed.</summary>
/// <remarks>This is the "late payment" case (S13): the timeout fired, but the money was already taken.</remarks>
/// <param name="OrderId">The order that was in fact paid.</param>
/// <param name="Amount">Amount that was debited, in cents.</param>
/// <param name="BalanceAfter">Wallet balance after that earlier debit.</param>
public sealed record PaymentAlreadyCompleted(Guid OrderId, long Amount, long BalanceAfter) : Message;

/// <summary>Orders: the saga finished successfully. Inventory turns the reservation into real consumption.</summary>
/// <param name="OrderId">The confirmed order.</param>
/// <param name="CustomerId">Who placed it.</param>
/// <param name="Amount">What was paid, in cents.</param>
public sealed record OrderConfirmed(Guid OrderId, string CustomerId, long Amount) : Message;

/// <summary>Orders: the saga finished with a rejection (all compensations confirmed).</summary>
/// <param name="OrderId">The rejected order.</param>
/// <param name="CustomerId">Who placed it.</param>
/// <param name="Reason">The first reason that made the order fail (out of stock, insufficient funds, timeout...).</param>
public sealed record OrderRejected(Guid OrderId, string CustomerId, string Reason) : Message;

/// <summary>Orders: compensation could not be confirmed after the allowed attempts; a human must look.</summary>
/// <remarks>
/// A saga cannot always undo everything automatically.  Instead of retrying forever or pretending the
/// order is clean, it stops and flags the order (S12).
/// </remarks>
/// <param name="OrderId">The order that needs manual attention.</param>
/// <param name="CustomerId">Who placed it.</param>
/// <param name="Reason">What could not be compensated.</param>
public sealed record OrderRequiresAttention(Guid OrderId, string CustomerId, string Reason) : Message;

/// <summary>Inventory: the catalog (price / availability) changed. The Web projects product cards from this.</summary>
/// <remarks>
/// The Web projection OVERWRITES the product card with these values (it does not add/subtract), so applying
/// the same event twice gives the same result: the projection is idempotent.  It also ignores an event whose
/// OccurredAtUtc is older than the card it already has, so a late/out-of-order delivery cannot move the card back in time.
/// </remarks>
/// <param name="ProductId">The product that changed.</param>
/// <param name="Sku">Stock-keeping unit (short product code).</param>
/// <param name="Name">Display name.</param>
/// <param name="Price">Current price in cents.</param>
/// <param name="OnHand">Units physically in stock.</param>
/// <param name="Reserved">Units held for orders that are not finished yet.</param>
/// <param name="Version">Catalog version of the product (copied onto the Web product card; not the stock row's concurrency token).</param>
public sealed record ProductCatalogChanged(Guid ProductId, string Sku, string Name, long Price, int OnHand, int Reserved, int Version) : Message
{
    /// <summary>Units that can still be sold = on hand minus reserved.  Computed, so it can never disagree with the two inputs.</summary>
    public int Available => OnHand - Reserved;
}

/// <summary>Payments: wallet balance changed. The Web projects the wallet summary from this.</summary>
/// <param name="CustomerId">The wallet owner.</param>
/// <param name="Balance">New balance in cents.</param>
public sealed record WalletBalanceChanged(string CustomerId, long Balance) : Message;

/// <summary>Snapshot of one product's stock, embedded in Inventory events as evidence for Before/During/After views.</summary>
/// <remarks>Not a message itself (it does not derive from <see cref="Message"/>): just a value carried inside one.</remarks>
/// <param name="ProductId">The product.</param>
/// <param name="Name">Its display name.</param>
/// <param name="OnHand">Units physically in stock.</param>
/// <param name="Reserved">Units held for unfinished orders.</param>
public sealed record StockLevel(Guid ProductId, string Name, int OnHand, int Reserved)
{
    /// <summary>Units that can still be sold = on hand minus reserved.</summary>
    public int Available => OnHand - Reserved;
}

/// <summary>
/// Web → everyone: "my read model is empty or incomplete, please re-announce your reference data".
///
/// Why it exists: RabbitMQ only delivers an event to queues that are bound at the moment of publishing.
/// On a first start, Inventory and Payments may publish ProductCatalogChanged / WalletBalanceChanged
/// before the Web has created its queue, and those events are simply dropped.  The broker is not an
/// event store, so a late subscriber must ask the owners for a snapshot.  Inventory answers with one
/// ProductCatalogChanged per product, Payments with one WalletBalanceChanged per wallet; the projections
/// are idempotent, so answering more than once is harmless.
/// </summary>
/// <remarks>Sent by src/MiniShop.Web/Projections/ReadModelBootstrapper.cs and after a demo reset.</remarks>
/// <param name="RequestedBy">Service asking for the snapshot (normally "Web").</param>
/// <param name="Reason">Why ("read model is empty", "demo reset"), for logs and the console.</param>
public sealed record ReadModelSnapshotRequested(string RequestedBy, string Reason) : Message;
