// ==================================================================================================
// FILE: src/MiniShop.Web/Projections/Projector.cs
//
// WHAT IT IS
//   The heart of the read model: the code that turns ONE business event into changes on the read-model
//   tables (ProductCards, WalletBalances, Orders).  It does not save; the caller does (inside the
//   consumer's transaction, or at the end of a rebuild).
//
// WHERE IT SITS IN THE ARCHITECTURE
//   CQRS read side, inside MiniShop.Web.  It is called from two places:
//     1. live:    ProjectionConsumer (EventConsumers.cs), once per event received from RabbitMQ;
//     2. rebuild: ReadModelRebuilder, once per event replayed from EventAudit.
//   Using the SAME code for both guarantees that a rebuild produces the same tables as live traffic.
//
// PATTERN: idempotent, order-tolerant projection
//   RabbitMQ gives us AT-LEAST-ONCE delivery (a message can arrive twice) and NO GLOBAL ORDER (events
//   from Inventory and Payments travel on different paths; a retried message arrives after newer
//   ones).  The Inbox stops exact duplicates, but the projection itself must still survive re-ordering:
//     - upsert: "create the row if missing, otherwise update it" (never "insert or fail");
//     - ordering guard: if the event OCCURRED before the row's UpdatedAtUtc, it is old news: ignore it;
//     - terminal guard: an order that is Confirmed / Rejected / RequiresAttention never goes back to
//       an in-progress state, even if a late "StockReserved" shows up.
//
// Read next: EventConsumers.cs (who calls ApplyAsync and saves), then ReadModelRebuilder.cs.
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Messaging;
using MiniShop.Web.Data;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Projections;

/// <summary>
/// Applies ONE business event to the read-model tables.  Rules that make projections safe under
/// at-least-once, out-of-order delivery:
///   - upserts only (never "insert or fail");
///   - an older event never overwrites a newer row (compare OccurredAtUtc);
///   - a terminal order state (Confirmed / Rejected / RequiresAttention) is never downgraded.
/// The same code is used live (from RabbitMQ) and during a rebuild (from EventAudit).
/// Registered as SCOPED: it uses the DbContext of the current message (or rebuild) and leaves the
/// SaveChanges to the caller, so the projection, the audit row and the inbox row commit together.
/// </summary>
public sealed class Projector(ReadModelDbContext db)
{
    /// <summary>
    /// Applies <paramref name="event"/> and returns a human-readable description of what happened
    /// (stored in EventAudit.Detail and shown in the timeline).  Changes are only tracked, not saved.
    /// </summary>
    public async Task<string> ApplyAsync(Message @event, CancellationToken ct)
    {
        // Pattern matching on the runtime type: one branch per event that changes a table.
        switch (@event)
        {
            // Inventory announced the full, current state of one product.
            case ProductCatalogChanged e:
                return await UpsertProductAsync(e, ct);
            // Payments announced the FINAL balance of one wallet (not a delta).
            case WalletBalanceChanged e:
                return await UpsertWalletAsync(e, ct);
            // A new order: create the row and fill the fields only OrderPlaced knows (customer, amount, lines).
            // The init callback runs even if a later event already created the row (out of order), so the
            // row is completed instead of duplicated.
            case OrderPlaced e:
                return await UpsertOrderAsync(e.OrderId, e, "Pending", null, o =>
                {
                    o.CustomerId = e.CustomerId;
                    o.Amount = e.Amount;
                    o.LinesJson = Json.Serialize(e.Lines);
                    o.ScenarioId = e.ScenarioId;
                    o.CreatedAtUtc = e.OccurredAtUtc;
                }, ct);
            // Inventory reserved the stock: the saga now waits for the payment.
            case StockReserved e:
                return await UpsertOrderAsync(e.OrderId, e, "AwaitingPayment", null, null, ct);
            // Not enough stock: the order is on its way to Rejected (OrderRejected will make it final).
            case StockReservationRejected e:
                return await UpsertOrderAsync(e.OrderId, e, "Rejecting", e.Reason, null, ct);
            // Payment failed: the saga compensates (releases the reserved stock) before rejecting.
            case PaymentFailed e:
                return await UpsertOrderAsync(e.OrderId, e, "Compensating", e.Reason, null, ct);
            // Payment cancelled (e.g. saga timeout): compensation as well.
            case PaymentCancelled e:
                return await UpsertOrderAsync(e.OrderId, e, "Compensating", e.Reason, null, ct);
            // Terminal: success.
            case OrderConfirmed e:
                return await UpsertOrderAsync(e.OrderId, e, "Confirmed", null, null, ct);
            // Terminal: failure, with the reason.
            case OrderRejected e:
                return await UpsertOrderAsync(e.OrderId, e, "Rejected", e.Reason, null, ct);
            // Terminal: the saga could not finish on its own (e.g. compensation kept failing); a human must look.
            case OrderRequiresAttention e:
                return await UpsertOrderAsync(e.OrderId, e, "RequiresAttention", e.Reason, null, ct);
            // Events that do not change any table are still audited by the consumer and appear in the timeline.
            default:
                return $"{@event.Type}: no projection"; // PaymentCompleted, StockReleased... only feed the timeline
        }
    }

    /// <summary>
    /// States an order can never leave.  Once the saga has finished, any late event about that order is
    /// out of date by definition and must not "re-open" it in the UI.
    /// </summary>
    private static readonly HashSet<string> TerminalStates = ["Confirmed", "Rejected", "RequiresAttention"];

    /// <summary>
    /// Upsert of an <see cref="OrderSummary"/> with the two ordering guards.
    /// <paramref name="init"/> fills fields only the creating event knows (null for later events).
    /// </summary>
    private async Task<string> UpsertOrderAsync(Guid orderId, Message e, string newState, string? reason,
        Action<OrderSummary>? init, CancellationToken ct)
    {
        // Load the existing row, if any (tracked, so changes are saved by the caller's SaveChanges).
        var order = await db.Orders.FirstOrDefaultAsync(o => o.OrderId == orderId, ct);
        // Upsert: an event about an order we have never seen (OrderPlaced not arrived yet, or lost before
        // our queue existed) still creates a row, instead of failing and retrying forever.
        if (order is null)
        {
            order = new OrderSummary { OrderId = orderId, CreatedAtUtc = e.OccurredAtUtc, ScenarioId = e.ScenarioId };
            db.Orders.Add(order);
        }
        // Fill the OrderPlaced-only fields.  Done BEFORE the guards on purpose: even if OrderPlaced arrives
        // after a newer event (so its state is ignored), its customer/amount/lines are still needed.
        init?.Invoke(order);

        // Ordering guards: never go back in time, never leave a terminal state.
        // isOlder: this event happened before the one that produced the current row.
        var isOlder = e.OccurredAtUtc < order.UpdatedAtUtc;
        // wouldDowngrade: the row is final and this event would move it back to an in-progress state.
        var wouldDowngrade = TerminalStates.Contains(order.State) && !TerminalStates.Contains(newState);
        // Ignoring is NOT an error: the message is still acknowledged, audited and marked in the inbox.
        if (isOlder || wouldDowngrade)
            return $"{e.Type}: ignored for projection (order already {order.State}, newer)";

        // Apply the new state.  Keep an earlier failure reason when this event has none.
        order.State = newState;
        order.FailureReason = reason ?? order.FailureReason;
        // Remember WHEN (producer time) the applied event happened: this is the next guard value.
        order.UpdatedAtUtc = e.OccurredAtUtc;
        // For the UI: "updated by OrderConfirmed".
        order.LastEvent = e.Type;
        return $"{e.Type}: order {orderId:N} → {newState}";
    }

    /// <summary>Upsert of a <see cref="ProductCard"/>.  The event carries the whole product, so it simply replaces the row.</summary>
    private async Task<string> UpsertProductAsync(ProductCatalogChanged e, CancellationToken ct)
    {
        var card = await db.ProductCards.FirstOrDefaultAsync(p => p.ProductId == e.ProductId, ct);
        // First time we hear of this product: create the card.
        if (card is null)
        {
            card = new ProductCard { ProductId = e.ProductId };
            db.ProductCards.Add(card);
        }
        // Ordering guard: a late, older snapshot must not overwrite fresher stock numbers.
        else if (e.OccurredAtUtc < card.UpdatedAtUtc)
            return "ProductCatalogChanged: older than current card, ignored";

        // Overwrite every field with the event's values ("state transfer" event: no arithmetic, so a
        // duplicate gives exactly the same result).
        card.Sku = e.Sku;
        card.Name = e.Name;
        card.Price = e.Price;
        card.OnHand = e.OnHand;
        card.Reserved = e.Reserved;
        card.Version = e.Version;
        card.UpdatedAtUtc = e.OccurredAtUtc;
        return $"ProductCard {e.Name}: onHand {e.OnHand}, reserved {e.Reserved}";
    }

    /// <summary>
    /// Upsert of a <see cref="WalletBalance"/>.  WalletBalanceChanged carries the FINAL balance, not a
    /// delta: we assign, we never add.  Adding a delta would double-count a duplicate and give wrong
    /// totals when events arrive out of order.
    /// </summary>
    private async Task<string> UpsertWalletAsync(WalletBalanceChanged e, CancellationToken ct)
    {
        var wallet = await db.WalletBalances.FirstOrDefaultAsync(w => w.CustomerId == e.CustomerId, ct);
        // New customer: create the row.
        if (wallet is null)
        {
            wallet = new WalletBalance { CustomerId = e.CustomerId };
            db.WalletBalances.Add(wallet);
        }
        // Ordering guard: an older balance must not replace a newer one.
        else if (e.OccurredAtUtc < wallet.UpdatedAtUtc)
            return "WalletBalanceChanged: older than current row, ignored";

        // Assign (not add) the final balance and remember when it was true.
        wallet.Balance = e.Balance;
        wallet.UpdatedAtUtc = e.OccurredAtUtc;
        return $"WalletBalance {e.CustomerId}: {e.Balance:N0}";
    }
}
