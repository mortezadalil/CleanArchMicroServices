// ==================================================================================================
// FILE: src/MiniShop.Inventory/Consumers/ReserveStockConsumer.cs
//
// WHAT THIS FILE IS
//   The message handler for the ReserveStock COMMAND: "please hold these products for order X".
//   It is the first step of the order saga after the order was placed.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Orders (saga) --ReserveStock (sent to queue minishop.inventory)--> THIS CONSUMER
//   THIS CONSUMER --StockReserved or StockReservationRejected (published)--> Orders saga (+ Web read model)
//   It also publishes ProductCatalogChanged for every product whose numbers changed, so the Web's
//   product cards show the new "available" count.
//
// PATTERNS
//   - Idempotent consumer (Inbox): inherited from shared/Messaging/IdempotentConsumer.cs.  The base class
//     opens ONE transaction, skips a MessageId it has already seen, calls HandleAsync below, stores the
//     MessageId in the Inbox, and commits.  So HandleAsync must NOT call SaveChanges itself.
//   - Business idempotency: the Inbox only catches the SAME message twice.  A saga retry sends a NEW
//     message (new MessageId) with the SAME OrderId, so we also check "is there already a reservation for
//     this order?" and, if so, repeat the earlier answer (ReAnswer) instead of reserving twice.
//   - Transactional Outbox: answers are added as Outbox rows in the same transaction as the stock change,
//     so "stock reserved" and "StockReserved will be published" are committed together or not at all.
//   - Never trust the client: the price the customer saw is re-validated against the catalog price.
//   - Optimistic concurrency (Stock.Version): two orders racing for the last unit (S15) cannot both win.
//   - Tombstone: a Cancelled row written by an earlier ReleaseStock makes a late ReserveStock fail.
//
// Read next: Consumers/ReleaseStockConsumer.cs (compensation: giving a reservation back).
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Data;
using MiniShop.Inventory.Domain;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Consumers;

/// <summary>
/// Handles the ReserveStock command.  Business idempotency key = OrderId: if a reservation already
/// exists for the order, the previous answer is simply re-published (a different message may carry the
/// same command, e.g. after a saga retry).
///
/// Rejection reasons: unknown/inactive product, price snapshot differs from the catalog, insufficient
/// stock, or the order was already cancelled (tombstone written by an earlier ReleaseStock).
///
/// Note that a rejection is a normal BUSINESS ANSWER (an event the saga reacts to), not an exception:
/// exceptions mean "something broke, retry me"; "no stock" will not get better by retrying.
/// </summary>
/// <remarks>
/// The constructor parameters are resolved by dependency injection (MassTransit creates one consumer per
/// message, in its own DI scope, so <paramref name="db"/> is a fresh DbContext for every message) and are
/// passed straight to the <see cref="IdempotentConsumer{TMessage,TDbContext}"/> base class.
/// </remarks>
/// <param name="db">The Inventory database (business tables + Inbox + Outbox).</param>
/// <param name="diagnostics">Publishes telemetry for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit so answers leave immediately.</param>
/// <param name="faults">Fault-injection rules (Development only), used for the FaultPoint below.</param>
/// <param name="logger">Standard logger.</param>
public sealed class ReserveStockConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ReserveStockConsumer> logger)
    : IdempotentConsumer<ReserveStock, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>
    /// Name of this consumer's fault-injection point ("Inventory.ReserveStock").  The base class applies a
    /// matching rule BEFORE the transaction starts: Fail (throw → retry), Delay (slow Inventory, used by S09),
    /// Pause, or Reject (we turn it into a business rejection below via <c>Fault.Reject</c>).
    /// </summary>
    protected override string FaultPoint => FaultPoints.InventoryReserve;

    /// <summary>
    /// The business logic of "reserve stock for an order".  Runs INSIDE the base class's transaction:
    /// it only changes tracked entities and adds Outbox rows; the base class saves and commits.
    /// </summary>
    /// <param name="context">MassTransit's view of the delivered message (the command + headers + cancellation).</param>
    /// <returns>A human-readable detail and the stock levels, shown in the Developer Console timeline.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReserveStock> context)
    {
        // The command itself: OrderId + the order lines (product, quantity, the unit price the customer saw).
        var command = context.Message;
        // Cancellation token: set when the service is shutting down, so database calls stop promptly.
        var ct = context.CancellationToken;

        // BUSINESS IDEMPOTENCY: does this order already have a reservation row (in ANY state)?
        // Include(Lines) loads the reserved lines too, in case we need them for the answer.
        var existing = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == command.OrderId, ct);
        // Yes → we already decided for this order.  Do not touch the stock again; just repeat our answer,
        // because the saga sent the command again (it may never have received the first answer) and is
        // waiting for one.  Staying silent here would leave the saga waiting until its timeout.
        if (existing is not null)
            return await ReAnswerAsync(existing, command, ct);

        // First time we see this order: create its reservation row now, before validation, so that even a
        // rejection is RECORDED (a later duplicate must get the same "rejected" answer, see ReAnswerAsync).
        var now = DateTime.UtcNow;
        var reservation = new Reservation { Id = Guid.NewGuid(), OrderId = command.OrderId, CreatedAtUtc = now, UpdatedAtUtc = now };
        // Add() only marks the row for insertion; it is written when the base class calls SaveChanges.
        // If a concurrent duplicate already inserted a row for this OrderId, the unique index makes that
        // SaveChanges fail → the transaction rolls back → retry → the retry takes the ReAnswer path above.
        Db.Reservations.Add(reservation);

        // Decide whether we may reserve.  An injected "Reject" fault (Scenario Lab) wins and supplies the reason;
        // otherwise the real business checks run.  null means "all good".
        var rejection = Fault.Reject ? Fault.Reason : await ValidateAsync(command, ct);
        if (rejection is not null)
        {
            // Record the refusal and its reason on the reservation row (no lines: nothing is held).
            reservation.State = ReservationState.Rejected;
            reservation.Reason = rejection;
            // Queue the answer for the saga through the Outbox (published after commit by the dispatcher).
            // Stamp() copies the correlation/causation ids from the command so the event joins the order's trace.
            Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, rejection, await Db.StockLevelsAsync(ct)), command));
            // Return normally (no exception!): a rejection is a valid outcome and must be committed, so the
            // Inbox row, the Rejected row and the outbox event are all saved together.
            return new HandleResult($"reservation rejected: {rejection}", await Db.StockLevelsAsync(ct));
        }

        // All checks passed: hold the stock. Version++ makes a concurrent reservation of the same product fail and retry.
        foreach (var line in command.Lines)
        {
            // Load the (tracked) stock row of this product; it exists because ValidateAsync checked the product.
            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            // Hold the units: they stay in the warehouse (OnHand unchanged) but are no longer Available.
            stock.Reserved += line.Quantity;
            // Bump the concurrency token: the UPDATE will only succeed if nobody changed this row since we read it
            // (S15: two orders for the last unit → one commits, the other gets a concurrency error, retries,
            // re-reads Available = 0 and is rejected).  Stock can therefore never be over-reserved.
            stock.Version++;
            // Remember what we held, because ReleaseStock / OrderConfirmed only carry the OrderId.
            reservation.Lines.Add(new ReservationLine { ProductId = line.ProductId, Quantity = line.Quantity });

            // Tell the read model (Web product cards) that this product's numbers changed.
            var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
            Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, command));
        }
        // The reservation is now active: the units are held until Release (compensation) or Commit (confirmed).
        reservation.State = ReservationState.Reserved;

        // Snapshot of all stock levels AFTER the change (pending in this transaction), carried as evidence.
        var levels = await Db.StockLevelsAsync(ct);
        // The answer the saga is waiting for: it will now go on and ask Payments to charge the wallet.
        Db.PublishViaOutbox(Stamp(new StockReserved(command.OrderId, levels), command));
        // Back to the base class, which adds the Inbox row, saves everything in one go and commits.
        return new HandleResult($"reserved {command.Lines.Sum(l => l.Quantity)} unit(s) for order {command.OrderId}", levels);
    }

    /// <summary>
    /// Returns a rejection reason, or null when the order can be reserved.
    /// Checks every line; the first problem found becomes the reason (a whole order is reserved or nothing).
    /// </summary>
    /// <param name="command">The ReserveStock command with the customer's lines.</param>
    /// <param name="ct">Cancellation token.</param>
    private async Task<string?> ValidateAsync(ReserveStock command, CancellationToken ct)
    {
        foreach (var line in command.Lines)
        {
            // The product must exist and still be on sale.  A message could name any id (a stale cart,
            // a bug, a tampered request), so the catalog here is the only authority.
            var product = await Db.Products.FirstOrDefaultAsync(p => p.Id == line.ProductId, ct);
            if (product is null || !product.IsActive)
                return $"product {line.ProductId} is not available";
            // PRICE RE-VALIDATION.  UnitPrice is the price the customer SAW (it came from the browser via Orders).
            // Never trust it: the price may have changed since the page was loaded, or someone may have edited
            // the request.  If it differs from the catalog price we refuse, and the customer must refresh the cart.
            if (product.Price != line.UnitPrice)
                return $"price of {product.Name} changed ({line.UnitPrice:N0} → {product.Price:N0}); please refresh the cart";

            // Enough units must be AVAILABLE (OnHand - Reserved), not merely on hand: units reserved for other
            // pending orders are already promised.  (S02: quantity 999 is rejected here.)
            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            if (stock.Available < line.Quantity)
                return $"insufficient stock for {product.Name}: requested {line.Quantity}, available {stock.Available}";
        }
        // Every line passed.
        return null;
    }

    /// <summary>
    /// The order already has a reservation record: repeat the earlier answer instead of changing anything.
    /// This is what makes the consumer idempotent on the BUSINESS key (OrderId), not just on MessageId.
    /// Whatever happens, an answer is always published, so the saga is never left waiting.
    /// </summary>
    /// <param name="existing">The reservation row found for this OrderId.</param>
    /// <param name="command">The (repeated) ReserveStock command; used to stamp the answer.</param>
    /// <param name="ct">Cancellation token.</param>
    private async Task<HandleResult> ReAnswerAsync(Reservation existing, ReserveStock command, CancellationToken ct)
    {
        // Current stock levels (unchanged by this message), included as evidence in the answer.
        var levels = await Db.StockLevelsAsync(ct);
        switch (existing.State)
        {
            // We reserved for this order before (and maybe it was even confirmed since): the answer was "yes".
            // Re-publish StockReserved, but do NOT add to Reserved again; that would lock the stock twice.
            case ReservationState.Reserved:
            case ReservationState.Committed:
                Db.PublishViaOutbox(Stamp(new StockReserved(command.OrderId, levels), command));
                return new HandleResult($"order already reserved ({existing.State}); StockReserved re-published, stock unchanged", levels);
            // TOMBSTONE: a ReleaseStock arrived BEFORE this ReserveStock (messages overtook each other).
            // The order is already given up, so reserving now would lock stock that nobody will ever release.
            // Refuse with a clear reason.
            case ReservationState.Cancelled:
                Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, "order was cancelled before stock could be reserved (tombstone)", levels), command));
                return new HandleResult("late ReserveStock refused: order already cancelled (tombstone)", levels);
            // Rejected or Released: the answer was "no" (or the hold is gone).  Repeat the stored reason so the
            // saga gets the same answer it would have got the first time.
            default:
                var reason = existing.Reason ?? $"reservation is {existing.State}";
                Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, reason, levels), command));
                return new HandleResult($"earlier answer repeated: rejected ({reason})", levels);
        }
    }

    /// <summary>
    /// Copies the tracing envelope from the message that caused an answer onto the answer itself.
    /// Messages are immutable records, so <c>with { ... }</c> creates a modified copy.
    /// Also used by ReleaseStockConsumer (hence <c>internal</c>).
    /// </summary>
    /// <typeparam name="T">The concrete event type (StockReserved, StockReleased...).</typeparam>
    /// <param name="event">The freshly built answer.</param>
    /// <param name="cause">The command that triggered it.</param>
    /// <returns>A copy of <paramref name="event"/> linked to <paramref name="cause"/>.</returns>
    internal static T Stamp<T>(T @event, Message cause) where T : Message => @event with
    {
        // Same flow: CorrelationId is the OrderId, so the saga can find the order this answer belongs to.
        CorrelationId = cause.CorrelationId,
        // Parent link: "this event was caused by that command" (draws the arrows in the timeline).
        CausationId = cause.MessageId,
        // Keep the Scenario Lab experiment id so scoped fault rules and the UI follow the whole chain.
        ScenarioId = cause.ScenarioId,
        // Who produced the event.
        Producer = ServiceNames.Inventory,
    };
}
