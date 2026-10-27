// ==================================================================================================
// FILE: src/MiniShop.Inventory/Consumers/OrderConfirmedConsumer.cs
//
// WHAT THIS FILE IS
//   The handler for the OrderConfirmed EVENT: the saga finished successfully (stock reserved AND
//   wallet charged), so the held units now really leave the warehouse.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Unlike ReserveStock / ReleaseStock (commands SENT to our queue), OrderConfirmed is an EVENT that the
//   Orders service PUBLISHES.  MassTransit binds our queue (minishop.inventory) to the OrderConfirmed
//   exchange because this consumer is registered, so a copy of the event lands in our queue.  The Web
//   receives its own copy in its own queue.
//
//     Orders (saga) --OrderConfirmed (published)--> THIS CONSUMER (and the Web)
//
//   Why this step matters: a reservation only HOLDS units (Reserved).  If nobody "commits" it, those units
//   stay locked forever: Available never comes back, yet OnHand still counts goods that were sold.
//   Commit turns the hold into real consumption: OnHand -= qty and Reserved -= qty.
//
// PATTERNS: Idempotent consumer (Inbox) + state check (only a Reserved reservation can be committed),
//           Transactional Outbox for the ProductCatalogChanged notifications.
//
// Read next: Consumers/SnapshotRequestedConsumer.cs (answering a late subscriber).
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
/// When the saga confirms the order, the reservation becomes real consumption:
/// OnHand -= qty and Reserved -= qty, state Committed.  Without this step stock would stay locked forever.
/// No answer is sent back: the saga is already finished and does not wait for Inventory here.
/// </summary>
/// <param name="db">The Inventory database (business tables + Inbox + Outbox), one instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit.</param>
/// <param name="faults">Fault-injection rules (this consumer defines no fault point of its own).</param>
/// <param name="logger">Standard logger.</param>
public sealed class OrderConfirmedConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<OrderConfirmedConsumer> logger)
    : IdempotentConsumer<OrderConfirmed, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>
    /// Commits the reservation of the confirmed order.  Runs inside the base class's transaction
    /// (inbox check → this method → inbox row → SaveChanges → commit).
    /// </summary>
    /// <param name="context">The delivered OrderConfirmed event and its cancellation token.</param>
    /// <returns>A human-readable detail and the stock levels for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<OrderConfirmed> context)
    {
        // "@event" because "event" is a C# keyword; the @ prefix lets us use it as a variable name.
        var @event = context.Message;
        // Cancellation token (service shutdown).
        var ct = context.CancellationToken;

        // Find this order's reservation together with its lines (we need the quantities to commit).
        var reservation = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == @event.OrderId, ct);
        // Only an ACTIVE (Reserved) reservation can be committed.  Anything else means there is nothing to do:
        //   - Committed already: a second OrderConfirmed for the same order (a different MessageId the Inbox
        //     could not catch); committing again would subtract the goods twice.
        //   - none / Released / Rejected / Cancelled: nothing is held, so nothing can leave the warehouse.
        // We return normally (no exception) so the message is acknowledged and not retried pointlessly.
        if (reservation is null || reservation.State != ReservationState.Reserved)
            return new HandleResult($"no active reservation to commit (state: {reservation?.State.ToString() ?? "none"})", await Db.StockLevelsAsync(ct));

        // Turn every held line into real consumption.
        foreach (var line in reservation.Lines)
        {
            // Load the tracked stock row of the product.
            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            // The goods physically leave the warehouse...
            stock.OnHand -= line.Quantity;
            // ...so they no longer need to be held.  Available (OnHand - Reserved) stays the same: the units were
            // already unavailable while reserved; now they are simply gone.
            stock.Reserved -= line.Quantity;
            // Concurrency token, as everywhere the stock row changes.
            stock.Version++;
            // Tell the Web the new OnHand / Reserved numbers of this product.
            var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
            Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, @event));
        }
        // Final state: Committed.  A later ReleaseStock for this order will be refused (see ReleaseStockConsumer).
        reservation.State = ReservationState.Committed;
        reservation.UpdatedAtUtc = DateTime.UtcNow;

        // The base class adds the Inbox row and commits everything atomically.
        return new HandleResult($"reservation committed: {reservation.Lines.Sum(l => l.Quantity)} unit(s) left the warehouse", await Db.StockLevelsAsync(ct));
    }
}
