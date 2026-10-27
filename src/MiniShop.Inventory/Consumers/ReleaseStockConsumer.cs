// ==================================================================================================
// FILE: src/MiniShop.Inventory/Consumers/ReleaseStockConsumer.cs
//
// WHAT THIS FILE IS
//   The message handler for the ReleaseStock COMMAND: "give back whatever you hold for order X".
//
// WHERE IT SITS IN THE ARCHITECTURE
//   This is a COMPENSATION step of the order saga.  A saga cannot roll back a distributed
//   transaction (there is none), so when a later step fails (payment refused S03, payment timed out S10)
//   the saga "undoes" the earlier step by sending a compensating command:
//     Orders (saga) --ReleaseStock (sent to minishop.inventory)--> THIS CONSUMER
//     THIS CONSUMER --StockReleased (published)--> Orders saga, which can then mark the order Rejected.
//
// PATTERNS
//   - Idempotent consumer (Inbox, inherited from IdempotentConsumer) + idempotency on OrderId.
//   - Idempotent compensation: releasing twice must not give the units back twice, and the saga must
//     always receive a StockReleased so it can finish (it may be retrying because it lost the first one).
//   - Tombstone: if no reservation exists yet, we write a Cancelled row so a ReserveStock that is still
//     on its way (and arrives later) is refused instead of locking stock forever.
//   - Fault point "Inventory.ReleaseStock": S12 makes this consumer fail permanently, so the saga ends
//     in RequiresAttention and a human must retry.
//
// Read next: Consumers/OrderConfirmedConsumer.cs (the happy ending: committing a reservation).
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
/// Compensation: give a reservation back.  Idempotent on OrderId:
///   Reserved           → Reserved -= qty, state Released, StockReleased
///   Released/Rejected/Cancelled → nothing to do, StockReleased re-published (the saga may be retrying)
///   no reservation yet → write a Cancelled TOMBSTONE so a late ReserveStock is refused, then StockReleased
///   Committed          → cannot release goods that were already shipped; answer with a clear reason
/// </summary>
/// <param name="db">The Inventory database (business tables + Inbox + Outbox), one instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit.</param>
/// <param name="faults">Fault-injection rules (Development only).</param>
/// <param name="logger">Standard logger.</param>
public sealed class ReleaseStockConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ReleaseStockConsumer> logger)
    : IdempotentConsumer<ReleaseStock, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>
    /// Fault-injection point "Inventory.ReleaseStock", applied by the base class before the transaction.
    /// S12: Fail here → retries → error queue; the saga's compensation never gets confirmed.
    /// </summary>
    protected override string FaultPoint => FaultPoints.InventoryRelease;   // S12: Fail here → retries → error queue

    /// <summary>
    /// The business logic of "release the stock held for an order".  Runs inside the base class's
    /// transaction; it never calls SaveChanges itself.
    /// </summary>
    /// <param name="context">The delivered ReleaseStock command (OrderId + Reason) and its cancellation token.</param>
    /// <returns>A human-readable detail and the stock levels for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReleaseStock> context)
    {
        // The command: which order, and why it is being released (e.g. "payment failed: insufficient funds").
        var command = context.Message;
        // Cancellation token (service shutdown).
        var ct = context.CancellationToken;

        // One timestamp for every row written by this message.
        var now = DateTime.UtcNow;
        // Find what we decided earlier for this order (if anything), with its lines so we know how much to give back.
        var reservation = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == command.OrderId, ct);
        // Text for the Developer Console, filled by the branch that runs.
        string detail;

        if (reservation is null)
        {
            // CASE 1: nothing reserved yet.  This happens when the saga gives up (timeout) while its ReserveStock
            // is still delayed somewhere (in a queue, in a retry).  Nothing to give back, BUT we must make sure
            // that late ReserveStock cannot reserve anymore, so we write a TOMBSTONE: a reservation row in state
            // Cancelled.  The unique index on OrderId means the late ReserveStock will find this row and take
            // the ReAnswer path ("refused: tombstone") instead of locking stock for a dead order.
            Db.Reservations.Add(new Reservation
            {
                Id = Guid.NewGuid(), OrderId = command.OrderId, State = ReservationState.Cancelled,
                Reason = command.Reason, CreatedAtUtc = now, UpdatedAtUtc = now,
            });
            detail = "nothing was reserved; tombstone stored so a late ReserveStock will be refused";
        }
        else if (reservation.State == ReservationState.Reserved)
        {
            // CASE 2: the normal compensation.  Stock is held → give every line back.
            foreach (var line in reservation.Lines)
            {
                // Load the tracked stock row of this product.
                var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
                // Un-hold the units: OnHand never changed (the goods never left), only the promise is removed.
                stock.Reserved -= line.Quantity;
                // Concurrency token: if another message changed this row meanwhile, SaveChanges fails and we retry.
                stock.Version++;
                // Let the Web's product card show the units as available again.
                var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
                Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, command));
            }
            // Final state: Released.  A second ReleaseStock will now hit the "else" branch below and change nothing,
            // which is exactly why releasing twice never gives the units back twice.
            reservation.State = ReservationState.Released;
            reservation.Reason = command.Reason;
            reservation.UpdatedAtUtc = now;
            detail = $"released {reservation.Lines.Sum(l => l.Quantity)} unit(s) ({command.Reason})";
        }
        else if (reservation.State == ReservationState.Committed)
        {
            // CASE 3: the order was already confirmed and the goods have left the warehouse (OnHand was reduced).
            // Releasing now would invent stock out of thin air.  This should never happen in a correct saga
            // (it only confirms after payment), so it is logged as an ERROR for a human to investigate.
            Logger.LogError("ReleaseStock for order {OrderId} ignored: goods already committed", command.OrderId);
            // We return WITHOUT publishing StockReleased: claiming "released" would be a lie.
            return new HandleResult("reservation already committed (order confirmed); nothing released", await Db.StockLevelsAsync(ct));
        }
        else
        {
            // CASE 4: Released, Rejected or Cancelled: nothing is held, so nothing to do.  We still answer below,
            // because this is probably the saga retrying a compensation whose first answer it never received.
            detail = $"reservation already {reservation.State}; StockReleased re-published, stock unchanged";
        }

        // Stock levels after the change (or unchanged), included as evidence in the answer.
        var levels = await Db.StockLevelsAsync(ct);
        // The answer the saga waits for, in every case except Committed: "nothing is held for this order anymore".
        // Re-publishing it is safe because the saga handles it idempotently.
        Db.PublishViaOutbox(ReserveStockConsumer.Stamp(new StockReleased(command.OrderId, command.Reason, levels), command));
        // The base class now stores the Inbox row, saves and commits all of the above atomically.
        return new HandleResult(detail, levels);
    }
}
