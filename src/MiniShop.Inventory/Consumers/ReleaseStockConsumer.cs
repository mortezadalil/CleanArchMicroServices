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
public sealed class ReleaseStockConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ReleaseStockConsumer> logger)
    : IdempotentConsumer<ReleaseStock, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override string FaultPoint => FaultPoints.InventoryRelease;   // S12: Fail here → retries → error queue

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReleaseStock> context)
    {
        var command = context.Message;
        var ct = context.CancellationToken;

        var now = DateTime.UtcNow;
        var reservation = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == command.OrderId, ct);
        string detail;

        if (reservation is null)
        {
            Db.Reservations.Add(new Reservation
            {
                Id = Guid.NewGuid(), OrderId = command.OrderId, State = ReservationState.Cancelled,
                Reason = command.Reason, CreatedAtUtc = now, UpdatedAtUtc = now,
            });
            detail = "nothing was reserved; tombstone stored so a late ReserveStock will be refused";
        }
        else if (reservation.State == ReservationState.Reserved)
        {
            foreach (var line in reservation.Lines)
            {
                var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
                stock.Reserved -= line.Quantity;
                stock.Version++;
                var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
                Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, command));
            }
            reservation.State = ReservationState.Released;
            reservation.Reason = command.Reason;
            reservation.UpdatedAtUtc = now;
            detail = $"released {reservation.Lines.Sum(l => l.Quantity)} unit(s) ({command.Reason})";
        }
        else if (reservation.State == ReservationState.Committed)
        {
            Logger.LogError("ReleaseStock for order {OrderId} ignored: goods already committed", command.OrderId);
            return new HandleResult("reservation already committed (order confirmed); nothing released", await Db.StockLevelsAsync(ct));
        }
        else
        {
            detail = $"reservation already {reservation.State}; StockReleased re-published, stock unchanged";
        }

        var levels = await Db.StockLevelsAsync(ct);
        Db.PublishViaOutbox(ReserveStockConsumer.Stamp(new StockReleased(command.OrderId, command.Reason, levels), command));
        return new HandleResult(detail, levels);
    }
}
