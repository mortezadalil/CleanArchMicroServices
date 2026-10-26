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
/// </summary>
public sealed class OrderConfirmedConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<OrderConfirmedConsumer> logger)
    : IdempotentConsumer<OrderConfirmed, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<OrderConfirmed> context)
    {
        var @event = context.Message;
        var ct = context.CancellationToken;

        var reservation = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == @event.OrderId, ct);
        if (reservation is null || reservation.State != ReservationState.Reserved)
            return new HandleResult($"no active reservation to commit (state: {reservation?.State.ToString() ?? "none"})", await Db.StockLevelsAsync(ct));

        foreach (var line in reservation.Lines)
        {
            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            stock.OnHand -= line.Quantity;
            stock.Reserved -= line.Quantity;
            stock.Version++;
            var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
            Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, @event));
        }
        reservation.State = ReservationState.Committed;
        reservation.UpdatedAtUtc = DateTime.UtcNow;

        return new HandleResult($"reservation committed: {reservation.Lines.Sum(l => l.Quantity)} unit(s) left the warehouse", await Db.StockLevelsAsync(ct));
    }
}
