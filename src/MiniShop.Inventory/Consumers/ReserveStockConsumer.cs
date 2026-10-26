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
/// </summary>
public sealed class ReserveStockConsumer(InventoryDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ReserveStockConsumer> logger)
    : IdempotentConsumer<ReserveStock, InventoryDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override string FaultPoint => FaultPoints.InventoryReserve;

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ReserveStock> context)
    {
        var command = context.Message;
        var ct = context.CancellationToken;

        var existing = await Db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == command.OrderId, ct);
        if (existing is not null)
            return await ReAnswerAsync(existing, command, ct);

        var now = DateTime.UtcNow;
        var reservation = new Reservation { Id = Guid.NewGuid(), OrderId = command.OrderId, CreatedAtUtc = now, UpdatedAtUtc = now };
        Db.Reservations.Add(reservation);

        var rejection = Fault.Reject ? Fault.Reason : await ValidateAsync(command, ct);
        if (rejection is not null)
        {
            reservation.State = ReservationState.Rejected;
            reservation.Reason = rejection;
            Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, rejection, await Db.StockLevelsAsync(ct)), command));
            return new HandleResult($"reservation rejected: {rejection}", await Db.StockLevelsAsync(ct));
        }

        // All checks passed: hold the stock. Version++ makes a concurrent reservation of the same product fail and retry.
        foreach (var line in command.Lines)
        {
            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            stock.Reserved += line.Quantity;
            stock.Version++;
            reservation.Lines.Add(new ReservationLine { ProductId = line.ProductId, Quantity = line.Quantity });

            var product = await Db.Products.FirstAsync(p => p.Id == line.ProductId, ct);
            Db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, command));
        }
        reservation.State = ReservationState.Reserved;

        var levels = await Db.StockLevelsAsync(ct);
        Db.PublishViaOutbox(Stamp(new StockReserved(command.OrderId, levels), command));
        return new HandleResult($"reserved {command.Lines.Sum(l => l.Quantity)} unit(s) for order {command.OrderId}", levels);
    }

    /// <summary>Returns a rejection reason, or null when the order can be reserved.</summary>
    private async Task<string?> ValidateAsync(ReserveStock command, CancellationToken ct)
    {
        foreach (var line in command.Lines)
        {
            var product = await Db.Products.FirstOrDefaultAsync(p => p.Id == line.ProductId, ct);
            if (product is null || !product.IsActive)
                return $"product {line.ProductId} is not available";
            if (product.Price != line.UnitPrice)
                return $"price of {product.Name} changed ({line.UnitPrice:N0} → {product.Price:N0}); please refresh the cart";

            var stock = await Db.Stock.FirstAsync(s => s.ProductId == line.ProductId, ct);
            if (stock.Available < line.Quantity)
                return $"insufficient stock for {product.Name}: requested {line.Quantity}, available {stock.Available}";
        }
        return null;
    }

    /// <summary>The order already has a reservation record: repeat the earlier answer instead of changing anything.</summary>
    private async Task<HandleResult> ReAnswerAsync(Reservation existing, ReserveStock command, CancellationToken ct)
    {
        var levels = await Db.StockLevelsAsync(ct);
        switch (existing.State)
        {
            case ReservationState.Reserved:
            case ReservationState.Committed:
                Db.PublishViaOutbox(Stamp(new StockReserved(command.OrderId, levels), command));
                return new HandleResult($"order already reserved ({existing.State}); StockReserved re-published, stock unchanged", levels);
            case ReservationState.Cancelled:
                Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, "order was cancelled before stock could be reserved (tombstone)", levels), command));
                return new HandleResult("late ReserveStock refused: order already cancelled (tombstone)", levels);
            default:
                var reason = existing.Reason ?? $"reservation is {existing.State}";
                Db.PublishViaOutbox(Stamp(new StockReservationRejected(command.OrderId, reason, levels), command));
                return new HandleResult($"earlier answer repeated: rejected ({reason})", levels);
        }
    }

    internal static T Stamp<T>(T @event, Message cause) where T : Message => @event with
    {
        CorrelationId = cause.CorrelationId,
        CausationId = cause.MessageId,
        ScenarioId = cause.ScenarioId,
        Producer = ServiceNames.Inventory,
    };
}
