using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Data;
using MiniShop.Inventory.Domain;
using Xunit;

namespace MiniShop.Inventory.Tests;

public class ReservationTests
{
    private static OrderLine Mouse(int qty) => new(CatalogSeed.Mouse, "Mouse", 300_000, qty);
    private static ReserveStock Reserve(Guid orderId, int qty) => new(orderId, [Mouse(qty)]) { CorrelationId = orderId, Producer = "Orders" };

    [Fact]
    public async Task Reservation_holds_stock_and_is_written_with_its_outbox_event()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(Reserve(orderId, 2));

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(10, stock.OnHand);
        Assert.Equal(2, stock.Reserved);
        var reservation = await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId));
        Assert.Equal(ReservationState.Reserved, reservation.State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReserved) && o.CorrelationId == orderId)));
        Assert.True(await host.QueryAsync(db => db.Inbox.AnyAsync(i => i.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Same_message_delivered_twice_reserves_once()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var command = Reserve(Guid.NewGuid(), 1);

        await host.SendAsync(command);
        await host.SendAsync(command);   // identical MessageId → Inbox duplicate

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(1, stock.Reserved);
        Assert.Equal(1, await host.QueryAsync(db => db.Reservations.CountAsync(r => r.OrderId == command.OrderId)));
    }

    [Fact]
    public async Task Different_messages_for_the_same_order_reserve_once()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(Reserve(orderId, 1));
        await host.SendAsync(Reserve(orderId, 1));   // new MessageId, same business key (OrderId)

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(1, stock.Reserved);
        // the earlier answer was repeated so the saga is not left waiting
        Assert.Equal(2, await host.QueryAsync(db => db.Outbox.CountAsync(o => o.Type == nameof(StockReserved) && o.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Insufficient_stock_is_rejected_without_touching_stock()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(Reserve(orderId, 999));

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(0, stock.Reserved);
        Assert.Equal(ReservationState.Rejected, (await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId))).State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReservationRejected) && o.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Price_mismatch_is_rejected()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(new ReserveStock(orderId, [new OrderLine(CatalogSeed.Mouse, "Mouse", 1, 1)]) { CorrelationId = orderId });

        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReservationRejected) && o.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Release_before_reserve_leaves_a_tombstone_that_refuses_the_late_reservation()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(new ReleaseStock(orderId, "timeout") { CorrelationId = orderId });
        Assert.Equal(ReservationState.Cancelled, (await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId))).State);

        await host.SendAsync(Reserve(orderId, 1));   // arrives late

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(0, stock.Reserved);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReservationRejected) && o.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Release_gives_stock_back_and_is_idempotent()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();
        await host.SendAsync(Reserve(orderId, 3));

        await host.SendAsync(new ReleaseStock(orderId, "payment failed") { CorrelationId = orderId });
        await host.SendAsync(new ReleaseStock(orderId, "retry") { CorrelationId = orderId });

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(0, stock.Reserved);
        Assert.Equal(10, stock.OnHand);
        Assert.Equal(2, await host.QueryAsync(db => db.Outbox.CountAsync(o => o.Type == nameof(StockReleased) && o.CorrelationId == orderId)));
    }

    [Fact]
    public async Task Confirmed_order_turns_the_reservation_into_consumption()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();
        await host.SendAsync(Reserve(orderId, 2));

        await host.SendAsync(new OrderConfirmed(orderId, "c1", 600_000) { CorrelationId = orderId });

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(8, stock.OnHand);
        Assert.Equal(0, stock.Reserved);
        Assert.Equal(ReservationState.Committed, (await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId))).State);
    }

    [Fact]
    public async Task Two_orders_for_the_last_unit_only_one_wins()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        await host.QueryAsync(async db =>
        {
            var s = await db.Stock.FirstAsync(x => x.ProductId == CatalogSeed.Mouse);
            s.OnHand = 1;
            return await db.SaveChangesAsync();
        });
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();

        await host.SendAsync(Reserve(a, 1));
        await host.SendAsync(Reserve(b, 1));

        var states = await host.QueryAsync(db => db.Reservations.Where(r => r.OrderId == a || r.OrderId == b).Select(r => r.State).ToListAsync());
        Assert.Single(states, ReservationState.Reserved);
        Assert.Single(states, ReservationState.Rejected);
        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.True(stock.Available >= 0);
    }
}
