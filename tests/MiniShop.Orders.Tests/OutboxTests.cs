using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Orders.Data;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;
using Xunit;

namespace MiniShop.Orders.Tests;

/// <summary>Outbox rows must round-trip: what is stored is exactly what will be published later.</summary>
public class OutboxTests
{
    [Fact]
    public async Task Outbox_row_round_trips_through_the_contract_type_registry()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<OrdersDbContext>().UseSqlite(connection).Options;
        await using var db = new OrdersDbContext(options);
        await db.Database.EnsureCreatedAsync();

        var orderId = Guid.NewGuid();
        var @event = new OrderPlaced(orderId, "c1", 500_000, [new OrderLine(Guid.NewGuid(), "Headphones", 500_000, 1)])
            { CorrelationId = orderId, Producer = ServiceNames.Orders };
        db.PublishViaOutbox(@event);
        db.SendViaOutbox(new ReserveStock(orderId, @event.Lines) { CorrelationId = orderId }, Topology.InventoryQueue);
        await db.SaveChangesAsync();

        var rows = await db.Outbox.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Null(r.PublishedAtUtc));
        Assert.Null(rows[0].DestinationQueue);                      // event → publish
        Assert.Equal(Topology.InventoryQueue, rows[1].DestinationQueue); // command → send

        var restored = (OrderPlaced)Json.Deserialize(rows[0].Payload, ContractTypes.Resolve(rows[0].Type))!;
        Assert.Equal(@event.MessageId, restored.MessageId);
        Assert.Equal(500_000, restored.Lines[0].UnitPrice);
    }
}
