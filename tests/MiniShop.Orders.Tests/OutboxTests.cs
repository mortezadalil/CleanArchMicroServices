// =====================================================================================================
// OutboxTests.cs  -  INTEGRATION test (small): real EF Core model + real SQLite, but in memory
// =====================================================================================================
//
// WHAT THIS FILE PROVES
//   The Transactional Outbox stores messages so that they can be published LATER, by a background
//   dispatcher, possibly after a crash or a broker outage.  That only works if the stored row contains
//   everything needed to rebuild the exact same message:
//     - the payload JSON can be turned back into the right C# type (via the "Type" column and
//       ContractTypes.Resolve),
//     - nothing is lost on the way (same MessageId, same numbers),
//     - events and commands are told apart (DestinationQueue null = publish, a queue name = send),
//     - a freshly written row is still "pending" (PublishedAtUtc is null).
//   If any of this broke, the dispatcher would publish broken or wrong messages, or none at all, and
//   no unit test of the saga would notice.
//
// WHY THIS IS AN INTEGRATION TEST AND NOT A UNIT TEST
//   It uses the real OrdersDbContext (real table mapping, real column types) and a real SQLite engine.
//   Only the location is fake: the database lives in RAM instead of a file.
//
// INFRASTRUCTURE NEEDED
//   None.  SQLite is an in-process library; "DataSource=:memory:" creates a database in RAM.
//
// Read next: tests/MiniShop.Inventory.Tests/ConsumerTestHost.cs
// =====================================================================================================

// SqliteConnection: the ADO.NET connection for SQLite (Microsoft.Data.Sqlite).
using Microsoft.Data.Sqlite;
// DbContextOptionsBuilder, UseSqlite, ToListAsync, EnsureCreatedAsync ...
using Microsoft.EntityFrameworkCore;
// OrderPlaced, ReserveStock, OrderLine, Topology, ServiceNames.
using MiniShop.Contracts;
// OrdersDbContext: the Orders service's real EF Core model (Orders, Saga, Inbox and Outbox tables).
using MiniShop.Orders.Data;
// Json (the one shared serializer configuration) and ContractTypes (type name → CLR type).
using MiniShop.Shared.Messaging;
// PublishViaOutbox / SendViaOutbox extension methods.
using MiniShop.Shared.Outbox;
// [Fact] and Assert.
using Xunit;

namespace MiniShop.Orders.Tests;

/// <summary>Outbox rows must round-trip: what is stored is exactly what will be published later.</summary>
public class OutboxTests
{
    // Arrange : an empty in-memory Orders database; an OrderPlaced EVENT and a ReserveStock COMMAND.
    // Act     : write both through the outbox helpers and SaveChanges; read the rows back; deserialize
    //           the event's payload the same way the OutboxDispatcher does.
    // Assert  : two pending rows, the event has no queue, the command targets Inventory, and the
    //           restored event has the same MessageId and the same price as the original.
    // Invariant: "what is stored is exactly what will be published later" (same message, same id).
    //           The same MessageId matters a lot: consumers de-duplicate on it (Inbox), so if the
    //           dispatcher publishes a row twice (S06 Duplicate delivery, S11 Crash after publish)
    //           both copies must carry the same id.
    // Bug caught: a contract type missing from ContractTypes (Resolve would throw), a serializer setting
    //           that drops properties (e.g. the Lines list), a payload stored without its envelope, or
    //           mixing up Publish and Send.
    // Related scenarios: S04 (broker outage: the row waits in the outbox), S06, S11.
    [Fact]
    public async Task Outbox_row_round_trips_through_the_contract_type_registry()
    {
        // ---- Arrange: an in-memory SQLite database --------------------------------------------------
        // An in-memory SQLite database lives only as long as the connection that created it is OPEN.
        // If EF Core opened and closed its own connection for every command (its default behaviour when
        // you give it a connection STRING), the database would be thrown away after EnsureCreated and
        // the next query would find no tables.  So we open ONE connection ourselves, keep it open for the
        // whole test, and hand that same connection object to EF Core.
        // `await using` disposes it (and with it the database) when the test method ends.
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        // Build DbContext options by hand (no dependency injection needed for such a small test).
        var options = new DbContextOptionsBuilder<OrdersDbContext>().UseSqlite(connection).Options;
        await using var db = new OrdersDbContext(options);
        // EnsureCreated builds the schema directly from the EF model (no migrations).  Perfect for tests.
        await db.Database.EnsureCreatedAsync();

        var orderId = Guid.NewGuid();
        // `@event` - the @ prefix lets us use the C# keyword "event" as a variable name.
        // The object initializer `{ CorrelationId = ..., Producer = ... }` fills the envelope fields that
        // every Message carries (see contracts/Message.cs).
        var @event = new OrderPlaced(orderId, "c1", 500_000, [new OrderLine(Guid.NewGuid(), "Headphones", 500_000, 1)])
            { CorrelationId = orderId, Producer = ServiceNames.Orders };

        // ---- Act: write through the outbox -----------------------------------------------------------
        // An EVENT: published to whoever subscribes (no destination queue).
        db.PublishViaOutbox(@event);
        // A COMMAND: sent to exactly one queue (Inventory's).
        db.SendViaOutbox(new ReserveStock(orderId, @event.Lines) { CorrelationId = orderId }, Topology.InventoryQueue);
        // Both rows are written in ONE SaveChanges = one transaction.  In production the business change
        // (the new Order row) would be in that same transaction - that is the whole point of the pattern.
        await db.SaveChangesAsync();

        // Read the rows back.  Id is an auto-increment number, so ordering by it gives insertion order.
        var rows = await db.Outbox.OrderBy(r => r.Id).ToListAsync();

        // ---- Assert -----------------------------------------------------------------------------------
        Assert.Equal(2, rows.Count);
        // Assert.All runs the inner assertion on every element: nothing has been published yet.
        Assert.All(rows, r => Assert.Null(r.PublishedAtUtc));
        Assert.Null(rows[0].DestinationQueue);                      // event → publish
        Assert.Equal(Topology.InventoryQueue, rows[1].DestinationQueue); // command → send

        // Rebuild the message exactly like the dispatcher does:
        //   1. rows[0].Type is the short type name, e.g. "OrderPlaced";
        //   2. ContractTypes.Resolve turns it into typeof(OrderPlaced);
        //   3. Json.Deserialize turns the JSON payload into an object of that type.
        // The `!` (null-forgiving operator) tells the compiler "trust me, this is not null"; the cast to
        // OrderPlaced would throw if the wrong type had come back, failing the test.
        var restored = (OrderPlaced)Json.Deserialize(rows[0].Payload, ContractTypes.Resolve(rows[0].Type))!;
        // Same identity: a re-published copy must be recognisable as a duplicate by the consumer's Inbox.
        Assert.Equal(@event.MessageId, restored.MessageId);
        // Same data, including nested collections: the price the customer saw survives the round trip.
        Assert.Equal(500_000, restored.Lines[0].UnitPrice);
    }
}
