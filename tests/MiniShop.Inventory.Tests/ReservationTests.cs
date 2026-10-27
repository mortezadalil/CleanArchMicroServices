// =====================================================================================================
// ReservationTests.cs  -  INTEGRATION tests (middle layer of the test pyramid)
// =====================================================================================================
//
// WHAT THIS FILE PROVES
//   Inventory's consumers keep the stock numbers correct even though messages are delivered
//   "at least once" (so sometimes twice), in any order, and possibly late:
//     - a reservation, its Outbox event and its Inbox row are written together (one transaction);
//     - the same message delivered twice changes nothing the second time (Inbox, S06);
//     - two DIFFERENT messages for the same order still reserve only once (business key = OrderId);
//     - rejections (no stock, wrong price) never touch the stock;
//     - a release that overtakes its reservation leaves a TOMBSTONE that refuses the late reservation;
//     - releasing is idempotent; confirming turns "reserved" into "really gone";
//     - the last unit can only be sold once (S15).
//
// INFRASTRUCTURE NEEDED
//   None.  Each test builds its own ConsumerTestHost: real consumers + real EF model on an in-memory
//   SQLite database + MassTransit in-memory transport.  See ConsumerTestHost.cs for how that works.
//
// THE NUMBERS USED BELOW
//   CatalogSeed gives every product OnHand = 10, Reserved = 0.  The mouse costs 300,000 Toman.
//   Available = OnHand - Reserved.  Reserving raises Reserved; confirming lowers BOTH OnHand and
//   Reserved (the goods have really left the warehouse).
//
// TEST SHAPE
//   Every test follows the same pattern:
//     await using var host = new ConsumerTestHost();  await host.StartAsync();     <- Arrange
//     await host.SendAsync(...);                                                  <- Act
//     await host.QueryAsync(db => ...);  Assert...                                <- Assert
//   We assert on DATABASE STATE (stock, reservations, Outbox rows, Inbox rows), because that is the
//   evidence that survives a crash and that the rest of the system will act on.
//
// Read next: tests/MiniShop.Payments.Tests/ConsumerTestHost.cs and LedgerTests.cs
// =====================================================================================================

// FirstAsync, AnyAsync, CountAsync, ToListAsync - EF Core's async LINQ operators.
using Microsoft.EntityFrameworkCore;
// The message contracts: ReserveStock, ReleaseStock, OrderConfirmed, StockReserved, OrderLine, ...
using MiniShop.Contracts;
// CatalogSeed: the fixed product ids (CatalogSeed.Mouse).
using MiniShop.Inventory.Data;
// ReservationState enum: Reserved, Rejected, Cancelled, Committed, ...
using MiniShop.Inventory.Domain;
// [Fact] and Assert.
using Xunit;

namespace MiniShop.Inventory.Tests;

public class ReservationTests
{
    // Small factories so each test reads like a sentence.
    // Mouse(qty): one order line for `qty` mice at the CORRECT catalog price (300,000), so price
    // validation passes and only quantity/stock decides the outcome.
    private static OrderLine Mouse(int qty) => new(CatalogSeed.Mouse, "Mouse", 300_000, qty);
    // Reserve(orderId, qty): the ReserveStock command exactly as the Orders saga would send it -
    // CorrelationId = OrderId (so the answer can be matched to the order) and Producer = "Orders".
    // Every call creates a NEW message with a NEW MessageId (Message.MessageId defaults to Guid.NewGuid()).
    private static ReserveStock Reserve(Guid orderId, int qty) => new(orderId, [Mouse(qty)]) { CorrelationId = orderId, Producer = "Orders" };

    // Scenario Lab: S01 (successful purchase, Inventory part).
    //
    // Arrange : a fresh host with 10 mice on hand.
    // Act     : reserve 2 mice for a new order.
    // Assert  : OnHand still 10 (nothing has left the warehouse yet), Reserved = 2, a Reservation row in
    //           state Reserved, a StockReserved row in the Outbox and the message's id in the Inbox.
    // Invariant: the business change, the outgoing event and the "I processed this message" mark are
    //           committed TOGETHER, or not at all.  That is what makes a crash at any moment safe.
    // Bug caught: forgetting to write the Outbox row (the saga would wait forever), forgetting the Inbox
    //           row (a redelivery would reserve again), or decrementing OnHand at reservation time.
    [Fact]
    public async Task Reservation_holds_stock_and_is_written_with_its_outbox_event()
    {
        // `await using`: the host is disposed (bus stopped, in-memory DB deleted) when the method ends.
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        await host.SendAsync(Reserve(orderId, 2));

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(10, stock.OnHand);
        Assert.Equal(2, stock.Reserved);
        var reservation = await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId));
        Assert.Equal(ReservationState.Reserved, reservation.State);
        // The Outbox query is filtered by Type AND CorrelationId because the catalog seed and the
        // reservation itself also write other rows (ProductCatalogChanged) into the same table.
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReserved) && o.CorrelationId == orderId)));
        Assert.True(await host.QueryAsync(db => db.Inbox.AnyAsync(i => i.CorrelationId == orderId)));
    }

    // Scenario Lab: S06 (duplicate delivery).
    //
    // Arrange : one ReserveStock command object for 1 mouse.
    // Act     : deliver the very SAME object twice (same MessageId) - exactly what happens when the
    //           outbox dispatcher publishes a row twice, or RabbitMQ redelivers after a lost ack.
    // Assert  : Reserved = 1 (not 2) and only one Reservation row exists for the order.
    // Invariant: at-least-once delivery + Inbox = effectively-once processing.
    // Bug caught: a missing or broken Inbox check (the second copy would reserve another unit).
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

    // Business idempotency (the saga may re-send a command as a NEW message, e.g. after a restart or a
    // retry; related scenarios: S06, S09).
    //
    // Arrange : one order id.
    // Act     : two DIFFERENT ReserveStock messages (two MessageIds) for the same order.  The Inbox cannot
    //           help here, because to the Inbox these are two unrelated messages.
    // Assert  : Reserved = 1, and there are TWO StockReserved rows in the Outbox: the second message got
    //           the earlier answer repeated instead of silence.
    // Invariant: one reservation per order, whatever the number of messages asking for it.  The business
    //           key (OrderId, unique index on Reservations) protects where the Inbox cannot.
    // Bug caught: relying only on the Inbox (the second message would reserve again), or swallowing the
    //           second message without an answer (a saga that lost the first answer would wait forever).
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

    // Scenario Lab: S02 (insufficient stock).
    //
    // Arrange : 10 mice on hand.
    // Act     : ask for 999.
    // Assert  : Reserved is still 0, the Reservation row is Rejected, and StockReservationRejected is in
    //           the Outbox (so the saga can reject the order).
    // Invariant: a rejected request never changes stock numbers.
    // Bug caught: reserving first and checking afterwards (Available would briefly go negative), or
    //           rejecting without telling the saga.
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

    // Arrange : the catalog says a mouse costs 300,000.
    // Act     : a ReserveStock whose line claims the mouse costs 1 (a stale cart or a tampered request).
    // Assert  : StockReservationRejected is written to the Outbox.
    // Invariant: Inventory owns the prices; an order is only accepted at the current catalog price.
    // Bug caught: trusting the price sent by the client - someone could buy a 300,000 mouse for 1.
    [Fact]
    public async Task Price_mismatch_is_rejected()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();

        // OrderLine(productId, name, unitPrice: 1, quantity: 1) - the wrong price is the only problem.
        await host.SendAsync(new ReserveStock(orderId, [new OrderLine(CatalogSeed.Mouse, "Mouse", 1, 1)]) { CorrelationId = orderId });

        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(StockReservationRejected) && o.CorrelationId == orderId)));
    }

    // Out-of-order delivery (related scenario: S10 timeout path; the saga releases on a stock timeout).
    //
    // Arrange : nothing is reserved for the order yet.
    // Act 1   : ReleaseStock arrives FIRST (the saga timed out and compensated before ReserveStock was
    //           processed).
    // Assert 1: a Reservation row exists in state Cancelled - the "tombstone".
    // Act 2   : the original ReserveStock finally arrives.
    // Assert 2: Reserved stays 0 and StockReservationRejected is written.
    // Invariant: once an order was cancelled, no late message can lock stock for it.
    // Bug caught: treating "release with nothing to release" as a no-op.  The late ReserveStock would
    //           then hold stock forever for an order the saga already gave up on (a stock leak).
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

    // Scenario Lab: S03 (insufficient wallet → release), S12 (the saga re-sends ReleaseStock).
    //
    // Arrange : 3 mice reserved for the order.
    // Act     : two DIFFERENT ReleaseStock messages for the same order (the saga retried).
    // Assert  : Reserved back to 0, OnHand untouched at 10 (nothing was sold), and TWO StockReleased
    //           rows: each request got an answer, but stock was only given back once.
    // Invariant: compensation is idempotent - running it twice equals running it once.
    // Bug caught: Reserved -= 3 twice (Reserved = -3, i.e. phantom stock appears), or ignoring the second
    //           request silently (a saga that lost the first StockReleased would end in RequiresAttention).
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

    // Scenario Lab: S01 (successful purchase, the happy ending in Inventory).
    //
    // Arrange : 2 mice reserved for the order.
    // Act     : the OrderConfirmed EVENT arrives (published by the saga after the payment).
    // Assert  : OnHand 10 → 8, Reserved 2 → 0, Reservation state Committed.
    // Invariant: a reservation is temporary; confirmation turns it into a real decrease of stock, and the
    //           units are not counted twice (once as reserved AND once as gone).
    // Bug caught: decreasing OnHand but forgetting to clear Reserved (Available would drop by 4 instead of
    //           2), or never committing at all (stock would look reserved forever).
    [Fact]
    public async Task Confirmed_order_turns_the_reservation_into_consumption()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        var orderId = Guid.NewGuid();
        await host.SendAsync(Reserve(orderId, 2));

        // OrderConfirmed(orderId, customerId, amount).  It is an event, but SendAsync delivers it straight
        // to Inventory's queue - for this test only the consumer's reaction matters, not how it got there.
        await host.SendAsync(new OrderConfirmed(orderId, "c1", 600_000) { CorrelationId = orderId });

        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.Equal(8, stock.OnHand);
        Assert.Equal(0, stock.Reserved);
        Assert.Equal(ReservationState.Committed, (await host.QueryAsync(db => db.Reservations.FirstAsync(r => r.OrderId == orderId))).State);
    }

    // Scenario Lab: S15 (two orders race for the last unit).
    //
    // Arrange : force OnHand of the mouse down to 1 directly in the database.
    // Act     : two orders each ask for that 1 unit.
    // Assert  : exactly one reservation is Reserved and exactly one is Rejected, and Available never
    //           goes below zero.
    // Invariant: never sell more than you have (no overselling).
    // Bug caught: a missing stock check, or a check that does not see the first reservation, so both
    //           orders win and Available becomes -1.
    //
    // HONEST NOTE FOR STUDENTS: SendAsync waits for each message to be consumed, so in this test the two
    // reservations are processed one AFTER the other - it proves the business rule, not the truly
    // simultaneous case.  The simultaneous case is protected by Stock.Version, an optimistic-concurrency
    // token: if two transactions read the same Version, the second SaveChanges fails, MassTransit retries
    // the message, and the retry sees the updated stock.  S15 in the Scenario Lab exercises that on the
    // real stack.
    [Fact]
    public async Task Two_orders_for_the_last_unit_only_one_wins()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync();
        // QueryAsync is used here to WRITE: it just hands us a DbContext in a fresh scope.  The lambda must
        // return something (QueryAsync<T>), so it returns the number of saved rows, which we ignore.
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

        // We do not care WHICH order wins, only that exactly one does - so we collect both states.
        var states = await host.QueryAsync(db => db.Reservations.Where(r => r.OrderId == a || r.OrderId == b).Select(r => r.State).ToListAsync());
        // Assert.Single(collection, expected) = "exactly one element equals expected".
        Assert.Single(states, ReservationState.Reserved);
        Assert.Single(states, ReservationState.Rejected);
        var stock = await host.QueryAsync(db => db.Stock.FirstAsync(s => s.ProductId == CatalogSeed.Mouse));
        Assert.True(stock.Available >= 0);
    }
}
