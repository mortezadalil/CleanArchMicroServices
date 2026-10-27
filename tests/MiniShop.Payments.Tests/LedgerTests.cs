// =====================================================================================================
// LedgerTests.cs  -  INTEGRATION tests (middle layer of the test pyramid)
// =====================================================================================================
//
// WHAT THIS FILE PROVES
//   Money moves AT MOST ONCE per order, whatever the messaging layer does:
//     - a successful charge updates the wallet, writes a ledger row, an Outbox event and an Inbox row
//       in ONE transaction;
//     - duplicate deliveries (same MessageId) AND duplicate commands (new MessageId, same order) never
//       debit twice (S06);
//     - insufficient funds fail cleanly without moving money (S03);
//     - a cancel that arrives BEFORE the charge leaves a tombstone, so a late charge can never debit (S10);
//     - a cancel that arrives AFTER the charge does not pretend nothing happened: it answers
//       PaymentAlreadyCompleted so the saga confirms the order (S13).
//
//   Two independent protections are at work, and the tests target each of them:
//     1. the Inbox  (same MessageId seen before? → ignore)            - protects against redelivery;
//     2. the unique ledger key (OrderId, "Debit")                     - protects against a second,
//        different message asking for the same debit, and makes the tombstone work.
//
// INFRASTRUCTURE NEEDED
//   None.  Each test builds its own ConsumerTestHost (real consumers + real EF model on in-memory SQLite
//   + MassTransit in-memory transport).  See ConsumerTestHost.cs.
//
// THE NUMBERS USED BELOW
//   The order amount is 300,000 Toman.  Most tests start customer "c1" with 1,000,000, so after one
//   debit the balance must be exactly 700,000 - any other number means money moved twice or not at all.
//
// Read next: tests/MiniShop.E2E/ScenarioE2ETests.cs (the top of the pyramid)
// =====================================================================================================

// FirstAsync, SingleAsync, AnyAsync, CountAsync - EF Core's async LINQ operators.
using Microsoft.EntityFrameworkCore;
// Message contracts: ChargeWallet, CancelPayment, PaymentCompleted, PaymentFailed, ...
using MiniShop.Contracts;
// LedgerState enum: Completed, Failed, Cancelled.
using MiniShop.Payments.Domain;
// [Fact] and Assert.
using Xunit;

namespace MiniShop.Payments.Tests;

public class LedgerTests
{
    // Factory for the ChargeWallet command as the saga sends it: customer "c1", default amount 300,000,
    // CorrelationId = OrderId, Producer = "Orders".  Each call creates a NEW message (new MessageId).
    private static ChargeWallet Charge(Guid orderId, long amount = 300_000) => new(orderId, "c1", amount) { CorrelationId = orderId, Producer = "Orders" };

    // Scenario Lab: S01 (successful purchase, Payments part).
    //
    // Arrange : wallet with 1,000,000.
    // Act     : charge 300,000 for a new order.
    // Assert  : balance 700,000; exactly one ledger row for the order, state Completed; PaymentCompleted
    //           in the Outbox; the message id in the Inbox.
    // Invariant: the debit, its ledger record, its outgoing event and its Inbox mark commit together.
    // Bug caught: debiting without a ledger row (no audit, no duplicate protection), forgetting the
    //           Outbox row (the saga would time out although the money was taken - see S13), or a
    //           missing Inbox row.
    [Fact]
    public async Task Charge_debits_once_and_records_ledger_outbox_inbox_in_one_transaction()
    {
        // `await using`: the host and its in-memory database are disposed at the end of the test.
        await using var host = new ConsumerTestHost();
        await host.StartAsync(1_000_000);
        var orderId = Guid.NewGuid();

        await host.SendAsync(Charge(orderId));

        // There is only one wallet in this database, so FirstAsync() without a filter is fine.
        Assert.Equal(700_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        // SingleAsync THROWS if there are zero or more than one matching rows - so this line alone already
        // asserts "exactly one ledger row for this order".
        var entry = await host.QueryAsync(db => db.Ledger.SingleAsync(l => l.OrderId == orderId));
        Assert.Equal(LedgerState.Completed, entry.State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentCompleted) && o.CorrelationId == orderId)));
        Assert.True(await host.QueryAsync(db => db.Inbox.AnyAsync(i => i.CorrelationId == orderId)));
    }

    // Scenario Lab: S06 (duplicate delivery) - plus the "saga re-sent the command" case.
    //
    // Arrange : wallet with 1,000,000; one ChargeWallet object.
    // Act     : 1) deliver it; 2) deliver the SAME object again (same MessageId → the Inbox stops it);
    //           3) deliver a NEW ChargeWallet for the same order (new MessageId → the Inbox cannot help;
    //           the unique (OrderId, Debit) ledger slot and the "re-answer" logic stop it).
    // Assert  : balance 700,000 (one debit only) and exactly one ledger row for the order.
    // Invariant: one debit per order, no matter how many messages ask for it.
    // Bug caught: relying on only ONE of the two protections.  Without the Inbox, case 2 would debit
    //           again; without the business key, case 3 would.  Charging a customer twice is the most
    //           expensive bug in this whole system.
    [Fact]
    public async Task Duplicate_delivery_and_duplicate_command_never_debit_twice()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync(1_000_000);
        var orderId = Guid.NewGuid();
        var command = Charge(orderId);

        await host.SendAsync(command);          // first delivery
        await host.SendAsync(command);          // same MessageId (Inbox)
        await host.SendAsync(Charge(orderId));  // different MessageId, same order (unique ledger key)

        Assert.Equal(700_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        Assert.Equal(1, await host.QueryAsync(db => db.Ledger.CountAsync(l => l.OrderId == orderId)));
    }

    // Scenario Lab: S03 (insufficient wallet).
    //
    // Arrange : wallet with only 100,000.
    // Act     : charge 300,000.
    // Assert  : balance unchanged (100,000), a ledger row in state Failed, PaymentFailed in the Outbox.
    // Invariant: a balance never goes negative, and a refusal is recorded and announced (so the saga can
    //           release the stock, and a re-sent command gets the same "failed" answer).
    // Bug caught: debiting into a negative balance, or failing without a ledger row / without telling the
    //           saga (the order would sit in AwaitingPayment until the timeout).
    [Fact]
    public async Task Insufficient_funds_fails_without_moving_money()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync(100_000);
        var orderId = Guid.NewGuid();

        await host.SendAsync(Charge(orderId));

        Assert.Equal(100_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        Assert.Equal(LedgerState.Failed, (await host.QueryAsync(db => db.Ledger.SingleAsync(l => l.OrderId == orderId))).State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentFailed) && o.CorrelationId == orderId)));
    }

    // Scenario Lab: S10 (payment timeout → cancellation).
    //
    // Arrange : wallet with 1,000,000; no charge has been processed yet.
    // Act 1   : CancelPayment arrives first (the saga timed out waiting for the payment).
    // Assert 1: PaymentCancelled is in the Outbox - the saga may now safely release the stock.
    // Act 2   : the original ChargeWallet finally arrives (it was stuck, slow, or in a retry loop).
    // Assert 2: balance still 1,000,000; the single ledger row for the order is the Cancelled TOMBSTONE;
    //           PaymentFailed is written so whoever is listening learns the charge was refused.
    // Invariant: once Payments has said "cancelled", it must stay true forever.  The tombstone occupies
    //           the unique (OrderId, Debit) slot, so no later message can debit.
    // Bug caught: answering "cancelled" without recording anything.  The late charge would then take the
    //           money for an order the saga already rejected and whose stock was released.
    [Fact]
    public async Task Cancel_before_charge_writes_a_tombstone_so_the_late_charge_is_refused()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync(1_000_000);
        var orderId = Guid.NewGuid();

        await host.SendAsync(new CancelPayment(orderId, "timeout") { CorrelationId = orderId });
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentCancelled) && o.CorrelationId == orderId)));

        await host.SendAsync(Charge(orderId));   // arrives late

        Assert.Equal(1_000_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        Assert.Equal(LedgerState.Cancelled, (await host.QueryAsync(db => db.Ledger.SingleAsync(l => l.OrderId == orderId))).State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentFailed) && o.CorrelationId == orderId)));
    }

    // Scenario Lab: S13 (late PaymentCompleted after timeout).
    //
    // Arrange : wallet with 1,000,000.
    // Act     : the charge succeeds FIRST, then a CancelPayment arrives (the saga timed out because the
    //           PaymentCompleted answer was slow, not because the payment did not happen).
    // Assert  : balance stays 700,000 (the cancel did not refund or undo anything), PaymentAlreadyCompleted
    //           is written, and NO PaymentCancelled is written.
    // Invariant: Payments tells the truth about money.  The saga gets a definitive answer and confirms the
    //           order instead of rejecting it (see OrderSagaTests.Late_payment_after_timeout_confirms_the_order).
    // Bug caught: answering PaymentCancelled although the debit happened - the saga would release the stock
    //           and reject the order while the customer's money is gone.
    [Fact]
    public async Task Cancel_after_charge_answers_PaymentAlreadyCompleted_and_keeps_the_money()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync(1_000_000);
        var orderId = Guid.NewGuid();

        await host.SendAsync(Charge(orderId));
        await host.SendAsync(new CancelPayment(orderId, "timeout") { CorrelationId = orderId });

        Assert.Equal(700_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentAlreadyCompleted) && o.CorrelationId == orderId)));
        // Assert.False: proving the WRONG answer is absent matters as much as proving the right one exists.
        Assert.False(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentCancelled) && o.CorrelationId == orderId)));
    }
}
