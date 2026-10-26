using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Domain;
using Xunit;

namespace MiniShop.Payments.Tests;

public class LedgerTests
{
    private static ChargeWallet Charge(Guid orderId, long amount = 300_000) => new(orderId, "c1", amount) { CorrelationId = orderId, Producer = "Orders" };

    [Fact]
    public async Task Charge_debits_once_and_records_ledger_outbox_inbox_in_one_transaction()
    {
        await using var host = new ConsumerTestHost();
        await host.StartAsync(1_000_000);
        var orderId = Guid.NewGuid();

        await host.SendAsync(Charge(orderId));

        Assert.Equal(700_000, (await host.QueryAsync(db => db.Wallets.FirstAsync())).Balance);
        var entry = await host.QueryAsync(db => db.Ledger.SingleAsync(l => l.OrderId == orderId));
        Assert.Equal(LedgerState.Completed, entry.State);
        Assert.True(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentCompleted) && o.CorrelationId == orderId)));
        Assert.True(await host.QueryAsync(db => db.Inbox.AnyAsync(i => i.CorrelationId == orderId)));
    }

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
        Assert.False(await host.QueryAsync(db => db.Outbox.AnyAsync(o => o.Type == nameof(PaymentCancelled) && o.CorrelationId == orderId)));
    }
}
