using MiniShop.Contracts;
using MiniShop.Orders.Domain;
using MiniShop.Orders.Saga;
using Xunit;

namespace MiniShop.Orders.Tests;

/// <summary>
/// The saga is a pure function, so every transition of the state chart can be tested without a
/// database or a broker.  Each test reads: given state X and message Y → expect state Z and commands.
/// </summary>
public class OrderSagaTests
{
    private static readonly SagaOptions Options = new() { MaxCompensationAttempts = 3 };
    private static readonly Order Order = new() { Id = Guid.NewGuid(), CustomerId = "c1", Amount = 300_000, State = OrderState.Pending };

    private static OrderSagaState Saga(string state, int attempts = 0) =>
        new() { CorrelationId = Order.Id, CurrentState = state, CompensationAttempts = attempts, FailureReason = "payment failed" };

    private static T Single<T>(SagaDecision d) where T : Message => Assert.IsType<T>(Assert.Single(d.Outgoing).Message);

    [Fact]
    public void OrderPlaced_starts_by_reserving_stock()
    {
        var lines = new[] { new OrderLine(Guid.NewGuid(), "Mouse", 300_000, 1) };
        var d = OrderSaga.Decide(Saga(SagaStates.Started), Order, new OrderPlaced(Order.Id, "c1", 300_000, lines), Options);

        Assert.Equal(SagaStates.WaitingForStock, d.SagaState);
        Assert.Equal(OrderState.Reserving, d.OrderState);
        var command = Single<ReserveStock>(d);
        Assert.Equal(Topology.InventoryQueue, d.Outgoing[0].DestinationQueue);
        Assert.Equal(Order.Id, command.CorrelationId);
        Assert.NotNull(d.WaitFor);
    }

    [Fact]
    public void StockReserved_charges_the_wallet()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new StockReserved(Order.Id, []), Options);
        Assert.Equal(SagaStates.WaitingForPayment, d.SagaState);
        Assert.Equal(OrderState.AwaitingPayment, d.OrderState);
        Assert.Equal(300_000, Single<ChargeWallet>(d).Amount);
    }

    [Fact]
    public void PaymentCompleted_confirms_the_order()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPayment), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.Equal(SagaStates.Completed, d.SagaState);
        Assert.Equal(OrderState.Confirmed, d.OrderState);
        Single<OrderConfirmed>(d);
        Assert.Null(d.Outgoing[0].DestinationQueue);   // event, not command
    }

    [Fact]
    public void StockRejected_rejects_without_compensation()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new StockReservationRejected(Order.Id, "no stock", []), Options);
        Assert.Equal(SagaStates.Rejected, d.SagaState);
        Assert.Equal("no stock", Single<OrderRejected>(d).Reason);
    }

    [Fact]
    public void PaymentFailed_releases_stock_and_only_StockReleased_rejects()
    {
        var afterFailure = OrderSaga.Decide(Saga(SagaStates.WaitingForPayment), Order, new PaymentFailed(Order.Id, "c1", 300_000, "insufficient", 0), Options);
        Assert.Equal(SagaStates.WaitingForRelease, afterFailure.SagaState);
        Assert.Equal(OrderState.Compensating, afterFailure.OrderState);     // NOT Rejected yet
        Single<ReleaseStock>(afterFailure);

        var afterRelease = OrderSaga.Decide(Saga(SagaStates.WaitingForRelease), Order, new StockReleased(Order.Id, "x", []), Options);
        Assert.Equal(SagaStates.Rejected, afterRelease.SagaState);
        Assert.Equal(OrderState.Rejected, afterRelease.OrderState);
    }

    [Fact]
    public void Payment_timeout_asks_for_cancellation_instead_of_assuming_not_paid()
    {
        var d = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForPayment), Order, Options);
        Assert.Equal(SagaStates.WaitingForPaymentCancellation, d.SagaState);
        Single<CancelPayment>(d);
        Assert.Equal(1, d.CompensationAttempts);
    }

    [Fact]
    public void Late_payment_after_timeout_confirms_the_order()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPaymentCancellation), Order, new PaymentAlreadyCompleted(Order.Id, 300_000, 0), Options);
        Assert.Equal(SagaStates.Completed, d.SagaState);
        Assert.Equal(OrderState.Confirmed, d.OrderState);
        Assert.Contains("LATE", d.Detail);
    }

    [Fact]
    public void Cancelled_payment_releases_stock()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPaymentCancellation), Order, new PaymentCancelled(Order.Id, "timeout"), Options);
        Assert.Equal(SagaStates.WaitingForRelease, d.SagaState);
        Single<ReleaseStock>(d);
    }

    [Fact]
    public void Unconfirmed_release_is_retried_then_becomes_RequiresAttention()
    {
        var retry = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForRelease, attempts: 1), Order, Options);
        Assert.Equal(SagaStates.WaitingForRelease, retry.SagaState);
        Assert.Equal(2, retry.CompensationAttempts);
        Single<ReleaseStock>(retry);

        var giveUp = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForRelease, attempts: 3), Order, Options);
        Assert.Equal(SagaStates.RequiresAttention, giveUp.SagaState);
        Assert.Equal(OrderState.RequiresAttention, giveUp.OrderState);
        Single<OrderRequiresAttention>(giveUp);
    }

    [Fact]
    public void Manual_retry_from_RequiresAttention_resends_release_and_StockReleased_rejects()
    {
        var retry = OrderSaga.RetryCompensation(Saga(SagaStates.RequiresAttention), Order, Options);
        Assert.Equal(SagaStates.WaitingForRelease, retry.SagaState);
        Single<ReleaseStock>(retry);

        var recovered = OrderSaga.Decide(Saga(SagaStates.RequiresAttention), Order, new StockReleased(Order.Id, "x", []), Options);
        Assert.Equal(SagaStates.Rejected, recovered.SagaState);
    }

    [Theory]
    [InlineData(SagaStates.Completed)]
    [InlineData(SagaStates.Rejected)]
    public void Messages_in_terminal_states_are_ignored(string terminal)
    {
        var d = OrderSaga.Decide(Saga(terminal), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.True(d.Ignored);
        Assert.Empty(d.Outgoing);
    }

    [Fact]
    public void Unexpected_message_is_ignored_not_crashed()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.True(d.Ignored);
    }
}
