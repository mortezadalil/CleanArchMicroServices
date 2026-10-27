// =====================================================================================================
// OrderSagaTests.cs  -  UNIT tests (the bottom, widest layer of the test pyramid)
// =====================================================================================================
//
// WHAT THIS FILE PROVES
//   Every important transition of the order saga's state chart does what the design says:
//     Started --OrderPlaced--> WaitingForStock --StockReserved--> WaitingForPayment --PaymentCompleted--> Completed
//   plus every failure / timeout / compensation branch (release stock, cancel payment, RequiresAttention).
//
// WHY NO DATABASE AND NO BROKER ARE NEEDED
//   OrderSaga (src/MiniShop.Orders/Saga/OrderSaga.cs) is written as a PURE FUNCTION:
//       (current saga state, the order, incoming message, options)  ->  SagaDecision
//   It does not read a database, does not send anything on a bus and does not look at the clock to
//   decide.  It only RETURNS a decision object ("go to state X, set the order to Y, send these
//   messages").  Someone else (SagaRunner) applies that decision inside a database transaction.
//   Because of that design choice we can call OrderSaga.Decide(...) like any normal C# method and
//   inspect the result.  No setup, no cleanup, no timing issues, each test runs in microseconds.
//   This is the main payoff of separating "deciding" from "doing".
//
// HOW TO READ EACH TEST
//   given saga state X  +  message (or timeout) Y   ->   expect saga state Z, order state W, and the
//   outgoing command/event(s).
//
// INFRASTRUCTURE NEEDED
//   None.  Run with:  dotnet test tests/MiniShop.Orders.Tests
//
// Read next: OutboxTests.cs (in this folder), then tests/MiniShop.Inventory.Tests/ConsumerTestHost.cs
// =====================================================================================================

// Message contracts: OrderPlaced, StockReserved, ChargeWallet, ReleaseStock, Topology, ...
using MiniShop.Contracts;
// The Order aggregate and its OrderState enum (Pending, Reserving, AwaitingPayment, Confirmed, ...).
using MiniShop.Orders.Domain;
// The code under test: OrderSaga, SagaDecision, SagaStates, OrderSagaState, SagaOptions.
using MiniShop.Orders.Saga;
// xUnit: [Fact], [Theory], [InlineData] and the Assert class.
using Xunit;

namespace MiniShop.Orders.Tests;

/// <summary>
/// The saga is a pure function, so every transition of the state chart can be tested without a
/// database or a broker.  Each test reads: given state X and message Y → expect state Z and commands.
/// </summary>
/// <remarks>
/// xUnit creates a NEW instance of this class for every single test method, so tests cannot leak
/// state into each other through instance fields.  The fields below are static and read-only, and the
/// saga never mutates its inputs, so sharing them between tests is safe.
/// </remarks>
public class OrderSagaTests
{
    // Saga configuration used by every test.  Only MaxCompensationAttempts matters here (it drives the
    // "retry 3 times, then RequiresAttention" rule).  The timeout seconds keep their defaults; the
    // tests never wait for them, because a timeout is simulated by calling OrderSaga.OnTimeout directly.
    private static readonly SagaOptions Options = new() { MaxCompensationAttempts = 3 };

    // One sample order: customer "c1", amount 300,000 Toman (= 1 mouse in the demo catalog).
    // The saga reads Order.Id (to stamp CorrelationId), CustomerId and Amount (to build ChargeWallet).
    private static readonly Order Order = new() { Id = Guid.NewGuid(), CustomerId = "c1", Amount = 300_000, State = OrderState.Pending };

    // Helper that builds the persisted saga row "as if" the saga were currently in `state`.
    // This is the "Arrange" step of almost every test: we jump straight to the state we want to test,
    // instead of replaying all earlier messages.  `attempts` lets the compensation tests start with
    // a number of earlier attempts already made.  FailureReason is pre-filled because some transitions
    // (e.g. StockReleased → Rejected) copy it into the OrderRejected event.
    private static OrderSagaState Saga(string state, int attempts = 0) =>
        new() { CorrelationId = Order.Id, CurrentState = state, CompensationAttempts = attempts, FailureReason = "payment failed" };

    // Helper assertion: "the decision sends EXACTLY ONE outgoing message, and it is of type T".
    //   Assert.Single(list)        fails unless the list has exactly one element, and returns that element.
    //   Assert.IsType<T>(object)   fails unless the object is exactly of type T, and returns it already cast.
    // So in one line we check "one message, of the right type" and get it back typed, ready for more
    // asserts.  Catches bugs like "the saga sent the command twice" or "sent the wrong command".
    private static T Single<T>(SagaDecision d) where T : Message => Assert.IsType<T>(Assert.Single(d.Outgoing).Message);

    // -------------------------------------------------------------------------------------------------
    // HAPPY PATH (Scenario Lab S01 - Successful purchase)
    // -------------------------------------------------------------------------------------------------

    // [Fact] marks a test method that takes no parameters and is run exactly once.
    //
    // Arrange : a new saga (state Started) and an OrderPlaced event with one line.
    // Act     : ask the saga what to do with OrderPlaced.
    // Assert  : it moves to WaitingForStock, the order becomes Reserving, it sends ONE ReserveStock
    //           command to the Inventory queue, correlated with the order, and it starts a timer.
    // Invariant: an order always reserves stock FIRST (before any money moves).
    // Bug caught: charging the wallet before stock is known; sending the command to the wrong queue
    //           (it would sit there unread); forgetting the CorrelationId (Inventory's answer could
    //           not be matched to this order); forgetting the deadline (a lost answer would leave the
    //           order stuck in Reserving forever).
    [Fact]
    public void OrderPlaced_starts_by_reserving_stock()
    {
        // Arrange: one order line - product id, display name, unit price, quantity.
        var lines = new[] { new OrderLine(Guid.NewGuid(), "Mouse", 300_000, 1) };
        // Act: the whole "system under test" is this one pure function call.
        var d = OrderSaga.Decide(Saga(SagaStates.Started), Order, new OrderPlaced(Order.Id, "c1", 300_000, lines), Options);

        // Assert: new saga state and new order state.  Assert.Equal(expected, actual) - expected first!
        Assert.Equal(SagaStates.WaitingForStock, d.SagaState);
        Assert.Equal(OrderState.Reserving, d.OrderState);
        // Exactly one outgoing message, and it is a ReserveStock command.
        var command = Single<ReserveStock>(d);
        // A COMMAND is SENT to one specific queue (point-to-point), here Inventory's queue.
        Assert.Equal(Topology.InventoryQueue, d.Outgoing[0].DestinationQueue);
        // CorrelationId = OrderId on every message of this order, so replies find their way back.
        Assert.Equal(Order.Id, command.CorrelationId);
        // WaitFor = how long to wait for Inventory's answer before the timeout worker steps in.
        Assert.NotNull(d.WaitFor);
    }

    // Arrange : saga waiting for stock.
    // Act     : Inventory answers StockReserved.
    // Assert  : saga waits for payment, order is AwaitingPayment, ONE ChargeWallet for the order amount.
    // Invariant: money is only requested AFTER the goods are held, and for exactly the order amount.
    // Bug caught: charging a wrong amount (e.g. a line price instead of the order total), or skipping
    //           the payment step entirely.
    [Fact]
    public void StockReserved_charges_the_wallet()
    {
        // `[]` is a C# collection expression: an empty list of stock levels (not relevant here).
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new StockReserved(Order.Id, []), Options);
        Assert.Equal(SagaStates.WaitingForPayment, d.SagaState);
        Assert.Equal(OrderState.AwaitingPayment, d.OrderState);
        // Single<ChargeWallet> returns the typed command, so we can check its Amount directly.
        Assert.Equal(300_000, Single<ChargeWallet>(d).Amount);
    }

    // Arrange : saga waiting for payment.
    // Act     : Payments answers PaymentCompleted.
    // Assert  : saga Completed, order Confirmed, ONE OrderConfirmed message that is PUBLISHED (event).
    // Invariant: a successful payment always ends in a confirmed order, announced to everybody.
    // Bug caught: sending OrderConfirmed to a single queue instead of publishing it - then Inventory
    //           (which turns the reservation into consumption) or the Web read model would never hear it.
    [Fact]
    public void PaymentCompleted_confirms_the_order()
    {
        // PaymentCompleted(orderId, customerId, amount, new balance)
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPayment), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.Equal(SagaStates.Completed, d.SagaState);
        Assert.Equal(OrderState.Confirmed, d.OrderState);
        // We only need the type check here, so the returned value is ignored.
        Single<OrderConfirmed>(d);
        // DestinationQueue == null means "publish": every interested subscriber gets a copy.
        Assert.Null(d.Outgoing[0].DestinationQueue);   // event, not command
    }

    // -------------------------------------------------------------------------------------------------
    // REJECTIONS AND COMPENSATION (Scenario Lab S02 Insufficient stock, S03 Insufficient wallet)
    // -------------------------------------------------------------------------------------------------

    // Arrange : saga waiting for stock.
    // Act     : Inventory answers StockReservationRejected("no stock").
    // Assert  : saga goes straight to Rejected and publishes OrderRejected carrying Inventory's reason.
    // Invariant: if nothing was reserved, there is nothing to compensate - reject immediately.  (S02)
    // Bug caught: sending a pointless ReleaseStock, or losing the reason the customer should see.
    [Fact]
    public void StockRejected_rejects_without_compensation()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new StockReservationRejected(Order.Id, "no stock", []), Options);
        Assert.Equal(SagaStates.Rejected, d.SagaState);
        // Single<OrderRejected> also proves no ReleaseStock was sent (there is exactly one message).
        Assert.Equal("no stock", Single<OrderRejected>(d).Reason);
    }

    // Two steps in one test, because the second step only makes sense after the first.
    //
    // Step 1 Arrange/Act: saga waiting for payment receives PaymentFailed ("insufficient").
    //        Assert: saga waits for the release, order is Compensating (NOT Rejected yet), ONE ReleaseStock.
    // Step 2 Arrange/Act: saga waiting for release receives StockReleased.
    //        Assert: NOW the saga and the order are Rejected.
    // Invariant: an order is only shown as "Rejected" once the compensation is CONFIRMED.  Until then
    //           the stock may still be held, and saying "Rejected" would be a lie.  (S03)
    // Bug caught: rejecting the order immediately on PaymentFailed - if ReleaseStock then got lost,
    //           the stock would stay reserved forever while the UI claims everything is cleaned up.
    [Fact]
    public void PaymentFailed_releases_stock_and_only_StockReleased_rejects()
    {
        // PaymentFailed(orderId, customerId, amount, reason, balance)
        var afterFailure = OrderSaga.Decide(Saga(SagaStates.WaitingForPayment), Order, new PaymentFailed(Order.Id, "c1", 300_000, "insufficient", 0), Options);
        Assert.Equal(SagaStates.WaitingForRelease, afterFailure.SagaState);
        Assert.Equal(OrderState.Compensating, afterFailure.OrderState);     // NOT Rejected yet
        Single<ReleaseStock>(afterFailure);

        // Second step: we build a fresh saga row in WaitingForRelease (the pure function does not keep
        // state between calls - in production SagaRunner would have persisted afterFailure.SagaState).
        var afterRelease = OrderSaga.Decide(Saga(SagaStates.WaitingForRelease), Order, new StockReleased(Order.Id, "x", []), Options);
        Assert.Equal(SagaStates.Rejected, afterRelease.SagaState);
        Assert.Equal(OrderState.Rejected, afterRelease.OrderState);
    }

    // -------------------------------------------------------------------------------------------------
    // TIMEOUTS (Scenario Lab S10 Payment timeout, S13 Late payment)
    // -------------------------------------------------------------------------------------------------

    // Arrange : saga waiting for payment, no answer arrived.
    // Act     : simulate the deadline passing by calling OrderSaga.OnTimeout directly (no real waiting:
    //           in production SagaTimeoutWorker calls this same method when DeadlineUtc is in the past).
    // Assert  : saga goes to WaitingForPaymentCancellation and sends ONE CancelPayment; attempt counter = 1.
    // Invariant: "no answer" does NOT mean "not paid".  The debit may have happened and only the
    //           answer is slow.  The saga must ask Payments for a definitive answer first.  (S10)
    // Bug caught: releasing the stock and rejecting the order on timeout while the money was actually
    //           taken - the customer pays and gets nothing.
    [Fact]
    public void Payment_timeout_asks_for_cancellation_instead_of_assuming_not_paid()
    {
        var d = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForPayment), Order, Options);
        Assert.Equal(SagaStates.WaitingForPaymentCancellation, d.SagaState);
        Single<CancelPayment>(d);
        // This is the first compensation attempt; the counter drives the retry/give-up rule later.
        Assert.Equal(1, d.CompensationAttempts);
    }

    // Arrange : saga already asked Payments to cancel (after a timeout).
    // Act     : Payments answers PaymentAlreadyCompleted - the debit HAD happened, it was just late.
    // Assert  : the order is Confirmed (not rejected) and the decision's Detail mentions "LATE".
    // Invariant: if the money was taken, the customer gets the goods.  Reconcile, don't reject.  (S13)
    // Bug caught: ignoring the late answer (money kept, order rejected) or crashing on an "unexpected"
    //           message in this state.
    [Fact]
    public void Late_payment_after_timeout_confirms_the_order()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPaymentCancellation), Order, new PaymentAlreadyCompleted(Order.Id, 300_000, 0), Options);
        Assert.Equal(SagaStates.Completed, d.SagaState);
        Assert.Equal(OrderState.Confirmed, d.OrderState);
        // Assert.Contains(substring, text): Detail is the human-readable explanation shown in the
        // Developer Console; it must make the late reconciliation visible to the student.
        Assert.Contains("LATE", d.Detail);
    }

    // Arrange : saga already asked Payments to cancel.
    // Act     : Payments answers PaymentCancelled - nothing was debited, and now nothing ever can be.
    // Assert  : it is now safe to compensate: saga waits for release and sends ONE ReleaseStock.  (S10)
    // Bug caught: going straight to Rejected and forgetting to give the reserved stock back.
    [Fact]
    public void Cancelled_payment_releases_stock()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForPaymentCancellation), Order, new PaymentCancelled(Order.Id, "timeout"), Options);
        Assert.Equal(SagaStates.WaitingForRelease, d.SagaState);
        Single<ReleaseStock>(d);
    }

    // -------------------------------------------------------------------------------------------------
    // COMPENSATION THAT NEVER GETS CONFIRMED (Scenario Lab S12 Compensation keeps failing)
    // -------------------------------------------------------------------------------------------------

    // Two cases in one test.
    //
    // Case 1 Arrange: waiting for release, 1 attempt made so far (< max 3).  Act: timeout.
    //        Assert: stays in WaitingForRelease, counter goes to 2, ReleaseStock is SENT AGAIN.
    // Case 2 Arrange: waiting for release, 3 attempts made (= max).  Act: timeout.
    //        Assert: saga and order go to RequiresAttention and publish OrderRequiresAttention.
    // Invariant: a compensation is retried a bounded number of times, then a HUMAN is told.  The order
    //           is never silently marked Rejected while the stock might still be held.  (S12)
    // Bug caught: retrying forever (infinite message loop), giving up after the first try, or pretending
    //           the order was rejected cleanly when it was not.
    [Fact]
    public void Unconfirmed_release_is_retried_then_becomes_RequiresAttention()
    {
        // `attempts: 1` is a C# named argument - it makes clear which optional parameter we are setting.
        var retry = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForRelease, attempts: 1), Order, Options);
        Assert.Equal(SagaStates.WaitingForRelease, retry.SagaState);
        Assert.Equal(2, retry.CompensationAttempts);
        Single<ReleaseStock>(retry);

        var giveUp = OrderSaga.OnTimeout(Saga(SagaStates.WaitingForRelease, attempts: 3), Order, Options);
        Assert.Equal(SagaStates.RequiresAttention, giveUp.SagaState);
        Assert.Equal(OrderState.RequiresAttention, giveUp.OrderState);
        Single<OrderRequiresAttention>(giveUp);
    }

    // Two steps:
    //   1. An operator presses "retry compensation" (OrderSaga.RetryCompensation) on a saga in
    //      RequiresAttention → saga goes back to WaitingForRelease and re-sends ONE ReleaseStock.
    //   2. If a StockReleased arrives while the saga is still in RequiresAttention (e.g. the stuck
    //      ReleaseStock was finally processed) → the order becomes Rejected normally.
    // Invariant: RequiresAttention is NOT a dead end; the order can still be brought to a clean end.  (S12)
    // Bug caught: treating RequiresAttention as terminal and ignoring the late confirmation, which would
    //           leave the order flagged forever even though the stock was released.
    [Fact]
    public void Manual_retry_from_RequiresAttention_resends_release_and_StockReleased_rejects()
    {
        var retry = OrderSaga.RetryCompensation(Saga(SagaStates.RequiresAttention), Order, Options);
        Assert.Equal(SagaStates.WaitingForRelease, retry.SagaState);
        Single<ReleaseStock>(retry);

        var recovered = OrderSaga.Decide(Saga(SagaStates.RequiresAttention), Order, new StockReleased(Order.Id, "x", []), Options);
        Assert.Equal(SagaStates.Rejected, recovered.SagaState);
    }

    // -------------------------------------------------------------------------------------------------
    // ROBUSTNESS: late, duplicate and out-of-order messages (Scenario Lab S06 Duplicate delivery, S13)
    // -------------------------------------------------------------------------------------------------

    // [Theory] = ONE test method run SEVERAL times with different inputs.
    // Each [InlineData(...)] line supplies the arguments for one run, so this method runs twice:
    // once with terminal = "Completed" and once with terminal = "Rejected".  The test runner shows
    // them as two separate test cases, so you can see which input failed.
    //
    // Arrange : a saga that already finished (Completed or Rejected).
    // Act     : a PaymentCompleted arrives anyway (a duplicate delivery, or a very late message).
    // Assert  : the decision is marked Ignored and sends nothing.
    // Invariant: once an order is finished, its outcome never changes again.
    // Bug caught: a duplicate PaymentCompleted re-confirming the order and publishing OrderConfirmed
    //           twice (Inventory would try to commit the stock twice), or a late message resurrecting
    //           a Rejected order.
    [Theory]
    [InlineData(SagaStates.Completed)]
    [InlineData(SagaStates.Rejected)]
    public void Messages_in_terminal_states_are_ignored(string terminal)
    {
        var d = OrderSaga.Decide(Saga(terminal), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.True(d.Ignored);
        // Assert.Empty fails if the collection contains anything.
        Assert.Empty(d.Outgoing);
    }

    // Arrange : saga waiting for STOCK.
    // Act     : a PaymentCompleted arrives - a message that makes no sense in this state.
    // Assert  : it is ignored (no exception thrown).
    // Invariant: with at-least-once, unordered delivery, "impossible" messages DO happen.  The saga
    //           must stay calm and ignore them.
    // Bug caught: throwing an exception here.  MassTransit would retry the message a few times and
    //           then park it in the error queue - noise for operators, and it hides real failures.
    //           (If Decide threw, this test would fail with that exception - no try/catch needed.)
    [Fact]
    public void Unexpected_message_is_ignored_not_crashed()
    {
        var d = OrderSaga.Decide(Saga(SagaStates.WaitingForStock), Order, new PaymentCompleted(Order.Id, "c1", 300_000, 0), Options);
        Assert.True(d.Ignored);
    }
}
