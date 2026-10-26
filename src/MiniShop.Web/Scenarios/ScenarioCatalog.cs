using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Faults;

namespace MiniShop.Web.Scenarios;

/// <summary>
/// The sixteen scenarios of the Scenario Lab (S01–S16).  Read each body like a test:
/// arrange (wallet, stock, faults) → act (place orders) → wait on durable state → assert evidence.
/// Traces list the business messages, in order, that the owners must have PUBLISHED for the order.
/// </summary>
public static class ScenarioCatalog
{
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(75);

    private static readonly string[] HappyTrace =
        ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "PaymentCompleted", "OrderConfirmed"];
    private static readonly string[] CompensationTrace =
        ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "PaymentFailed", "ReleaseStock", "StockReleased", "OrderRejected"];

    public static IReadOnlyList<ScenarioDefinition> All { get; } =
    [
        new("S01", "Successful purchase", "Enough stock and enough money: reserve → charge → confirm.",
            "The happy path of the orchestrated saga. Note the final step: Inventory turns the reservation into real consumption when OrderConfirmed arrives.",
            HappyTrace, S01),

        new("S02", "Insufficient stock", "The customer asks for 999 mice; Inventory refuses before any money moves.",
            "A rejection BEFORE anything was reserved needs no compensation: the saga goes straight to Rejected and Payments is never contacted.",
            ["OrderPlaced", "ReserveStock", "StockReservationRejected", "OrderRejected"], S02),

        new("S03", "Insufficient wallet", "Stock is reserved, the wallet is too small, the reservation is released again.",
            "Compensation is a NEW business operation (ReleaseStock), not a database rollback. The order becomes Rejected only after Inventory confirmed the release.",
            CompensationTrace, S03, WalletBalance: 100_000),

        new("S04", "RabbitMQ outage", "The broker is stopped AFTER the order was committed; the message waits in the outbox and is delivered when the broker is back.",
            "The Transactional Outbox: the API still answers 202 because the message is in the database, not in the broker. Nothing is lost; delivery is only delayed.",
            HappyTrace, S04, TimeoutSeconds: 240,
            Requirement: "needs a way to stop/start the broker: the optional 'runner' container (compose profile 'faults') or you run 'docker compose stop rabbitmq' / 'start rabbitmq' by hand when the log asks for it."),

        new("S05", "Payments consumer paused", "The ChargeWallet message is held in the Payments consumer; the order waits; the request never blocked.",
            "Messaging decouples availability: the storefront keeps working, the message waits in the queue (visible backlog) and processing resumes where it stopped.",
            HappyTrace, S05),

        new("S06", "Duplicate delivery", "The outbox dispatcher is forced to publish OrderPlaced and ReserveStock twice.",
            "At-least-once delivery is normal. The Inbox detects the duplicates (InboxDuplicateIgnored) and the business effect happens exactly once.",
            HappyTrace, S06),

        new("S07", "Transient database failure", "Inventory's SaveChanges throws twice before the reservation commits.",
            "Nothing partial is written: the transaction rolls back, MassTransit retries, and the third attempt succeeds. Failures are visible, not hidden.",
            HappyTrace, S07),

        new("S08", "Slow read-model projection", "Every projection of this scenario is delayed by 2 seconds.",
            "CQRS has eventual consistency: the write side (orders.db) is Confirmed while the read side still shows an older state. The projection lag is measured, then converges.",
            HappyTrace, S08),

        new("S09", "Orders restarts mid-saga", "Inventory is slowed down so the saga is waiting; Orders is restarted; the saga continues from its persisted state.",
            "Saga state and outbox rows live in orders.db, so a restart loses nothing and sends nothing twice. (Compose: real container restart; dotnet run: the bus is stopped and restarted in-process.)",
            HappyTrace, S09, TimeoutSeconds: 150),

        new("S10", "Payment timeout → cancellation", "Payments cannot process ChargeWallet (every attempt fails → error queue). The saga times out, asks Payments to cancel, then releases the stock.",
            "A timeout is not 'not paid'. The saga asks Payments for a definitive answer; Payments stores a tombstone so a late debit can never happen; only then is stock released.",
            ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "CancelPayment", "PaymentCancelled", "ReleaseStock", "StockReleased", "OrderRejected"],
            S10, TimeoutSeconds: 120),

        new("S11", "Crash after publish, before outbox mark", "The dispatcher publishes OrderPlaced, then crashes before marking it as published; the row is published again.",
            "The classic gap of the outbox pattern produces a duplicate, never a loss. Consumers' Inbox makes the duplicate harmless.",
            HappyTrace, S11),

        new("S12", "Compensation keeps failing → RequiresAttention", "The wallet is too small and ReleaseStock fails permanently. After 3 attempts the saga stops and flags the order.",
            "The system must never claim 'Rejected' while stock is still held. RequiresAttention is an honest state; an operator retries once the fault is gone.",
            ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "PaymentFailed", "ReleaseStock", "OrderRequiresAttention", "ReleaseStock", "StockReleased", "OrderRejected"],
            S12, TimeoutSeconds: 150, WalletBalance: 100_000),

        new("S13", "Late PaymentCompleted after timeout", "Payments is slowed beyond the saga timeout. The saga asks to cancel, but the debit had already happened.",
            "Reconcile before you compensate: Payments answers PaymentAlreadyCompleted, so the saga CONFIRMS the order instead of releasing stock that the customer paid for.",
            ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "CancelPayment", "PaymentCompleted", "OrderConfirmed"], S13, TimeoutSeconds: 120),

        new("S14", "Checkout twice with one Idempotency-Key", "The same POST /api/orders is sent twice.",
            "The unique index on IdempotencyKey guarantees one order; the second call gets the SAME orderId back (safe retries for the UI).",
            HappyTrace, S14),

        new("S15", "Two orders race for the last unit", "Stock of the mouse is set to 1 and two orders are placed at the same moment.",
            "Atomic reservation (transaction + version check) lets at most one order win. Stock never goes negative.",
            [], S15),

        new("S16", "Projection crashes, then recovers", "The Web's projection consumer fails twice for this scenario's events.",
            "The projection is idempotent and the ProjectionInbox prevents double application, so retries converge to the same read model with no duplicates.",
            HappyTrace, S16),
    ];

    public static ScenarioDefinition? Find(string code) =>
        All.FirstOrDefault(s => string.Equals(s.Code, code, StringComparison.OrdinalIgnoreCase));

    // =================================================================================================
    //  Scenario bodies
    // =================================================================================================

    private static async Task S01(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        var walletBefore = await ctx.WalletBalanceAsync();

        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.HeadphonesLine(1), DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");

        await ctx.AssertDebitsAsync(orderId, 1);
        var walletAfter = await ctx.WalletBalanceAsync();
        await ctx.AssertAsync("wallet debited once by the order amount", walletAfter == walletBefore - 800_000,
            $"payments.db: {walletBefore:N0} → {walletAfter:N0}");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertStockAsync(DemoProducts.Headphones, before[DemoProducts.Headphones].OnHand - 1, 0, "headphones stock consumed, nothing reserved");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand - 1, 0, "mouse stock consumed, nothing reserved");
    }

    private static async Task S02(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(999));
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");

        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertReservationStateAsync(orderId, "Rejected");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, before[DemoProducts.Mouse].Reserved, "mouse stock unchanged");
        await ctx.AssertAsync("Payments was never contacted", await ctx.CountPublishedAsync("ChargeWallet", orderId) == 0, "no ChargeWallet published for the order");
    }

    private static async Task S03(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");

        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored after release");
        await ctx.AssertAsync("exactly one order", ctx.OrderIds.Count == 1, $"{ctx.OrderIds.Count} order(s) created");
        await ctx.AssertNoReservationHeldAsync(orderId);
    }

    private static async Task S04(ScenarioContext ctx)
    {
        // 1. take the broker down (runner or by hand)
        if (ctx.Clients.RunnerAvailable)
        {
            await ctx.LogAsync("runner available: stopping the rabbitmq container");
            await ctx.Clients.RunnerAsync("stop", "rabbitmq", ctx.Ct);
        }
        else
            await ctx.LogAsync("NO RUNNER: please stop the broker now, e.g.  docker compose stop rabbitmq   (waiting up to 90s)");

        await ctx.WaitUntilAsync("RabbitMQ to be DOWN", TimeSpan.FromSeconds(90),
            () => Task.FromResult(ctx.ServiceUp(ServiceNames.RabbitMq) ? null : "down"));
        await ctx.LogAsync("broker is down");

        // 2. place the order while the broker is down: the API still answers 202
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.KeyboardLine(1));
        await ctx.DelayAsync(TimeSpan.FromSeconds(4), "give the dispatcher time to fail its first attempts");
        var pending = await ctx.OutboxPendingAsync(ServiceNames.Orders);
        await ctx.AssertAsync("OrderPlaced waits in the Orders outbox", pending >= 1, $"orders.db outbox pending = {pending}");
        await ctx.AssertAsync("order accepted (Pending) without the broker", await ctx.OrderStateAsync(orderId) == "Pending", "orders.db state = Pending");

        // 3. bring the broker back
        if (ctx.Clients.RunnerAvailable)
        {
            await ctx.LogAsync("runner: starting the rabbitmq container");
            await ctx.Clients.RunnerAsync("start", "rabbitmq", ctx.Ct);
        }
        else
            await ctx.LogAsync("NO RUNNER: please start the broker again, e.g.  docker compose start rabbitmq   (waiting up to 120s)");

        await ctx.WaitUntilAsync("RabbitMQ to be UP", TimeSpan.FromSeconds(120),
            () => Task.FromResult(ctx.ServiceUp(ServiceNames.RabbitMq) ? "up" : null));
        await ctx.LogAsync("broker is up; services reconnect and the outbox drains");

        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(90), "Confirmed");
        await ctx.AssertAsync("outbox fully drained", await ctx.OutboxPendingAsync(ServiceNames.Orders) == 0, "orders.db outbox pending = 0");
        await ctx.AssertDebitsAsync(orderId, 1);
        await ctx.AssertReservationStateAsync(orderId, "Committed");
    }

    private static async Task S05(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Pause, "hold ChargeWallet in the consumer", hits: null);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "AwaitingPayment");

        await ctx.DelayAsync(TimeSpan.FromSeconds(5), "consumer is paused; the order must stay AwaitingPayment");
        await ctx.AssertAsync("order still waiting while the consumer is paused", await ctx.OrderStateAsync(orderId) == "AwaitingPayment",
            $"orders.db state = {await ctx.OrderStateAsync(orderId)}");
        var queue = ctx.Health.Queues.FirstOrDefault(q => q.Name == Topology.PaymentsQueue);
        await ctx.AssertAsync("ChargeWallet sits in the payments queue (backlog)", queue is { Backlog: >= 1 },
            $"RabbitMQ {Topology.PaymentsQueue}: ready={queue?.Ready}, unacked={queue?.Unacked}");

        await ctx.ClearFaultsAsync();
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    private static async Task S06(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Orders, FaultPoints.OutboxDispatch, FaultKind.Duplicate, "publish the next two outbox rows twice", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let telemetry of the duplicate deliveries arrive");

        var dedup = await ctx.CountAuditAsync(DiagnosticKind.InboxDuplicateIgnored);
        await ctx.AssertAsync("duplicates were detected by an Inbox", dedup >= 2, $"{dedup} InboxDuplicateIgnored record(s)");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    private static async Task S07(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.DbSaveChanges, FaultKind.Fail, "SaveChanges throws before commit", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.DelayAsync(TimeSpan.FromSeconds(1), "let telemetry arrive");

        var failed = await ctx.CountAuditAsync(DiagnosticKind.ConsumerFailed, orderId, ServiceNames.Inventory);
        await ctx.AssertAsync("Inventory failed and retried", failed >= 2, $"{failed} ConsumerFailed record(s) from Inventory");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    private static async Task S08(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Web, FaultPoints.WebProjection, FaultKind.Delay, "delay every projection of this scenario", hits: null, delayMs: 2000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");

        var readState = await ctx.ReadModelOrderStateAsync(orderId);
        await ctx.LogAsync($"write side says Confirmed; read model says {readState ?? "(no row yet)"}");
        await ctx.AssertAsync("read model lagged behind the write model", readState != "Confirmed",
            $"readmodel.db state right after the owner confirmed: {readState ?? "none"}");

        await ctx.WaitUntilAsync("read model to converge to Confirmed", Long,
            async () => await ctx.ReadModelOrderStateAsync(orderId) == "Confirmed" ? "ok" : null);
        var lag = await ctx.Queries.GetProjectionLagAsync(0);
        await ctx.AssertAsync("projection lag was measured", lag.LastLagMs >= 1500, $"last projection lag = {lag.LastLagMs:0} ms");
        await ctx.AssertAsync("read model converged", true, "readmodel.db state = Confirmed");
    }

    private static async Task S09(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.InventoryReserve, FaultKind.Delay, "slow reservation so the saga is mid-flight", hits: 1, delayMs: 8000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitUntilAsync("saga to be WaitingForStock", Short, async () => await ctx.SagaStateAsync(orderId) == "WaitingForStock" ? "ok" : null);
        await ctx.LogAsync("saga persisted as WaitingForStock; restarting Orders now");

        if (ctx.Clients.Urls.OrdersRestartMode == "Crash")
        {
            await ctx.Clients.CrashOrdersAsync(ctx.Ct);
            await ctx.WaitUntilAsync("Orders to go down", Short, () => Task.FromResult(ctx.ServiceUp(ServiceNames.Orders) ? null : "down"));
            await ctx.LogAsync("Orders process exited; waiting for the container to restart");
            await ctx.WaitUntilAsync("Orders to come back", TimeSpan.FromSeconds(90), () => Task.FromResult(ctx.ServiceUp(ServiceNames.Orders) ? "up" : null));
        }
        else
        {
            await ctx.Clients.RestartOrdersBusAsync(ctx.Ct);
            await ctx.LogAsync("Orders bus stopped and restarted in-process (dotnet run mode)");
        }

        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.AssertAsync("ReserveStock was sent exactly once", await ctx.CountPublishedAsync("ReserveStock", orderId) == 1, "distinct ReserveStock messages = 1");
        await ctx.AssertAsync("ChargeWallet was sent exactly once", await ctx.CountPublishedAsync("ChargeWallet", orderId) == 1, "distinct ChargeWallet messages = 1");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    private static async Task S10(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Fail, "ChargeWallet always crashes → error queue", hits: null);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(100), "Rejected");

        await ctx.AssertAsync("ChargeWallet ended in the error queue", await ctx.CountAuditAsync(DiagnosticKind.MessageDeadLettered, orderId) >= 1, "MessageDeadLettered recorded by Payments");
        await ctx.AssertDebitsAsync(orderId, 0);
        var ledger = await ctx.LedgerAsync(orderId);
        await ctx.AssertAsync("tombstone blocks any late debit", ledger.Any(l => l!["state"]!.GetValue<string>() == "Cancelled"),
            $"payments.db ledger states: [{string.Join(", ", ledger.Select(l => l!["state"]!.GetValue<string>()))}]");
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored");
    }

    private static async Task S11(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Orders, FaultPoints.OutboxAfterPublish, FaultKind.Fail, "crash after publish, before mark-as-published", hits: 1);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let telemetry arrive");

        await ctx.AssertAsync("outbox row was re-dispatched", await ctx.AuditContainsAsync(DiagnosticKind.RetryScheduled, "outbox attempt 1 failed"), "RetryScheduled recorded for the outbox row");
        var dedup = await ctx.CountAuditAsync(DiagnosticKind.InboxDuplicateIgnored);
        await ctx.AssertAsync("redelivered OrderPlaced was ignored by the Inbox", dedup >= 1, $"{dedup} InboxDuplicateIgnored record(s)");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    private static async Task S12(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.InventoryRelease, FaultKind.Fail, "ReleaseStock always crashes", hits: null);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(120), "RequiresAttention");

        await ctx.AssertAsync("order was NOT falsely rejected while stock is held", await ctx.OrderStateAsync(orderId) == "RequiresAttention", "orders.db state = RequiresAttention");
        await ctx.AssertReservationStateAsync(orderId, "Reserved");
        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertAsync("compensation was retried before giving up", await ctx.CountPublishedAsync("ReleaseStock", orderId) >= 3, $"{await ctx.CountPublishedAsync("ReleaseStock", orderId)} distinct ReleaseStock commands");

        await ctx.LogAsync("operator fixes the problem (fault removed) and retries the compensation");
        await ctx.ClearFaultsAsync();
        await ctx.Clients.RetryCompensationAsync(orderId, ctx.Ct);
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored after manual retry");
    }

    private static async Task S13(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Delay, "payment processed 30s late (saga timeout is 20s)", hits: 1, delayMs: 30_000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitUntilAsync("saga to time out and send CancelPayment", TimeSpan.FromSeconds(45), async () => await ctx.SagaStateAsync(orderId) == "WaitingForPaymentCancellation" ? "ok" : null);
        await ctx.LogAsync("saga is WaitingForPaymentCancellation; the late debit is still in progress");

        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed", "Rejected");
        await ctx.AssertAsync("late payment reconciled → Confirmed, not Rejected", await ctx.OrderStateAsync(orderId) == "Confirmed", $"orders.db state = {await ctx.OrderStateAsync(orderId)}");
        await ctx.AssertDebitsAsync(orderId, 1);
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.WaitUntilAsync("PaymentAlreadyCompleted answer", Short, async () => await ctx.CountPublishedAsync("PaymentAlreadyCompleted", orderId) >= 1 ? "ok" : null);
        await ctx.AssertAsync("Payments answered the cancel with PaymentAlreadyCompleted", true, "PaymentAlreadyCompleted published");
        await ctx.WaitUntilAsync("the late-payment saga record to arrive", Short, async () => await ctx.AuditContainsAsync(DiagnosticKind.SagaTransitioned, "LATE") ? "ok" : null);
        await ctx.AssertAsync("late event was recorded", true, "SagaTransitioned detail mentions the late payment");
    }

    private static async Task S14(ScenarioContext ctx)
    {
        var key = $"S14-{ctx.ScenarioId:N}";
        var first = await ctx.PlaceOrderAsync(key, DemoProducts.MouseLine(1));
        var second = await ctx.PlaceOrderAsync(key, DemoProducts.MouseLine(1));
        await ctx.AssertAsync("second checkout returned the same orderId", first == second, $"{first} == {second}");
        var count = (await ctx.Clients.DiagnosticsAsync(ServiceNames.Orders, $"orders/count?idempotencyKey={key}", ctx.Ct))?["count"]?.GetValue<int>();
        await ctx.AssertAsync("exactly one order row for the key", count == 1, $"orders.db count for key = {count}");
        await ctx.WaitForOrderStateAsync(first, Short, "Confirmed");
        await ctx.AssertDebitsAsync(first, 1);
    }

    private static async Task S15(ScenarioContext ctx)
    {
        await ctx.SetStockAsync(DemoProducts.Mouse, 1);
        var tasks = new[]
        {
            ctx.PlaceOrderAsync($"S15-{ctx.ScenarioId:N}-a", DemoProducts.MouseLine(1)),
            ctx.PlaceOrderAsync($"S15-{ctx.ScenarioId:N}-b", DemoProducts.MouseLine(1)),
        };
        var ids = await Task.WhenAll(tasks);
        var states = new List<string>();
        foreach (var id in ids) states.Add(await ctx.WaitForOrderStateAsync(id, Short, "Confirmed", "Rejected"));

        await ctx.AssertAsync("exactly one order won the last unit", states.Count(s => s == "Confirmed") == 1 && states.Count(s => s == "Rejected") == 1,
            $"states: [{string.Join(", ", states)}]");
        var stock = await ctx.StockAsync();
        var (onHand, reserved) = stock[DemoProducts.Mouse];
        await ctx.AssertAsync("stock never negative, nothing left reserved", onHand == 0 && reserved == 0, $"inventory.db mouse: onHand={onHand}, reserved={reserved}");
        var debits = 0;
        foreach (var id in ids) debits += (await ctx.LedgerAsync(id)).Count(l => l!["state"]!.GetValue<string>() == "Completed");
        await ctx.AssertAsync("exactly one debit across both orders", debits == 1, $"{debits} Completed debit(s)");

        await ctx.SetStockAsync(DemoProducts.Mouse, 10);   // restore the seed value for the next experiments
    }

    private static async Task S16(ScenarioContext ctx)
    {
        await ctx.AddFaultAsync(ServiceNames.Web, FaultPoints.WebProjection, FaultKind.Fail, "projection consumer throws", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.WaitUntilAsync("read model to converge", Long, async () => await ctx.ReadModelOrderStateAsync(orderId) == "Confirmed" ? "ok" : null);
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let the last telemetry arrive");

        var failed = await ctx.CountAuditAsync(DiagnosticKind.ConsumerFailed, service: ServiceNames.Web);
        await ctx.AssertAsync("projection failed and was retried", failed >= 2, $"{failed} ConsumerFailed record(s) from Web");
        var (rows, distinct) = await ctx.BusinessAuditRowsAsync();
        await ctx.AssertAsync("no event projected twice", rows == distinct, $"{rows} business audit rows, {distinct} distinct message ids");
        await ctx.AssertAsync("read model converged", true, "readmodel.db state = Confirmed");
    }
}
