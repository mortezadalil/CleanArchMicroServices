// =====================================================================================================
//  ScenarioCatalog.cs  —  the sixteen experiments of the Scenario Lab (S01..S16)
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    The list of all scenarios (metadata at the top) and their bodies (private methods S01..S16 at the
//    bottom).  Each body is written like an automated test:
//      ARRANGE  — set wallet / stock, arm fault rules in a service
//      ACT      — place an order through the real public API (POST /api/orders)
//      WAIT     — poll DURABLE state in the OWNER database (via diagnostics) until it changes
//      ASSERT   — record invariants with evidence read from the owner databases / event audit
//
//  Where the student sees it:
//    Developer page → Scenario Lab cards (Code, Title, Summary, Lesson, Requirement) and, after a run,
//    the assertions and log written by these bodies.
//
//  Data sources:
//    Everything goes through ScenarioContext: owner databases are read via each service's read-only
//    diagnostics endpoints, never opened directly.  The Web's own event audit gives "how" evidence
//    (retries, duplicates, dead letters).
//
//  Patterns proved:
//    S01 saga happy path · S02 early rejection · S03 compensation · S04 transactional outbox ·
//    S05 async decoupling / queue backlog · S06 inbox de-duplication · S07 retry + atomic commit ·
//    S08 CQRS eventual consistency · S09 durable saga state across restarts · S10 timeout + tombstone ·
//    S11 outbox "publish then crash" gap · S12 failing compensation → RequiresAttention ·
//    S13 reconcile before compensate · S14 idempotency key · S15 concurrency on the last unit ·
//    S16 idempotent projection.
//
//  Read next: ScenarioContext.cs (the helpers used below), then ScenarioRunner.cs (the verdict).
// =====================================================================================================

using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Web.Services;   // QueueInfo: queue counters read from the RabbitMQ management API (S05)

namespace MiniShop.Web.Scenarios;

/// <summary>
/// The sixteen scenarios of the Scenario Lab (S01–S16).  Read each body like a test:
/// arrange (wallet, stock, faults) → act (place orders) → wait on durable state → assert evidence.
/// Traces list the business messages, in order, that the owners must have PUBLISHED for the order.
/// </summary>
public static class ScenarioCatalog
{
    // Two standard wait budgets.  Short (30 s) is plenty for a normal saga; Long (75 s) leaves room for
    // retries with back-off, injected delays and slow projections.
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(75);

    // The message sequence of a successful order: Orders publishes OrderPlaced; the saga (in Orders)
    // sends ReserveStock to Inventory, which answers StockReserved; the saga sends ChargeWallet to
    // Payments, which answers PaymentCompleted; the saga publishes OrderConfirmed.
    private static readonly string[] HappyTrace =
        ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "PaymentCompleted", "OrderConfirmed"];
    // The sequence when the payment fails AFTER stock was reserved: the saga must undo the reservation
    // with a new command (ReleaseStock) and only reject once Inventory confirms (StockReleased).
    private static readonly string[] CompensationTrace =
        ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "PaymentFailed", "ReleaseStock", "StockReleased", "OrderRejected"];

    /// <summary>
    /// All scenarios, in display order.  Each entry: code, title, summary, lesson, expected trace, body,
    /// and optional overrides (TimeoutSeconds, WalletBalance, Requirement).  See <see cref="ScenarioDefinition"/>.
    /// </summary>
    public static IReadOnlyList<ScenarioDefinition> All { get; } =
    [
        new("S01", "Successful purchase", "Enough stock and enough money: reserve → charge → confirm.",
            "The happy path of the orchestrated saga. Note the final step: Inventory turns the reservation into real consumption when OrderConfirmed arrives.",
            HappyTrace, S01),

        new("S02", "Insufficient stock", "The customer asks for 999 mice; Inventory refuses before any money moves.",
            "A rejection BEFORE anything was reserved needs no compensation: the saga goes straight to Rejected and Payments is never contacted.",
            ["OrderPlaced", "ReserveStock", "StockReservationRejected", "OrderRejected"], S02),

        // WalletBalance: 100_000 is less than one mouse (300,000), so the charge must fail.
        new("S03", "Insufficient wallet", "Stock is reserved, the wallet is too small, the reservation is released again.",
            "Compensation is a NEW business operation (ReleaseStock), not a database rollback. The order becomes Rejected only after Inventory confirmed the release.",
            CompensationTrace, S03, WalletBalance: 100_000),

        // 240 s: stopping and starting a container (or waiting for a human to do it) takes time.
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

        // Note the trace: CancelPayment / PaymentCancelled appear BEFORE the compensation (ReleaseStock).
        new("S10", "Payment timeout → cancellation", "Payments cannot process ChargeWallet (every attempt fails → error queue). The saga times out, asks Payments to cancel, then releases the stock.",
            "A timeout is not 'not paid'. The saga asks Payments for a definitive answer; Payments stores a tombstone so a late debit can never happen; only then is stock released.",
            ["OrderPlaced", "ReserveStock", "StockReserved", "ChargeWallet", "CancelPayment", "PaymentCancelled", "ReleaseStock", "StockReleased", "OrderRejected"],
            S10, TimeoutSeconds: 120),

        new("S11", "Crash after publish, before outbox mark", "The dispatcher publishes OrderPlaced, then crashes before marking it as published; the row is published again.",
            "The classic gap of the outbox pattern produces a duplicate, never a loss. Consumers' Inbox makes the duplicate harmless.",
            HappyTrace, S11),

        // The trace shows ReleaseStock twice: first the failing attempts, then the operator's manual retry.
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

        // Empty expected trace ([]): two orders with different outcomes, so no single trace to check.
        new("S15", "Two orders race for the last unit", "Stock of the mouse is set to 1 and two orders are placed at the same moment.",
            "Atomic reservation (transaction + version check) lets at most one order win. Stock never goes negative.",
            [], S15),

        new("S16", "Projection crashes, then recovers", "The Web's projection consumer fails twice for this scenario's events.",
            "The projection is idempotent and the ProjectionInbox prevents double application, so retries converge to the same read model with no duplicates.",
            HappyTrace, S16),
    ];

    /// <summary>Looks a scenario up by code, ignoring case ("s04" finds "S04"); null when unknown.</summary>
    public static ScenarioDefinition? Find(string code) =>
        All.FirstOrDefault(s => string.Equals(s.Code, code, StringComparison.OrdinalIgnoreCase));

    // =================================================================================================
    //  Scenario bodies
    // =================================================================================================

    /// <summary>S01 — happy path of the orchestrated saga (reserve → charge → confirm → consume).</summary>
    private static async Task S01(ScenarioContext ctx)
    {
        // ARRANGE: remember the starting numbers so we can assert the exact change afterwards.
        var before = await ctx.StockAsync();
        var walletBefore = await ctx.WalletBalanceAsync();

        // ACT: buy headphones (500,000) + mouse (300,000) = 800,000.
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.HeadphonesLine(1), DemoProducts.MouseLine(1));
        // WAIT on durable state: the order row in orders.db becomes Confirmed.
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");

        // ASSERT (payments.db): exactly one debit, of exactly the order amount.
        await ctx.AssertDebitsAsync(orderId, 1);
        var walletAfter = await ctx.WalletBalanceAsync();
        await ctx.AssertAsync("wallet debited once by the order amount", walletAfter == walletBefore - 800_000,
            $"payments.db: {walletBefore:N0} → {walletAfter:N0}");
        // ASSERT (inventory.db): the reservation was turned into real consumption (Committed),
        // on-hand went down by one per product and nothing is left "reserved".
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertStockAsync(DemoProducts.Headphones, before[DemoProducts.Headphones].OnHand - 1, 0, "headphones stock consumed, nothing reserved");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand - 1, 0, "mouse stock consumed, nothing reserved");
    }

    /// <summary>S02 — rejection before anything was reserved: no compensation, Payments never contacted.</summary>
    private static async Task S02(ScenarioContext ctx)
    {
        // ARRANGE: snapshot of the stock to prove later that it did not change.
        var before = await ctx.StockAsync();
        // ACT: 999 mice is far more than the seeded stock.
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(999));
        // WAIT: orders.db says Rejected.
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");

        // ASSERT: no money moved, Inventory recorded its refusal, stock untouched...
        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertReservationStateAsync(orderId, "Rejected");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, before[DemoProducts.Mouse].Reserved, "mouse stock unchanged");
        // ...and the saga never even sent ChargeWallet (event audit of published messages).
        await ctx.AssertAsync("Payments was never contacted", await ctx.CountPublishedAsync("ChargeWallet", orderId) == 0, "no ChargeWallet published for the order");
    }

    /// <summary>S03 — compensation: the payment fails after stock was reserved, so the stock is released.</summary>
    private static async Task S03(ScenarioContext ctx)
    {
        // ARRANGE: the wallet was already set to 100,000 by the runner (WalletBalance above).
        var before = await ctx.StockAsync();
        // ACT: one mouse costs 300,000 → PaymentFailed.
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT: Rejected appears only AFTER Inventory confirmed the release (StockReleased).
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");

        // ASSERT: no debit; the reservation went Reserved → Released (compensated, not deleted);
        // on-hand is back to the original value and nothing is reserved.
        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored after release");
        await ctx.AssertAsync("exactly one order", ctx.OrderIds.Count == 1, $"{ctx.OrderIds.Count} order(s) created");
        await ctx.AssertNoReservationHeldAsync(orderId);
    }

    /// <summary>S04 — Transactional Outbox: the broker is down, the order is still accepted, delivery waits.</summary>
    private static async Task S04(ScenarioContext ctx)
    {
        // 1. take the broker down (runner or by hand)
        // The optional "runner" container can call docker for us; otherwise we ask the student in the log.
        if (ctx.Clients.RunnerAvailable)
        {
            await ctx.LogAsync("runner available: stopping the rabbitmq container");
            await ctx.Clients.RunnerAsync("stop", "rabbitmq", ctx.Ct);
        }
        else
            await ctx.LogAsync("NO RUNNER: please stop the broker now, e.g.  docker compose stop rabbitmq   (waiting up to 90s)");

        // WAIT until the health monitor really sees RabbitMQ as down (probe returns "down" or null).
        await ctx.WaitUntilAsync("RabbitMQ to be DOWN", TimeSpan.FromSeconds(90),
            () => Task.FromResult(ctx.ServiceUp(ServiceNames.RabbitMq) ? null : "down"));
        await ctx.LogAsync("broker is down");

        // 2. place the order while the broker is down: the API still answers 202
        // (Orders saves the order AND the OrderPlaced outbox row in one database transaction.)
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.KeyboardLine(1));
        await ctx.DelayAsync(TimeSpan.FromSeconds(4), "give the dispatcher time to fail its first attempts");
        // ASSERT: the message is safe in orders.db (outbox), and the order exists as Pending.
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

        // 4. the delayed message is delivered and the saga completes normally.
        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(90), "Confirmed");
        // ASSERT: outbox empty (nothing lost, nothing stuck), one debit, reservation committed.
        await ctx.AssertAsync("outbox fully drained", await ctx.OutboxPendingAsync(ServiceNames.Orders) == 0, "orders.db outbox pending = 0");
        await ctx.AssertDebitsAsync(orderId, 1);
        await ctx.AssertReservationStateAsync(orderId, "Committed");
    }

    /// <summary>S05 — messaging decouples availability: a paused consumer creates a backlog, not an error.</summary>
    private static async Task S05(ScenarioContext ctx)
    {
        // ARRANGE: a Pause fault at Payments' ChargeWallet consumer, until removed (hits: null).
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Pause, "hold ChargeWallet in the consumer", hits: null);
        // ACT + WAIT: stock is reserved, the saga now waits for the payment.
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "AwaitingPayment");

        // ASSERT: after 5 more seconds the order is STILL AwaitingPayment (no timeout, no error)...
        await ctx.DelayAsync(TimeSpan.FromSeconds(5), "consumer is paused; the order must stay AwaitingPayment");
        await ctx.AssertAsync("order still waiting while the consumer is paused", await ctx.OrderStateAsync(orderId) == "AwaitingPayment",
            $"orders.db state = {await ctx.OrderStateAsync(orderId)}");
        // ...and the message is visible in RabbitMQ (ready or unacked) on the payments queue.
        // We ask the RabbitMQ management API directly (not the health monitor's cached copy) and keep asking
        // for up to 15 s, because the management API refreshes its queue statistics only every few seconds:
        // right after the message arrives the counters can still say 0 although the message is held.
        QueueInfo? queue = null;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        do
        {
            queue = (await ctx.Clients.QueuesAsync(ctx.Ct)).FirstOrDefault(q => q.Name == Topology.PaymentsQueue);
            if (queue is { Backlog: >= 1 }) break;
            await Task.Delay(1000, ctx.Ct);
        }
        while (DateTime.UtcNow < deadline);
        await ctx.AssertAsync("ChargeWallet sits in the payments queue (backlog)", queue is { Backlog: >= 1 },
            $"RabbitMQ {Topology.PaymentsQueue}: ready={queue?.Ready}, unacked={queue?.Unacked}");

        // "Repair" the consumer: processing resumes where it stopped and the order confirms.
        await ctx.ClearFaultsAsync();
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    /// <summary>S06 — at-least-once delivery: duplicates are detected by the Inbox, effect happens once.</summary>
    private static async Task S06(ScenarioContext ctx)
    {
        // ARRANGE: the Orders outbox dispatcher publishes its next two rows twice (same message id).
        await ctx.AddFaultAsync(ServiceNames.Orders, FaultPoints.OutboxDispatch, FaultKind.Duplicate, "publish the next two outbox rows twice", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Short, "Confirmed");
        // Telemetry has its own queue and may arrive a little after the business state.
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let telemetry of the duplicate deliveries arrive");

        // ASSERT "how": consumers reported InboxDuplicateIgnored at least twice.
        var dedup = await ctx.CountAuditAsync(DiagnosticKind.InboxDuplicateIgnored);
        await ctx.AssertAsync("duplicates were detected by an Inbox", dedup >= 2, $"{dedup} InboxDuplicateIgnored record(s)");
        // ASSERT "what": still one reservation and one debit.
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    /// <summary>S07 — transient database failure: rollback, retry, success; nothing partial is written.</summary>
    private static async Task S07(ScenarioContext ctx)
    {
        // ARRANGE: Inventory's SaveChanges throws twice (an EF Core interceptor fires the fault).
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.DbSaveChanges, FaultKind.Fail, "SaveChanges throws before commit", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // Long budget: MassTransit retries with a delay between attempts.
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.DelayAsync(TimeSpan.FromSeconds(1), "let telemetry arrive");

        // ASSERT: the failures are visible (ConsumerFailed from Inventory) and the end result is clean.
        var failed = await ctx.CountAuditAsync(DiagnosticKind.ConsumerFailed, orderId, ServiceNames.Inventory);
        await ctx.AssertAsync("Inventory failed and retried", failed >= 2, $"{failed} ConsumerFailed record(s) from Inventory");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    /// <summary>S08 — CQRS eventual consistency: the read model lags behind the write model, then converges.</summary>
    private static async Task S08(ScenarioContext ctx)
    {
        // ARRANGE: every projection in the Web (read side) waits 2 s before applying an event.
        await ctx.AddFaultAsync(ServiceNames.Web, FaultPoints.WebProjection, FaultKind.Delay, "delay every projection of this scenario", hits: null, delayMs: 2000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT on the WRITE side (orders.db).
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");

        // Immediately read the READ side: it should still show an older state.
        var readState = await ctx.ReadModelOrderStateAsync(orderId);
        await ctx.LogAsync($"write side says Confirmed; read model says {readState ?? "(no row yet)"}");
        await ctx.AssertAsync("read model lagged behind the write model", readState != "Confirmed",
            $"readmodel.db state right after the owner confirmed: {readState ?? "none"}");

        // WAIT for the read side to catch up, then check the measured lag reflects the 2 s delay.
        await ctx.WaitUntilAsync("read model to converge to Confirmed", Long,
            async () => await ctx.ReadModelOrderStateAsync(orderId) == "Confirmed" ? "ok" : null);
        var lag = await ctx.Queries.GetProjectionLagAsync(0);
        await ctx.AssertAsync("projection lag was measured", lag.LastLagMs >= 1500, $"last projection lag = {lag.LastLagMs:0} ms");
        // Converged is already proven by the wait above; recorded as an assertion so it shows in the report.
        await ctx.AssertAsync("read model converged", true, "readmodel.db state = Confirmed");
    }

    /// <summary>S09 — saga state and outbox are durable: Orders restarts mid-saga and simply continues.</summary>
    private static async Task S09(ScenarioContext ctx)
    {
        // ARRANGE: slow Inventory by 8 s so the saga is caught "in flight" (waiting for stock).
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.InventoryReserve, FaultKind.Delay, "slow reservation so the saga is mid-flight", hits: 1, delayMs: 8000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT until the saga row in orders.db is persisted in WaitingForStock.
        await ctx.WaitUntilAsync("saga to be WaitingForStock", Short, async () => await ctx.SagaStateAsync(orderId) == "WaitingForStock" ? "ok" : null);
        await ctx.LogAsync("saga persisted as WaitingForStock; restarting Orders now");

        // ACT: restart Orders.  Under docker compose the process really exits and the container restarts;
        // under "dotnet run" nobody would restart a crashed process, so only the bus is restarted.
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

        // The saga continues from its stored state when StockReserved arrives.
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        // ASSERT: the restart did not cause commands to be sent again (distinct message ids = 1).
        await ctx.AssertAsync("ReserveStock was sent exactly once", await ctx.CountPublishedAsync("ReserveStock", orderId) == 1, "distinct ReserveStock messages = 1");
        await ctx.AssertAsync("ChargeWallet was sent exactly once", await ctx.CountPublishedAsync("ChargeWallet", orderId) == 1, "distinct ChargeWallet messages = 1");
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    /// <summary>S10 — saga timeout: ask Payments to cancel (tombstone) BEFORE releasing stock.</summary>
    private static async Task S10(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        // ARRANGE: every ChargeWallet attempt crashes → after retries MassTransit moves it to the error queue.
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Fail, "ChargeWallet always crashes → error queue", hits: null);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT: the saga's payment timeout fires, CancelPayment → PaymentCancelled → ReleaseStock → Rejected.
        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(100), "Rejected");

        // ASSERT: the message really was dead-lettered, and no money moved.
        await ctx.AssertAsync("ChargeWallet ended in the error queue", await ctx.CountAuditAsync(DiagnosticKind.MessageDeadLettered, orderId) >= 1, "MessageDeadLettered recorded by Payments");
        await ctx.AssertDebitsAsync(orderId, 0);
        // ASSERT: Payments wrote a "Cancelled" ledger row (tombstone): a late ChargeWallet would now be refused.
        var ledger = await ctx.LedgerAsync(orderId);
        await ctx.AssertAsync("tombstone blocks any late debit", ledger.Any(l => l!["state"]!.GetValue<string>() == "Cancelled"),
            $"payments.db ledger states: [{string.Join(", ", ledger.Select(l => l!["state"]!.GetValue<string>()))}]");
        // ASSERT: and only then the stock was released and restored.
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored");
    }

    /// <summary>S11 — the outbox gap: crash after publish but before marking → duplicate, never loss.</summary>
    private static async Task S11(ScenarioContext ctx)
    {
        // ARRANGE: the dispatcher throws right AFTER publishing, so the row stays "not published".
        await ctx.AddFaultAsync(ServiceNames.Orders, FaultPoints.OutboxAfterPublish, FaultKind.Fail, "crash after publish, before mark-as-published", hits: 1);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let telemetry arrive");

        // ASSERT: the dispatcher scheduled a retry for the row (so it published a second time)...
        await ctx.AssertAsync("outbox row was re-dispatched", await ctx.AuditContainsAsync(DiagnosticKind.RetryScheduled, "outbox attempt 1 failed"), "RetryScheduled recorded for the outbox row");
        // ...the consumer's Inbox ignored the second copy...
        var dedup = await ctx.CountAuditAsync(DiagnosticKind.InboxDuplicateIgnored);
        await ctx.AssertAsync("redelivered OrderPlaced was ignored by the Inbox", dedup >= 1, $"{dedup} InboxDuplicateIgnored record(s)");
        // ...and the business effect happened exactly once.
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        await ctx.AssertDebitsAsync(orderId, 1);
    }

    /// <summary>S12 — compensation itself fails: the saga stops at RequiresAttention; an operator retries.</summary>
    private static async Task S12(ScenarioContext ctx)
    {
        var before = await ctx.StockAsync();
        // ARRANGE: small wallet (WalletBalance) → payment fails; and ReleaseStock always crashes.
        await ctx.AddFaultAsync(ServiceNames.Inventory, FaultPoints.InventoryRelease, FaultKind.Fail, "ReleaseStock always crashes", hits: null);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT: after its compensation attempts the saga gives up honestly.
        await ctx.WaitForOrderStateAsync(orderId, TimeSpan.FromSeconds(120), "RequiresAttention");

        // ASSERT: NOT "Rejected" (stock is still held), reservation still Reserved, no debit,
        // and the saga really tried several times (distinct ReleaseStock commands).
        await ctx.AssertAsync("order was NOT falsely rejected while stock is held", await ctx.OrderStateAsync(orderId) == "RequiresAttention", "orders.db state = RequiresAttention");
        await ctx.AssertReservationStateAsync(orderId, "Reserved");
        await ctx.AssertDebitsAsync(orderId, 0);
        await ctx.AssertAsync("compensation was retried before giving up", await ctx.CountPublishedAsync("ReleaseStock", orderId) >= 3, $"{await ctx.CountPublishedAsync("ReleaseStock", orderId)} distinct ReleaseStock commands");

        // Second act: the operator fixes the cause and presses "retry compensation" (an Orders endpoint).
        await ctx.LogAsync("operator fixes the problem (fault removed) and retries the compensation");
        await ctx.ClearFaultsAsync();
        await ctx.Clients.RetryCompensationAsync(orderId, ctx.Ct);
        // Now the release succeeds and the order may finally be Rejected.
        await ctx.WaitForOrderStateAsync(orderId, Short, "Rejected");
        await ctx.AssertReservationStateAsync(orderId, "Released");
        await ctx.AssertStockAsync(DemoProducts.Mouse, before[DemoProducts.Mouse].OnHand, 0, "stock restored after manual retry");
    }

    /// <summary>S13 — reconcile before compensating: a late but real payment leads to Confirmed.</summary>
    private static async Task S13(ScenarioContext ctx)
    {
        // ARRANGE: Payments processes the charge 30 s late, longer than the saga's 20 s payment timeout.
        await ctx.AddFaultAsync(ServiceNames.Payments, FaultPoints.PaymentsCharge, FaultKind.Delay, "payment processed 30s late (saga timeout is 20s)", hits: 1, delayMs: 30_000);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT: the saga timed out and is now asking Payments to cancel.
        await ctx.WaitUntilAsync("saga to time out and send CancelPayment", TimeSpan.FromSeconds(45), async () => await ctx.SagaStateAsync(orderId) == "WaitingForPaymentCancellation" ? "ok" : null);
        await ctx.LogAsync("saga is WaitingForPaymentCancellation; the late debit is still in progress");

        // WAIT for either final state, then ASSERT it is the right one (Confirmed).
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed", "Rejected");
        await ctx.AssertAsync("late payment reconciled → Confirmed, not Rejected", await ctx.OrderStateAsync(orderId) == "Confirmed", $"orders.db state = {await ctx.OrderStateAsync(orderId)}");
        await ctx.AssertDebitsAsync(orderId, 1);
        await ctx.AssertReservationStateAsync(orderId, "Committed");
        // Evidence of the reconciliation: Payments answered CancelPayment with PaymentAlreadyCompleted.
        // These are waits (not plain reads) because telemetry may arrive a little later.
        await ctx.WaitUntilAsync("PaymentAlreadyCompleted answer", Short, async () => await ctx.CountPublishedAsync("PaymentAlreadyCompleted", orderId) >= 1 ? "ok" : null);
        await ctx.AssertAsync("Payments answered the cancel with PaymentAlreadyCompleted", true, "PaymentAlreadyCompleted published");
        await ctx.WaitUntilAsync("the late-payment saga record to arrive", Short, async () => await ctx.AuditContainsAsync(DiagnosticKind.SagaTransitioned, "LATE") ? "ok" : null);
        await ctx.AssertAsync("late event was recorded", true, "SagaTransitioned detail mentions the late payment");
    }

    /// <summary>S14 — idempotency key: the same checkout sent twice creates one order.</summary>
    private static async Task S14(ScenarioContext ctx)
    {
        // ARRANGE: one explicit key reused for both calls (like a browser retrying after a timeout).
        var key = $"S14-{ctx.ScenarioId:N}";
        // ACT: two identical POSTs.
        var first = await ctx.PlaceOrderAsync(key, DemoProducts.MouseLine(1));
        var second = await ctx.PlaceOrderAsync(key, DemoProducts.MouseLine(1));
        // ASSERT: same id returned, and orders.db really holds only one row for the key.
        await ctx.AssertAsync("second checkout returned the same orderId", first == second, $"{first} == {second}");
        var count = (await ctx.Clients.DiagnosticsAsync(ServiceNames.Orders, $"orders/count?idempotencyKey={key}", ctx.Ct))?["count"]?.GetValue<int>();
        await ctx.AssertAsync("exactly one order row for the key", count == 1, $"orders.db count for key = {count}");
        // And that single order is processed normally, with one debit.
        await ctx.WaitForOrderStateAsync(first, Short, "Confirmed");
        await ctx.AssertDebitsAsync(first, 1);
    }

    /// <summary>S15 — concurrency: two orders race for the last unit; at most one wins.</summary>
    private static async Task S15(ScenarioContext ctx)
    {
        // ARRANGE: exactly one mouse in stock.
        await ctx.SetStockAsync(DemoProducts.Mouse, 1);
        // ACT: start both requests WITHOUT awaiting in between, so they run at the same time.
        var tasks = new[]
        {
            ctx.PlaceOrderAsync($"S15-{ctx.ScenarioId:N}-a", DemoProducts.MouseLine(1)),
            ctx.PlaceOrderAsync($"S15-{ctx.ScenarioId:N}-b", DemoProducts.MouseLine(1)),
        };
        var ids = await Task.WhenAll(tasks);
        // WAIT until each order reaches a final state (either one is fine for the wait).
        var states = new List<string>();
        foreach (var id in ids) states.Add(await ctx.WaitForOrderStateAsync(id, Short, "Confirmed", "Rejected"));

        // ASSERT: one Confirmed + one Rejected, stock 0/0 (never negative), exactly one debit in total.
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

    /// <summary>S16 — idempotent projection: the read-side consumer crashes, retries, and converges once.</summary>
    private static async Task S16(ScenarioContext ctx)
    {
        // ARRANGE: the Web's projection consumer throws twice for this scenario's events.
        await ctx.AddFaultAsync(ServiceNames.Web, FaultPoints.WebProjection, FaultKind.Fail, "projection consumer throws", hits: 2);
        var orderId = await ctx.PlaceOrderAsync(null, DemoProducts.MouseLine(1));
        // WAIT on the write side, then on the read side.
        await ctx.WaitForOrderStateAsync(orderId, Long, "Confirmed");
        await ctx.WaitUntilAsync("read model to converge", Long, async () => await ctx.ReadModelOrderStateAsync(orderId) == "Confirmed" ? "ok" : null);
        await ctx.DelayAsync(TimeSpan.FromSeconds(2), "let the last telemetry arrive");

        // ASSERT: the failures happened (ConsumerFailed from Web) ...
        var failed = await ctx.CountAuditAsync(DiagnosticKind.ConsumerFailed, service: ServiceNames.Web);
        await ctx.AssertAsync("projection failed and was retried", failed >= 2, $"{failed} ConsumerFailed record(s) from Web");
        // ... yet every business event was applied exactly once (no duplicate message ids in the audit).
        var (rows, distinct) = await ctx.BusinessAuditRowsAsync();
        await ctx.AssertAsync("no event projected twice", rows == distinct, $"{rows} business audit rows, {distinct} distinct message ids");
        await ctx.AssertAsync("read model converged", true, "readmodel.db state = Confirmed");
    }
}
