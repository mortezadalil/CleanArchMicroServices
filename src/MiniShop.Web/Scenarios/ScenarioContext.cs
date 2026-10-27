// =====================================================================================================
//  ScenarioContext.cs  —  the toolbox every scenario body uses
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    One ScenarioContext is created per scenario run (by ScenarioRunner) and handed to the scenario
//    body.  It offers four groups of helpers, in the same order a test is written:
//      1. setup    — set the wallet, set stock, arm fault rules              (ARRANGE)
//      2. actions  — place orders through the real public API              (ACT)
//      3. waiting  — poll DURABLE state in the owner databases              (WAIT)
//      4. evidence — read owner databases / audit records and assert        (ASSERT)
//    Every helper also writes a line to the run log, so the student can replay what happened.
//
//  Where the student sees it:
//    Developer page → Scenario Lab: the live log under a running scenario and the list of
//    PASS/FAIL assertions with their "evidence" text both come from the rows written here.
//
//  Data sources (important!):
//    * Owner state (orders.db, inventory.db, payments.db) is NEVER opened directly.  We ask each
//      owner service through its read-only diagnostics endpoints (clients.DiagnosticsAsync), so each
//      service stays the only one that touches its own database.
//    * The Web's own read model (readmodel.db: event audit, scenario runs, projected orders) is read
//      and written through ReadModelDbContext, because the Web service owns that database.
//
//  Read next: ScenarioRunner.cs (who creates this context and how the verdict is computed).
// =====================================================================================================

using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Faults;
using MiniShop.Web.Data;
using MiniShop.Web.Queries;
using MiniShop.Web.ReadModel;
using MiniShop.Web.Services;

namespace MiniShop.Web.Scenarios;

/// <summary>Known demo products (same fixed ids as the Inventory seed).</summary>
/// <remarks>
/// The ids are hard-coded on purpose: Inventory seeds exactly these products at start-up, so the
/// Web service can refer to them without querying another service's database.
/// </remarks>
public static class DemoProducts
{
    /// <summary>Headphones, price 500,000.</summary>
    public static readonly Guid Headphones = Guid.Parse("11111111-1111-1111-1111-111111111111");
    /// <summary>Keyboard, price 800,000.</summary>
    public static readonly Guid Keyboard = Guid.Parse("22222222-2222-2222-2222-222222222222");
    /// <summary>Mouse, price 300,000.  Most scenarios buy one mouse because it is cheap and well stocked.</summary>
    public static readonly Guid Mouse = Guid.Parse("33333333-3333-3333-3333-333333333333");

    // Shortcuts that build an order line (product id, name, unit price, quantity) for the API call.
    // The price travels with the order; Orders stores it as part of the order it owns.
    /// <summary>An order line for <paramref name="qty"/> headphones.</summary>
    public static OrderLine HeadphonesLine(int qty = 1) => new(Headphones, "Headphones", 500_000, qty);
    /// <summary>An order line for <paramref name="qty"/> keyboards.</summary>
    public static OrderLine KeyboardLine(int qty = 1) => new(Keyboard, "Keyboard", 800_000, qty);
    /// <summary>An order line for <paramref name="qty"/> mice.</summary>
    public static OrderLine MouseLine(int qty = 1) => new(Mouse, "Mouse", 300_000, qty);
}

/// <summary>Thrown by a scenario when a wait exceeds its deadline: the run ends as INCONCLUSIVE, never as a fake PASSED.</summary>
/// <remarks>
/// Why a dedicated exception?  "We did not see the expected state in time" is different from "we saw
/// a wrong state".  The runner catches this type specifically and reports INCONCLUSIVE, so a slow
/// laptop or a missing manual step (S04: nobody stopped the broker) is never mistaken for a bug.
/// </remarks>
public sealed class ScenarioTimeoutException(string message) : Exception(message);

/// <summary>
/// The toolbox a scenario uses: setup (wallet/stock/faults), action (place orders), waiting on
/// durable state, and evidence-based assertions read from the OWNER services' diagnostics.
/// Every call is logged into the run so the student can read what happened step by step.
/// </summary>
/// <remarks>
/// Primary-constructor parameters (C# 12):
///   run     — the ScenarioRun row this context writes its log and assertions into.
///   clients — typed HTTP clients for the other services (public API + diagnostics + fault endpoints).
///   queries — read-model queries of the Web service itself (CQRS read side).
///   factory — creates short-lived ReadModelDbContext instances; one per helper call, so that no
///             DbContext is shared between threads (DbContext is not thread-safe).
///   health  — the background ServiceHealthMonitor (is a service / RabbitMQ up? queue backlogs?).
///   ct      — cancelled when the scenario exceeds its overall TimeoutSeconds.
/// </remarks>
public sealed class ScenarioContext(ScenarioRun run, ServiceClients clients, ReadModelQueries queries,
    IDbContextFactory<ReadModelDbContext> factory, ServiceHealthMonitor health, CancellationToken ct)
{
    // In-memory copy of the log; the whole text is re-written to the database after each new line
    // so the UI (which reads the database) always shows the complete log so far.
    private readonly List<string> _log = [];
    // Every order this run created, in creation order.  _orderIds[0] is "the" order of the scenario
    // (used for the trace check and shown on the report page).
    private readonly List<Guid> _orderIds = [];

    /// <summary>The persisted run row (code, customer id, timestamps...).</summary>
    public ScenarioRun Run => run;
    /// <summary>
    /// The scenario id travels with every order (PlaceOrderRequest) and every fault rule.  Services copy
    /// it into their telemetry, so audit records and faults can be filtered to THIS run only.
    /// </summary>
    public Guid ScenarioId => run.Id;
    /// <summary>A fresh customer per run (e.g. "scn-s03-a1b2c3"), so runs never share a wallet.</summary>
    public string CustomerId => run.CustomerId!;
    /// <summary>Direct access to the HTTP clients for special steps (runner container, restarts...).</summary>
    public ServiceClients Clients => clients;
    /// <summary>Direct access to the read-model queries (read side of CQRS).</summary>
    public ReadModelQueries Queries => queries;
    /// <summary>The run's cancellation token; pass it to anything that may wait.</summary>
    public CancellationToken Ct => ct;
    /// <summary>All orders created by this run.</summary>
    public IReadOnlyList<Guid> OrderIds => _orderIds;
    /// <summary>False as soon as one assertion fails.  Assertions never throw, so the run keeps going
    /// and the student sees ALL failed invariants, not only the first one.</summary>
    public bool AllPassed { get; private set; } = true;

    // ---- logging & assertions ------------------------------------------------------------------------

    /// <summary>Adds a timestamped line to the run log and saves the full log to readmodel.db.</summary>
    public async Task LogAsync(string message)
    {
        // Millisecond timestamps let the student line the log up with the timeline view.
        var line = $"{DateTime.UtcNow:HH:mm:ss.fff}  {message}";
        _log.Add(line);
        // A new DbContext per call (from the factory) keeps this safe even if helpers run concurrently (S15).
        await using var db = await factory.CreateDbContextAsync(ct);
        // ExecuteUpdateAsync issues one UPDATE statement without loading the row first.
        await db.ScenarioRuns.Where(r => r.Id == run.Id).ExecuteUpdateAsync(s => s.SetProperty(r => r.Log, string.Join('\n', _log)), ct);
    }

    /// <summary>
    /// Records one invariant check.  <paramref name="evidence"/> says WHERE the value was read
    /// (e.g. "payments.db ledger ...") so a PASS is backed by facts, not by "the animation looked fine".
    /// </summary>
    public async Task AssertAsync(string name, bool passed, string evidence)
    {
        // &= keeps AllPassed false forever once any assertion has failed.
        AllPassed &= passed;
        await using var db = await factory.CreateDbContextAsync(ct);
        // One row per assertion: the report page lists them with their evidence.
        db.ScenarioAssertions.Add(new ScenarioAssertion { RunId = run.Id, Name = name, Passed = passed, Evidence = evidence, AtUtc = DateTime.UtcNow });
        await db.SaveChangesAsync(ct);
        // Also echo it in the log so the log alone tells the whole story.
        await LogAsync($"[{(passed ? "PASS" : "FAIL")}] {name} — {evidence}");
    }

    // ---- setup -----------------------------------------------------------------------------------------

    /// <summary>ARRANGE: asks Payments (the wallet owner) to set this run's customer balance.</summary>
    public async Task SetWalletAsync(long balance)
    {
        // Web never writes payments.db itself; it calls a Development-only endpoint of Payments.
        await clients.SetWalletAsync(CustomerId, balance, ct);
        await LogAsync($"wallet of {CustomerId} set to {balance:N0}");
    }

    /// <summary>ARRANGE: asks Inventory (the stock owner) to set the on-hand quantity of a product.</summary>
    public async Task SetStockAsync(Guid productId, int onHand)
    {
        await clients.SetStockAsync(productId, onHand, ct);
        // [..8] = first 8 characters of the Guid, enough to recognise a product in the log.
        await LogAsync($"stock of {productId.ToString()[..8]} set to onHand={onHand}");
    }

    /// <summary>
    /// ARRANGE: arms a fault rule inside one service.  The service checks its rules at a named
    /// activation point (e.g. "Payments.ChargeWallet") and then fails, delays, pauses or duplicates.
    /// </summary>
    /// <param name="service">Which service gets the rule (ServiceNames.*).</param>
    /// <param name="point">Activation point name (FaultPoints.*): the exact line of code where the fault fires.</param>
    /// <param name="kind">Fail / Delay / Pause / Duplicate.</param>
    /// <param name="description">Human text shown in the fault panel and log.</param>
    /// <param name="hits">How many times it fires; null = until removed (∞).</param>
    /// <param name="delayMs">For Delay faults: how long to wait.</param>
    public async Task AddFaultAsync(string service, string point, FaultKind kind, string description, int? hits = 1, int delayMs = 0)
    {
        var rule = new FaultRule
        {
            // Scoped to this run: only messages carrying this ScenarioId trigger the fault, so other
            // traffic (e.g. a student clicking in the shop at the same time) is not disturbed.
            ScenarioId = ScenarioId,
            TargetService = service,
            ActivationPoint = point,
            Kind = kind,
            RemainingHits = hits,
            DelayMs = delayMs,
            Description = description,
            // Safety net: even if teardown never ran (Web crashed), the rule dies by itself after 5 minutes.
            ExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
        };
        await clients.AddFaultAsync(service, rule, ct);
        await LogAsync($"fault armed in {service}: {kind} at {point} ({description}; hits={hits?.ToString() ?? "∞"})");
    }

    /// <summary>Removes this run's fault rules from every service (used mid-scenario to "fix the problem").</summary>
    public async Task ClearFaultsAsync()
    {
        await clients.RemoveFaultsEverywhereAsync(ScenarioId, ct);
        await LogAsync("scenario faults removed from every service");
    }

    // ---- actions ----------------------------------------------------------------------------------------

    /// <summary>
    /// ACT: places an order through the real public API (POST /api/orders on Orders), exactly like the
    /// storefront does.  Returns the order id from the 202 Accepted answer.
    /// </summary>
    /// <param name="idempotencyKey">
    /// Optional explicit key.  Passing the SAME key twice (S14) must return the same order.
    /// When null, a unique key is generated: code + scenario id + sequence number.
    /// </param>
    /// <param name="lines">The products to buy.</param>
    public async Task<Guid> PlaceOrderAsync(string? idempotencyKey = null, params OrderLine[] lines)
    {
        var key = idempotencyKey ?? $"{run.Code}-{ScenarioId:N}-{_orderIds.Count + 1}";
        // ScenarioId is part of the request, so Orders stamps it on the order and on all messages.
        var response = await clients.PlaceOrderAsync(new PlaceOrderRequest(CustomerId, lines, ScenarioId), key, ct);
        // A duplicate request returns an existing order id; do not count it twice.
        if (!_orderIds.Contains(response.OrderId)) _orderIds.Add(response.OrderId);
        // Persist the order ids on the run row so the UI can link to the order immediately.
        await using var db = await factory.CreateDbContextAsync(ct);
        await db.ScenarioRuns.Where(r => r.Id == run.Id).ExecuteUpdateAsync(s => s
            .SetProperty(r => r.OrderId, _orderIds[0])
            .SetProperty(r => r.OrderIdsJson, System.Text.Json.JsonSerializer.Serialize(_orderIds)), ct);
        await LogAsync($"POST /api/orders (Idempotency-Key {key}) → 202, orderId {response.OrderId}{(response.Duplicate ? " (duplicate: existing order returned)" : "")}");
        return response.OrderId;
    }

    // ---- waiting on DURABLE state (owner database), not on animations ---------------------------------
    // Why "durable"?  The UI animations and the read model are only pictures of what happened and may
    // lag.  The truth is the committed row in the owner's database, so every wait polls that.

    /// <summary>Order + saga evidence as JSON, read by Orders from orders.db (diagnostics endpoint).</summary>
    public async Task<JsonNode?> OrderEvidenceAsync(Guid orderId) => await clients.DiagnosticsAsync(ServiceNames.Orders, $"orders/{orderId}", ct);

    /// <summary>The order's business state (Pending, AwaitingPayment, Confirmed, Rejected...) from orders.db.</summary>
    public async Task<string?> OrderStateAsync(Guid orderId) => (await OrderEvidenceAsync(orderId))?["order"]?["state"]?.GetValue<string>();
    /// <summary>The saga's current state machine state (WaitingForStock, ...) from orders.db.</summary>
    public async Task<string?> SagaStateAsync(Guid orderId) => (await OrderEvidenceAsync(orderId))?["saga"]?["currentState"]?.GetValue<string>();

    /// <summary>
    /// WAIT: polls orders.db until the order reaches one of <paramref name="states"/>; returns the state seen.
    /// Throws <see cref="ScenarioTimeoutException"/> (→ INCONCLUSIVE) if it does not happen in time.
    /// </summary>
    public async Task<string> WaitForOrderStateAsync(Guid orderId, TimeSpan timeout, params string[] states)
    {
        // The probe returns the state when it matches and null otherwise; WaitUntilAsync loops on null.
        var state = await WaitUntilAsync($"order {orderId.ToString()[..8]} to be {string.Join('/', states)}", timeout,
            async () => { var s = await OrderStateAsync(orderId); return states.Contains(s) ? s : null; });
        await LogAsync($"order {orderId.ToString()[..8]} is now {state} (read from orders.db)");
        return state!;
    }

    /// <summary>Polls <paramref name="probe"/> until it returns non-null or the timeout passes (→ INCONCLUSIVE).</summary>
    /// <remarks>
    /// The generic "wait for a condition" loop used by all waits.  Polling every 500 ms is simple and
    /// good enough for a demo; the probe reads real state each time, never a cached value.
    /// </remarks>
    public async Task<T> WaitUntilAsync<T>(string what, TimeSpan timeout, Func<Task<T?>> probe) where T : class
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            T? value = null;
            try { value = await probe(); }
            catch (HttpRequestException) { /* service may be restarting: keep waiting */ }
            if (value is not null) return value;
            // ct: if the whole scenario exceeds its overall limit, this throws OperationCanceledException.
            await Task.Delay(500, ct);
        }
        throw new ScenarioTimeoutException($"timed out after {timeout.TotalSeconds:0}s waiting for {what}");
    }

    /// <summary>A deliberate pause, always with a logged reason so the student knows why we wait.</summary>
    public async Task DelayAsync(TimeSpan delay, string why)
    {
        await LogAsync($"waiting {delay.TotalSeconds:0.#}s: {why}");
        await Task.Delay(delay, ct);
    }

    /// <summary>Is the service (or RabbitMQ) currently answering its health check?</summary>
    public bool ServiceUp(string service) => health.IsUp(service);
    /// <summary>The health monitor itself (also exposes RabbitMQ queue depths, used by S05).</summary>
    public ServiceHealthMonitor Health => health;

    // ---- evidence helpers (owner databases) ---------------------------------------------------------------
    // Each helper asks the OWNER service for its own data through a read-only diagnostics endpoint.

    /// <summary>Payments ledger rows (debits / cancellations) for one order, from payments.db.</summary>
    public async Task<JsonArray> LedgerAsync(Guid orderId) =>
        (await clients.DiagnosticsAsync(ServiceNames.Payments, $"ledger?orderId={orderId}", ct))?.AsArray() ?? [];

    /// <summary>The scenario customer's current wallet balance, from payments.db.</summary>
    public async Task<long?> WalletBalanceAsync() =>
        (await clients.DiagnosticsAsync(ServiceNames.Payments, $"wallets/{CustomerId}", ct))?["balance"]?.GetValue<long>();

    /// <summary>Inventory reservation rows for one order (Reserved / Committed / Released / Rejected), from inventory.db.</summary>
    public async Task<JsonArray> ReservationsAsync(Guid orderId) =>
        (await clients.DiagnosticsAsync(ServiceNames.Inventory, $"reservations?orderId={orderId}", ct))?.AsArray() ?? [];

    /// <summary>Stock of every product as (OnHand, Reserved), from inventory.db.</summary>
    public async Task<Dictionary<Guid, (int OnHand, int Reserved)>> StockAsync()
    {
        var array = (await clients.DiagnosticsAsync(ServiceNames.Inventory, "stock", ct))?.AsArray() ?? [];
        // Turn the JSON array into a dictionary keyed by product id for easy lookups in assertions.
        return array.ToDictionary(
            n => Guid.Parse(n!["productId"]!.GetValue<string>()),
            n => (n!["onHand"]!.GetValue<int>(), n["reserved"]!.GetValue<int>()));
    }

    /// <summary>How many outbox rows of a service are still waiting to be published (-1 = unknown).</summary>
    public async Task<int> OutboxPendingAsync(string service) =>
        (await clients.DiagnosticsAsync(service, "outbox?take=1", ct))?["pending"]?.GetValue<int>() ?? -1;

    // The next helpers read the Web's OWN event audit (readmodel.db).  Every service sends telemetry
    // records (MessagePublished, MessageConsumed, InboxDuplicateIgnored, ConsumerFailed...) and the Web
    // stores them; they are evidence of HOW something happened (retries, duplicates), while the owner
    // databases are evidence of WHAT the final state is.

    /// <summary>How many audit records of a kind were recorded for this scenario (optionally one order / one service).</summary>
    public async Task<int> CountAuditAsync(string kind, Guid? orderId = null, string? service = null)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        // "orderId == null || ..." makes the filter optional inside a single translatable SQL query.
        return await db.EventAudit.CountAsync(a => a.ScenarioId == ScenarioId && a.Kind == kind
            && (orderId == null || a.CorrelationId == orderId) && (service == null || a.Service == service), ct);
    }

    /// <summary>How many DISTINCT messages of a type were published for an order (duplicates of one message count once).</summary>
    public async Task<int> CountPublishedAsync(string messageType, Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        // Distinct on the message id: a message re-published by the outbox (same id) is ONE message,
        // while a genuinely new command (e.g. a second ReleaseStock retry) has a new id.
        return await db.EventAudit.Where(a => a.CorrelationId == orderId && a.Kind == "MessagePublished" && a.SubjectMessageType == messageType)
            .Select(a => a.SubjectMessageId).Distinct().CountAsync(ct);
    }

    /// <summary>Is there an audit record of <paramref name="kind"/> whose detail text contains a fragment?</summary>
    public async Task<bool> AuditContainsAsync(string kind, string detailFragment)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        return await db.EventAudit.AnyAsync(a => a.ScenarioId == ScenarioId && a.Kind == kind && a.Detail != null && a.Detail.Contains(detailFragment), ct);
    }

    /// <summary>Business audit rows must have distinct message ids: proof that no event was projected twice.</summary>
    public async Task<(int Rows, int Distinct)> BusinessAuditRowsAsync()
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        var ids = await db.EventAudit.Where(a => a.ScenarioId == ScenarioId && a.Category == "Business").Select(a => a.MessageId).ToListAsync(ct);
        return (ids.Count, ids.Distinct().Count());
    }

    /// <summary>The order state as the READ MODEL sees it (may lag behind orders.db — that is CQRS).</summary>
    public async Task<string?> ReadModelOrderStateAsync(Guid orderId) => (await queries.GetOrderDetailsAsync(orderId))?.State;

    // ---- common invariant bundles -------------------------------------------------------------------------
    // Assertions that many scenarios share.  Each one reads the owner database and records evidence.

    /// <summary>Asserts how many COMPLETED debits Payments has for the order (0 = no money moved).</summary>
    public async Task AssertDebitsAsync(Guid orderId, int expectedCompleted)
    {
        var ledger = await LedgerAsync(orderId);
        var completed = ledger.Count(l => l!["state"]!.GetValue<string>() == "Completed");
        await AssertAsync(expectedCompleted == 0 ? "no debit" : $"exactly {expectedCompleted} debit", completed == expectedCompleted,
            $"payments.db ledger for order: {ledger.Count} row(s), {completed} Completed");
    }

    /// <summary>Asserts there is exactly one reservation for the order and it is in <paramref name="expectedState"/>.</summary>
    public async Task AssertReservationStateAsync(Guid orderId, string expectedState)
    {
        var reservations = await ReservationsAsync(orderId);
        var states = reservations.Select(r => r!["state"]!.GetValue<string>()).ToList();
        await AssertAsync($"reservation is {expectedState}", reservations.Count == 1 && states[0] == expectedState,
            $"inventory.db reservations for order: [{string.Join(", ", states)}]");
    }

    /// <summary>Asserts no reservation of the order still holds stock (nothing "leaked").</summary>
    public async Task AssertNoReservationHeldAsync(Guid orderId)
    {
        var reservations = await ReservationsAsync(orderId);
        var held = reservations.Count(r => r!["state"]!.GetValue<string>() == "Reserved");
        await AssertAsync("no dangling reservation", held == 0,
            $"inventory.db: {reservations.Count} reservation row(s), {held} still Reserved");
    }

    /// <summary>Asserts the exact OnHand / Reserved numbers of one product.</summary>
    public async Task AssertStockAsync(Guid productId, int expectedOnHand, int expectedReserved, string label)
    {
        var stock = await StockAsync();
        var (onHand, reserved) = stock[productId];
        await AssertAsync(label, onHand == expectedOnHand && reserved == expectedReserved,
            $"inventory.db: onHand={onHand}, reserved={reserved} (expected {expectedOnHand}/{expectedReserved})");
    }
}
