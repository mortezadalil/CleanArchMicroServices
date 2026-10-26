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
public static class DemoProducts
{
    public static readonly Guid Headphones = Guid.Parse("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Keyboard = Guid.Parse("22222222-2222-2222-2222-222222222222");
    public static readonly Guid Mouse = Guid.Parse("33333333-3333-3333-3333-333333333333");

    public static OrderLine HeadphonesLine(int qty = 1) => new(Headphones, "Headphones", 500_000, qty);
    public static OrderLine KeyboardLine(int qty = 1) => new(Keyboard, "Keyboard", 800_000, qty);
    public static OrderLine MouseLine(int qty = 1) => new(Mouse, "Mouse", 300_000, qty);
}

/// <summary>Thrown by a scenario when a wait exceeds its deadline: the run ends as INCONCLUSIVE, never as a fake PASSED.</summary>
public sealed class ScenarioTimeoutException(string message) : Exception(message);

/// <summary>
/// The toolbox a scenario uses: setup (wallet/stock/faults), action (place orders), waiting on
/// durable state, and evidence-based assertions read from the OWNER services' diagnostics.
/// Every call is logged into the run so the student can read what happened step by step.
/// </summary>
public sealed class ScenarioContext(ScenarioRun run, ServiceClients clients, ReadModelQueries queries,
    IDbContextFactory<ReadModelDbContext> factory, ServiceHealthMonitor health, CancellationToken ct)
{
    private readonly List<string> _log = [];
    private readonly List<Guid> _orderIds = [];

    public ScenarioRun Run => run;
    public Guid ScenarioId => run.Id;
    public string CustomerId => run.CustomerId!;
    public ServiceClients Clients => clients;
    public ReadModelQueries Queries => queries;
    public CancellationToken Ct => ct;
    public IReadOnlyList<Guid> OrderIds => _orderIds;
    public bool AllPassed { get; private set; } = true;

    // ---- logging & assertions ------------------------------------------------------------------------
    public async Task LogAsync(string message)
    {
        var line = $"{DateTime.UtcNow:HH:mm:ss.fff}  {message}";
        _log.Add(line);
        await using var db = await factory.CreateDbContextAsync(ct);
        await db.ScenarioRuns.Where(r => r.Id == run.Id).ExecuteUpdateAsync(s => s.SetProperty(r => r.Log, string.Join('\n', _log)), ct);
    }

    public async Task AssertAsync(string name, bool passed, string evidence)
    {
        AllPassed &= passed;
        await using var db = await factory.CreateDbContextAsync(ct);
        db.ScenarioAssertions.Add(new ScenarioAssertion { RunId = run.Id, Name = name, Passed = passed, Evidence = evidence, AtUtc = DateTime.UtcNow });
        await db.SaveChangesAsync(ct);
        await LogAsync($"[{(passed ? "PASS" : "FAIL")}] {name} — {evidence}");
    }

    // ---- setup -----------------------------------------------------------------------------------------
    public async Task SetWalletAsync(long balance)
    {
        await clients.SetWalletAsync(CustomerId, balance, ct);
        await LogAsync($"wallet of {CustomerId} set to {balance:N0}");
    }

    public async Task SetStockAsync(Guid productId, int onHand)
    {
        await clients.SetStockAsync(productId, onHand, ct);
        await LogAsync($"stock of {productId.ToString()[..8]} set to onHand={onHand}");
    }

    public async Task AddFaultAsync(string service, string point, FaultKind kind, string description, int? hits = 1, int delayMs = 0)
    {
        var rule = new FaultRule
        {
            ScenarioId = ScenarioId,
            TargetService = service,
            ActivationPoint = point,
            Kind = kind,
            RemainingHits = hits,
            DelayMs = delayMs,
            Description = description,
            ExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
        };
        await clients.AddFaultAsync(service, rule, ct);
        await LogAsync($"fault armed in {service}: {kind} at {point} ({description}; hits={hits?.ToString() ?? "∞"})");
    }

    public async Task ClearFaultsAsync()
    {
        await clients.RemoveFaultsEverywhereAsync(ScenarioId, ct);
        await LogAsync("scenario faults removed from every service");
    }

    // ---- actions ----------------------------------------------------------------------------------------
    public async Task<Guid> PlaceOrderAsync(string? idempotencyKey = null, params OrderLine[] lines)
    {
        var key = idempotencyKey ?? $"{run.Code}-{ScenarioId:N}-{_orderIds.Count + 1}";
        var response = await clients.PlaceOrderAsync(new PlaceOrderRequest(CustomerId, lines, ScenarioId), key, ct);
        if (!_orderIds.Contains(response.OrderId)) _orderIds.Add(response.OrderId);
        await using var db = await factory.CreateDbContextAsync(ct);
        await db.ScenarioRuns.Where(r => r.Id == run.Id).ExecuteUpdateAsync(s => s
            .SetProperty(r => r.OrderId, _orderIds[0])
            .SetProperty(r => r.OrderIdsJson, System.Text.Json.JsonSerializer.Serialize(_orderIds)), ct);
        await LogAsync($"POST /api/orders (Idempotency-Key {key}) → 202, orderId {response.OrderId}{(response.Duplicate ? " (duplicate: existing order returned)" : "")}");
        return response.OrderId;
    }

    // ---- waiting on DURABLE state (owner database), not on animations ---------------------------------
    public async Task<JsonNode?> OrderEvidenceAsync(Guid orderId) => await clients.DiagnosticsAsync(ServiceNames.Orders, $"orders/{orderId}", ct);

    public async Task<string?> OrderStateAsync(Guid orderId) => (await OrderEvidenceAsync(orderId))?["order"]?["state"]?.GetValue<string>();
    public async Task<string?> SagaStateAsync(Guid orderId) => (await OrderEvidenceAsync(orderId))?["saga"]?["currentState"]?.GetValue<string>();

    public async Task<string> WaitForOrderStateAsync(Guid orderId, TimeSpan timeout, params string[] states)
    {
        var state = await WaitUntilAsync($"order {orderId.ToString()[..8]} to be {string.Join('/', states)}", timeout,
            async () => { var s = await OrderStateAsync(orderId); return states.Contains(s) ? s : null; });
        await LogAsync($"order {orderId.ToString()[..8]} is now {state} (read from orders.db)");
        return state!;
    }

    /// <summary>Polls <paramref name="probe"/> until it returns non-null or the timeout passes (→ INCONCLUSIVE).</summary>
    public async Task<T> WaitUntilAsync<T>(string what, TimeSpan timeout, Func<Task<T?>> probe) where T : class
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            T? value = null;
            try { value = await probe(); }
            catch (HttpRequestException) { /* service may be restarting: keep waiting */ }
            if (value is not null) return value;
            await Task.Delay(500, ct);
        }
        throw new ScenarioTimeoutException($"timed out after {timeout.TotalSeconds:0}s waiting for {what}");
    }

    public async Task DelayAsync(TimeSpan delay, string why)
    {
        await LogAsync($"waiting {delay.TotalSeconds:0.#}s: {why}");
        await Task.Delay(delay, ct);
    }

    public bool ServiceUp(string service) => health.IsUp(service);
    public ServiceHealthMonitor Health => health;

    // ---- evidence helpers (owner databases) ---------------------------------------------------------------
    public async Task<JsonArray> LedgerAsync(Guid orderId) =>
        (await clients.DiagnosticsAsync(ServiceNames.Payments, $"ledger?orderId={orderId}", ct))?.AsArray() ?? [];

    public async Task<long?> WalletBalanceAsync() =>
        (await clients.DiagnosticsAsync(ServiceNames.Payments, $"wallets/{CustomerId}", ct))?["balance"]?.GetValue<long>();

    public async Task<JsonArray> ReservationsAsync(Guid orderId) =>
        (await clients.DiagnosticsAsync(ServiceNames.Inventory, $"reservations?orderId={orderId}", ct))?.AsArray() ?? [];

    public async Task<Dictionary<Guid, (int OnHand, int Reserved)>> StockAsync()
    {
        var array = (await clients.DiagnosticsAsync(ServiceNames.Inventory, "stock", ct))?.AsArray() ?? [];
        return array.ToDictionary(
            n => Guid.Parse(n!["productId"]!.GetValue<string>()),
            n => (n!["onHand"]!.GetValue<int>(), n["reserved"]!.GetValue<int>()));
    }

    public async Task<int> OutboxPendingAsync(string service) =>
        (await clients.DiagnosticsAsync(service, "outbox?take=1", ct))?["pending"]?.GetValue<int>() ?? -1;

    /// <summary>How many audit records of a kind were recorded for this scenario (optionally one order / one service).</summary>
    public async Task<int> CountAuditAsync(string kind, Guid? orderId = null, string? service = null)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        return await db.EventAudit.CountAsync(a => a.ScenarioId == ScenarioId && a.Kind == kind
            && (orderId == null || a.CorrelationId == orderId) && (service == null || a.Service == service), ct);
    }

    /// <summary>How many DISTINCT messages of a type were published for an order (duplicates of one message count once).</summary>
    public async Task<int> CountPublishedAsync(string messageType, Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        return await db.EventAudit.Where(a => a.CorrelationId == orderId && a.Kind == "MessagePublished" && a.SubjectMessageType == messageType)
            .Select(a => a.SubjectMessageId).Distinct().CountAsync(ct);
    }

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

    public async Task<string?> ReadModelOrderStateAsync(Guid orderId) => (await queries.GetOrderDetailsAsync(orderId))?.State;

    // ---- common invariant bundles -------------------------------------------------------------------------
    public async Task AssertDebitsAsync(Guid orderId, int expectedCompleted)
    {
        var ledger = await LedgerAsync(orderId);
        var completed = ledger.Count(l => l!["state"]!.GetValue<string>() == "Completed");
        await AssertAsync(expectedCompleted == 0 ? "no debit" : $"exactly {expectedCompleted} debit", completed == expectedCompleted,
            $"payments.db ledger for order: {ledger.Count} row(s), {completed} Completed");
    }

    public async Task AssertReservationStateAsync(Guid orderId, string expectedState)
    {
        var reservations = await ReservationsAsync(orderId);
        var states = reservations.Select(r => r!["state"]!.GetValue<string>()).ToList();
        await AssertAsync($"reservation is {expectedState}", reservations.Count == 1 && states[0] == expectedState,
            $"inventory.db reservations for order: [{string.Join(", ", states)}]");
    }

    public async Task AssertNoReservationHeldAsync(Guid orderId)
    {
        var reservations = await ReservationsAsync(orderId);
        var held = reservations.Count(r => r!["state"]!.GetValue<string>() == "Reserved");
        await AssertAsync("no dangling reservation", held == 0,
            $"inventory.db: {reservations.Count} reservation row(s), {held} still Reserved");
    }

    public async Task AssertStockAsync(Guid productId, int expectedOnHand, int expectedReserved, string label)
    {
        var stock = await StockAsync();
        var (onHand, reserved) = stock[productId];
        await AssertAsync(label, onHand == expectedOnHand && reserved == expectedReserved,
            $"inventory.db: onHand={onHand}, reserved={reserved} (expected {expectedOnHand}/{expectedReserved})");
    }
}
