// ==================================================================================================
// FILE: src/MiniShop.Web/Services/ServiceClients.cs
//
// WHAT IT IS
//   Every HTTP call MiniShop.Web makes to other processes, gathered in one typed HttpClient, so you can
//   check in one place that the Web obeys the architecture rules.
//
// THE RULES IT ENFORCES
//   Services cooperate through MESSAGES (RabbitMQ), not by calling each other or sharing databases.
//   The Web reads only its own readmodel.db.  The ONLY HTTP calls allowed are:
//     1. POST /api/orders on Orders: the COMMAND gateway.  A user action has to enter the system
//        somewhere; this is "the front door", not a service-to-service domain call.
//     2. Development-only control plane (/demo/*: faults, reset, set stock/wallet, crash/restart) and
//        read-only evidence (/diagnostics/*), used by the Scenario Lab and the Developer Console, never by
//        the storefront.  All carry the X-Demo-Token header and do not exist when Demo:Enabled is false.
//     3. Infrastructure telemetry: /health/ready of each service and the RabbitMQ management API.
//   Note what is NOT here: no "GET product from Inventory", no "GET order from Orders" for the UI.
//   The storefront reads those from the read model (Queries/ReadModelQueries.cs).
//
// Registered in Program.cs with AddHttpClient<ServiceClients> (10 s timeout).
//
// Read next: Cart/CartService.cs (where the Idempotency-Key for PlaceOrderAsync comes from).
// ==================================================================================================

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using MiniShop.Contracts;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;

namespace MiniShop.Web.Services;

/// <summary>
/// Base URLs of the other services (configuration section "Services").
/// Defaults match "dotnet run" on one machine; docker-compose overrides them with environment
/// variables such as Services__Orders=http://orders:8080.
/// </summary>
public sealed class ServiceUrls
{
    /// <summary>Name of the configuration section bound to this class in Program.cs.</summary>
    public const string Section = "Services";
    /// <summary>Orders API (command gateway + its control plane).</summary>
    public string Orders { get; set; } = "http://localhost:5101";
    /// <summary>Inventory API (health + control plane only).</summary>
    public string Inventory { get; set; } = "http://localhost:5102";
    /// <summary>Payments API (health + control plane only).</summary>
    public string Payments { get; set; } = "http://localhost:5103";
    /// <summary>RabbitMQ management API (queues, overview). Not a domain dependency: infrastructure telemetry.</summary>
    public string RabbitMqManagement { get; set; } = "http://localhost:15672";
    /// <summary>Optional local runner that can stop/start containers (S04). Empty = not available.</summary>
    public string? Runner { get; set; }
    /// <summary>"Bus" (dotnet run: restart the bus in-process) or "Crash" (compose: exit and let Docker restart the container).</summary>
    public string OrdersRestartMode { get; set; } = "Bus";

    /// <summary>Base URL of a service by name.  Throws for any other name (e.g. "Web" has no URL here).</summary>
    public string For(string service) => service switch
    {
        ServiceNames.Orders => Orders,
        ServiceNames.Inventory => Inventory,
        ServiceNames.Payments => Payments,
        _ => throw new ArgumentOutOfRangeException(nameof(service), service, "unknown service"),
    };
}

/// <summary>
/// Request body for placing an order (mirrors the Orders API contract).
/// CustomerId: who orders.  Lines: what (product, name, unit price, quantity).  ScenarioId: set by the
/// Scenario Lab so every message of the run can be traced; null for a normal storefront checkout.
/// </summary>
public sealed record PlaceOrderRequest(string CustomerId, IReadOnlyList<OrderLine> Lines, Guid? ScenarioId);
/// <summary>
/// Answer of POST /api/orders.  OrderId: the order (new, or the existing one).  Duplicate: true when the
/// Idempotency-Key had already been used, so Orders returned the ORIGINAL order instead of creating a
/// second one (S14).
/// </summary>
public sealed record PlaceOrderResponse(Guid OrderId, bool Duplicate);

/// <summary>
/// The ONLY HTTP calls the Web makes to other services:
///   1. POST /api/orders — the command entry point (allowed by design: it is a command gateway, not a domain-to-domain call).
///   2. Development-only control plane (/demo/*) and read-only evidence (/diagnostics/*), used by the Scenario Lab
///      and the Developer Console.  Never by the storefront.
/// (Plus health polling and the RabbitMQ management API, which are infrastructure telemetry.)
/// </summary>
public sealed class ServiceClients(HttpClient http, IOptions<ServiceUrls> urls, IOptions<DemoOptions> demo, FaultRegistry ownFaults)
{
    /// <summary>
    /// JSON settings for every request/response: camelCase names (the "Web" defaults) and enums as text,
    /// matching what the services' APIs send (see AddMiniShopBuildingBlocks).  Static: build it once.
    /// </summary>
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() },
    };

    /// <summary>The configured base URLs (read by the Scenario Lab, e.g. OrdersRestartMode).</summary>
    public ServiceUrls Urls => urls.Value;

    // ---- command gateway -------------------------------------------------------------------------
    /// <summary>
    /// Checkout: POST /api/orders with an Idempotency-Key header.  Orders stores the key with the order;
    /// if the same key arrives again (double click, browser retry, S14) it returns the first order with
    /// Duplicate = true instead of creating a second one.  The HTTP answer only means "accepted"; the order
    /// result (confirmed/rejected) arrives later as events through the read model.
    /// </summary>
    public async Task<PlaceOrderResponse> PlaceOrderAsync(PlaceOrderRequest request, string idempotencyKey, CancellationToken ct = default)
    {
        // Build the request by hand (not PostAsJsonAsync) because we need to add a custom header.
        using var message = new HttpRequestMessage(HttpMethod.Post, $"{Urls.Orders}/api/orders")
        {
            Content = JsonContent.Create(request, options: JsonOptions),
        };
        // The key is created by CartService once per cart, so every retry of THIS checkout reuses it.
        message.Headers.Add("Idempotency-Key", idempotencyKey);
        using var response = await http.SendAsync(message, ct);
        // 4xx/5xx → exception, so the UI shows an error instead of pretending the order was placed.
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<PlaceOrderResponse>(JsonOptions, ct))!;
    }

    // ---- health -------------------------------------------------------------------------------------
    /// <summary>
    /// GET {service}/health/ready.  Never throws: an unreachable service is reported as "Down" with the
    /// error text, because "down" is a normal, expected answer for the Service Graph.
    /// </summary>
    public async Task<(bool Up, string Status, string? Detail)> HealthAsync(string service, CancellationToken ct = default)
    {
        try
        {
            using var response = await http.GetAsync($"{Urls.For(service)}/health/ready", ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            // Prefer the "status" field of the JSON body (Healthy / Degraded / Unhealthy); fall back to the
            // HTTP status code if the body is not the expected JSON.
            var status = JsonNode.Parse(body)?["status"]?.GetValue<string>() ?? response.StatusCode.ToString();
            // /health/ready answers 200 for Healthy and Degraded, 503 for Unhealthy.
            return (response.IsSuccessStatusCode, status, body);
        }
        catch (Exception ex)
        {
            // Connection refused, timeout, invalid JSON...
            return (false, "Down", ex.Message);
        }
    }

    // ---- fault control plane ---------------------------------------------------------------------
    /// <summary>Adds (or replaces) a fault rule in a service and returns it as stored.</summary>
    public async Task<FaultRule> AddFaultAsync(string service, FaultRule rule, CancellationToken ct = default)
    {
        if (service == ServiceNames.Web)
            return ownFaults.Upsert(rule);     // the Web's own projection faults need no HTTP round-trip
        var response = await SendAsync(HttpMethod.Post, $"{Urls.For(service)}/demo/faults", rule, ct);
        return (await response.Content.ReadFromJsonAsync<FaultRule>(JsonOptions, ct))!;
    }

    /// <summary>Lists the active fault rules of a service; an unreachable service shows as "no rules".</summary>
    public async Task<IReadOnlyList<FaultRule>> ListFaultsAsync(string service, CancellationToken ct = default)
    {
        // The Web's own rules live in this process.
        if (service == ServiceNames.Web) return ownFaults.List();
        try
        {
            var response = await SendAsync(HttpMethod.Get, $"{Urls.For(service)}/demo/faults", null, ct);
            return (await response.Content.ReadFromJsonAsync<List<FaultRule>>(JsonOptions, ct))!;
        }
        // The fault panel must keep working while a service is down (S04, S09).
        catch (HttpRequestException) { return []; }
    }

    /// <summary>Removes one fault rule by id.</summary>
    public async Task RemoveFaultAsync(string service, Guid id, CancellationToken ct = default)
    {
        if (service == ServiceNames.Web) { ownFaults.Remove(id); return; }
        await SendAsync(HttpMethod.Delete, $"{Urls.For(service)}/demo/faults/{id}", null, ct);
    }

    /// <summary>Teardown: remove the rules of one scenario (or all rules) in EVERY service. Failures are swallowed: a down service has no rules to remove.</summary>
    public async Task RemoveFaultsEverywhereAsync(Guid? scenarioId, CancellationToken ct = default)
    {
        // The Web's own registry first: null = everything, otherwise only that scenario's rules.
        if (scenarioId is null) ownFaults.Clear(); else ownFaults.RemoveByScenario(scenarioId.Value);
        foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
        {
            try
            {
                // DELETE /demo/faults removes all; ?scenarioId=... only that run's rules, so a teardown never
                // removes rules another person added by hand.
                var query = scenarioId is null ? "" : $"?scenarioId={scenarioId}";
                await SendAsync(HttpMethod.Delete, $"{Urls.For(service)}/demo/faults{query}", null, ct);
            }
            catch (HttpRequestException) { /* service down: nothing to clean there */ }
        }
    }

    // ---- scenario setup helpers --------------------------------------------------------------------
    // These ask the OWNER to change its own data (Payments owns wallets, Inventory owns stock).  The owner
    // then publishes the usual WalletBalanceChanged / ProductCatalogChanged, so the read model follows.

    /// <summary>Sets a customer's wallet balance in Payments (e.g. "not enough money" scenarios).</summary>
    public Task SetWalletAsync(string customerId, long balance, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Payments}/demo/wallets", new { customerId, balance }, ct);

    /// <summary>Sets a product's stock in Inventory, with no reservations (e.g. "out of stock" scenarios).</summary>
    public Task SetStockAsync(Guid productId, int onHand, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Inventory}/demo/stock", new { productId, onHand, reserved = 0 }, ct);

    /// <summary>Asks a service to empty its tables and restore its seed (used by DemoResetService).</summary>
    public Task ResetServiceAsync(string service, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.For(service)}/demo/reset", null, ct);

    /// <summary>Makes the Orders process exit (Docker restarts it): a real crash, used with OrdersRestartMode "Crash".</summary>
    public Task CrashOrdersAsync(CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Orders}/demo/crash", null, ct);

    /// <summary>Stops and restarts only the Orders bus in-process: the "dotnet run" equivalent of a crash.</summary>
    public Task RestartOrdersBusAsync(CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Orders}/demo/restart-bus", null, ct);

    /// <summary>
    /// Operator action for an order in RequiresAttention: ask the Orders saga to try its compensation
    /// again.  A normal API call (no demo token); the status code is not checked.
    /// </summary>
    public Task RetryCompensationAsync(Guid orderId, CancellationToken ct = default) =>
        http.PostAsync($"{Urls.Orders}/api/orders/{orderId}/retry-compensation", null, ct);

    // ---- read-only evidence from the OWNER databases ----------------------------------------------
    /// <summary>
    /// GET {service}/diagnostics/{path}: read-only JSON straight from the owner (e.g. its outbox, inbox,
    /// orders, stock, wallets).  The Scenario Lab uses it as EVIDENCE for its assertions ("exactly one
    /// debit"), because the read model is only a copy and could itself be wrong.  The Web never writes
    /// through these endpoints and never opens the owner database.  Returns null on any HTTP error.
    /// </summary>
    public async Task<JsonNode?> DiagnosticsAsync(string service, string path, CancellationToken ct = default)
    {
        var response = await SendAsync(HttpMethod.Get, $"{Urls.For(service)}/diagnostics/{path}", null, ct, throwOnError: false);
        if (!response.IsSuccessStatusCode) return null;
        // JsonNode: the shape differs per endpoint, so we keep it dynamic instead of declaring DTOs.
        return JsonNode.Parse(await response.Content.ReadAsStringAsync(ct));
    }

    // ---- RabbitMQ management (queue depth, consumers) --------------------------------------------
    /// <summary>
    /// GET /api/queues of the RabbitMQ management plugin, filtered to MiniShop queues.  Shows backlogs
    /// (S08), consumers (is a service connected?) and error queues.  Returns an empty list on any failure;
    /// ServiceHealthMonitor treats an empty list as "broker down".
    /// </summary>
    public async Task<IReadOnlyList<QueueInfo>> QueuesAsync(CancellationToken ct = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{Urls.RabbitMqManagement}/api/queues");
            // The management API uses HTTP Basic auth; guest:guest is the local demo account.
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest")));
            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode) return [];
            // The answer is a JSON array with one object per queue.
            var nodes = JsonNode.Parse(await response.Content.ReadAsStringAsync(ct))?.AsArray() ?? [];
            return nodes.Select(q => new QueueInfo(
                    q!["name"]!.GetValue<string>(),
                    // Ready: waiting to be delivered.  Unacknowledged: delivered, being processed.
                    q["messages_ready"]?.GetValue<int>() ?? 0,
                    q["messages_unacknowledged"]?.GetValue<int>() ?? 0,
                    q["consumers"]?.GetValue<int>() ?? 0))
                // Ignore queues of other applications on the same broker.
                .Where(q => q.Name.StartsWith("minishop"))
                .OrderBy(q => q.Name)
                .ToList();
        }
        catch (Exception) { return []; }
    }

    /// <summary>Removes every message from all MiniShop queues (including *_error). Returns how many queues were purged.</summary>
    public async Task<int> PurgeQueuesAsync(CancellationToken ct = default)
    {
        var purged = 0;
        foreach (var queue in await QueuesAsync(ct))
        {
            // DELETE /api/queues/{vhost}/{name}/contents empties the queue but keeps it (and its bindings).
            // %2F is the URL-encoded default virtual host "/".
            using var request = new HttpRequestMessage(HttpMethod.Delete,
                $"{Urls.RabbitMqManagement}/api/queues/%2F/{Uri.EscapeDataString(queue.Name)}/contents");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest")));
            using var response = await http.SendAsync(request, ct);
            if (response.IsSuccessStatusCode) purged++;
        }
        return purged;
    }

    // ---- optional local runner (S04) -----------------------------------------------------------------
    /// <summary>True when a runner URL is configured (compose "faults" profile).</summary>
    public bool RunnerAvailable => !string.IsNullOrWhiteSpace(Urls.Runner);

    /// <summary>
    /// Asks the runner to perform <paramref name="action"/> (e.g. stop/start) on a container, e.g. to take
    /// RabbitMQ down for S04.  False when no runner is configured or the call fails.
    /// </summary>
    public async Task<bool> RunnerAsync(string action, string container, CancellationToken ct = default)
    {
        if (!RunnerAvailable) return false;
        try
        {
            using var response = await http.PostAsync($"{Urls.Runner!.TrimEnd('/')}/{action}/{container}", null, ct);
            return response.IsSuccessStatusCode;
        }
        catch (Exception) { return false; }
    }

    /// <summary>
    /// Common helper for control-plane calls: adds the X-Demo-Token header (without it the /demo and
    /// /diagnostics endpoints answer 401), serialises the optional body, and by default throws on an
    /// error status.  The caller disposes the returned response (or lets it be collected).
    /// </summary>
    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, object? body, CancellationToken ct, bool throwOnError = true)
    {
        using var request = new HttpRequestMessage(method, url);
        request.Headers.Add(DemoEndpoints.TokenHeader, demo.Value.Token);
        if (body is not null)
            request.Content = JsonContent.Create(body, options: JsonOptions);
        var response = await http.SendAsync(request, ct);
        if (throwOnError) response.EnsureSuccessStatusCode();
        return response;
    }
}

/// <summary>
/// One RabbitMQ queue as reported by the management API.
/// Ready: messages waiting.  Unacked: delivered to a consumer but not yet acknowledged (in progress).
/// Consumers: how many consumers are attached (0 = the owning service is down or disconnected).
/// </summary>
public sealed record QueueInfo(string Name, int Ready, int Unacked, int Consumers)
{
    /// <summary>Everything not finished yet: waiting + in progress.  This is the projection backlog in S08.</summary>
    public int Backlog => Ready + Unacked;
}
