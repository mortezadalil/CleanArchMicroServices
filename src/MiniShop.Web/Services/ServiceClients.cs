using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Options;
using MiniShop.Contracts;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;

namespace MiniShop.Web.Services;

/// <summary>Base URLs of the other services (configuration section "Services").</summary>
public sealed class ServiceUrls
{
    public const string Section = "Services";
    public string Orders { get; set; } = "http://localhost:5101";
    public string Inventory { get; set; } = "http://localhost:5102";
    public string Payments { get; set; } = "http://localhost:5103";
    /// <summary>RabbitMQ management API (queues, overview). Not a domain dependency: infrastructure telemetry.</summary>
    public string RabbitMqManagement { get; set; } = "http://localhost:15672";
    /// <summary>Optional local runner that can stop/start containers (S04). Empty = not available.</summary>
    public string? Runner { get; set; }
    /// <summary>"Bus" (dotnet run: restart the bus in-process) or "Crash" (compose: exit and let Docker restart the container).</summary>
    public string OrdersRestartMode { get; set; } = "Bus";

    public string For(string service) => service switch
    {
        ServiceNames.Orders => Orders,
        ServiceNames.Inventory => Inventory,
        ServiceNames.Payments => Payments,
        _ => throw new ArgumentOutOfRangeException(nameof(service), service, "unknown service"),
    };
}

/// <summary>Request body for placing an order (mirrors the Orders API contract).</summary>
public sealed record PlaceOrderRequest(string CustomerId, IReadOnlyList<OrderLine> Lines, Guid? ScenarioId);
public sealed record PlaceOrderResponse(Guid OrderId, bool Duplicate);

/// <summary>
/// The ONLY HTTP calls the Web makes to other services:
///   1. POST /api/orders — the command entry point (allowed by design: it is a command gateway, not a domain-to-domain call).
///   2. Development-only control plane (/demo/*) and read-only evidence (/diagnostics/*), used by the Scenario Lab
///      and the Developer Console.  Never by the storefront.
/// </summary>
public sealed class ServiceClients(HttpClient http, IOptions<ServiceUrls> urls, IOptions<DemoOptions> demo, FaultRegistry ownFaults)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() },
    };

    public ServiceUrls Urls => urls.Value;

    // ---- command gateway -------------------------------------------------------------------------
    public async Task<PlaceOrderResponse> PlaceOrderAsync(PlaceOrderRequest request, string idempotencyKey, CancellationToken ct = default)
    {
        using var message = new HttpRequestMessage(HttpMethod.Post, $"{Urls.Orders}/api/orders")
        {
            Content = JsonContent.Create(request, options: JsonOptions),
        };
        message.Headers.Add("Idempotency-Key", idempotencyKey);
        using var response = await http.SendAsync(message, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<PlaceOrderResponse>(JsonOptions, ct))!;
    }

    // ---- health -------------------------------------------------------------------------------------
    public async Task<(bool Up, string Status, string? Detail)> HealthAsync(string service, CancellationToken ct = default)
    {
        try
        {
            using var response = await http.GetAsync($"{Urls.For(service)}/health/ready", ct);
            var body = await response.Content.ReadAsStringAsync(ct);
            var status = JsonNode.Parse(body)?["status"]?.GetValue<string>() ?? response.StatusCode.ToString();
            return (response.IsSuccessStatusCode, status, body);
        }
        catch (Exception ex)
        {
            return (false, "Down", ex.Message);
        }
    }

    // ---- fault control plane ---------------------------------------------------------------------
    public async Task<FaultRule> AddFaultAsync(string service, FaultRule rule, CancellationToken ct = default)
    {
        if (service == ServiceNames.Web)
            return ownFaults.Upsert(rule);     // the Web's own projection faults need no HTTP round-trip
        var response = await SendAsync(HttpMethod.Post, $"{Urls.For(service)}/demo/faults", rule, ct);
        return (await response.Content.ReadFromJsonAsync<FaultRule>(JsonOptions, ct))!;
    }

    public async Task<IReadOnlyList<FaultRule>> ListFaultsAsync(string service, CancellationToken ct = default)
    {
        if (service == ServiceNames.Web) return ownFaults.List();
        try
        {
            var response = await SendAsync(HttpMethod.Get, $"{Urls.For(service)}/demo/faults", null, ct);
            return (await response.Content.ReadFromJsonAsync<List<FaultRule>>(JsonOptions, ct))!;
        }
        catch (HttpRequestException) { return []; }
    }

    public async Task RemoveFaultAsync(string service, Guid id, CancellationToken ct = default)
    {
        if (service == ServiceNames.Web) { ownFaults.Remove(id); return; }
        await SendAsync(HttpMethod.Delete, $"{Urls.For(service)}/demo/faults/{id}", null, ct);
    }

    /// <summary>Teardown: remove the rules of one scenario (or all rules) in EVERY service. Failures are swallowed: a down service has no rules to remove.</summary>
    public async Task RemoveFaultsEverywhereAsync(Guid? scenarioId, CancellationToken ct = default)
    {
        if (scenarioId is null) ownFaults.Clear(); else ownFaults.RemoveByScenario(scenarioId.Value);
        foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
        {
            try
            {
                var query = scenarioId is null ? "" : $"?scenarioId={scenarioId}";
                await SendAsync(HttpMethod.Delete, $"{Urls.For(service)}/demo/faults{query}", null, ct);
            }
            catch (HttpRequestException) { /* service down: nothing to clean there */ }
        }
    }

    // ---- scenario setup helpers --------------------------------------------------------------------
    public Task SetWalletAsync(string customerId, long balance, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Payments}/demo/wallets", new { customerId, balance }, ct);

    public Task SetStockAsync(Guid productId, int onHand, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Inventory}/demo/stock", new { productId, onHand, reserved = 0 }, ct);

    public Task ResetServiceAsync(string service, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.For(service)}/demo/reset", null, ct);

    public Task CrashOrdersAsync(CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Orders}/demo/crash", null, ct);

    public Task RestartOrdersBusAsync(CancellationToken ct = default) =>
        SendAsync(HttpMethod.Post, $"{Urls.Orders}/demo/restart-bus", null, ct);

    public Task RetryCompensationAsync(Guid orderId, CancellationToken ct = default) =>
        http.PostAsync($"{Urls.Orders}/api/orders/{orderId}/retry-compensation", null, ct);

    // ---- read-only evidence from the OWNER databases ----------------------------------------------
    public async Task<JsonNode?> DiagnosticsAsync(string service, string path, CancellationToken ct = default)
    {
        var response = await SendAsync(HttpMethod.Get, $"{Urls.For(service)}/diagnostics/{path}", null, ct, throwOnError: false);
        if (!response.IsSuccessStatusCode) return null;
        return JsonNode.Parse(await response.Content.ReadAsStringAsync(ct));
    }

    // ---- RabbitMQ management (queue depth, consumers) --------------------------------------------
    public async Task<IReadOnlyList<QueueInfo>> QueuesAsync(CancellationToken ct = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{Urls.RabbitMqManagement}/api/queues");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("guest:guest")));
            using var response = await http.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode) return [];
            var nodes = JsonNode.Parse(await response.Content.ReadAsStringAsync(ct))?.AsArray() ?? [];
            return nodes.Select(q => new QueueInfo(
                    q!["name"]!.GetValue<string>(),
                    q["messages_ready"]?.GetValue<int>() ?? 0,
                    q["messages_unacknowledged"]?.GetValue<int>() ?? 0,
                    q["consumers"]?.GetValue<int>() ?? 0))
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
    public bool RunnerAvailable => !string.IsNullOrWhiteSpace(Urls.Runner);

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

public sealed record QueueInfo(string Name, int Ready, int Unacked, int Consumers)
{
    public int Backlog => Ready + Unacked;
}
