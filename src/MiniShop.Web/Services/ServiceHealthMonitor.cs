// ==================================================================================================
// FILE: src/MiniShop.Web/Services/ServiceHealthMonitor.cs
//
// WHAT IT IS
//   A background poller that asks every service "are you ready?" (GET /health/ready) and asks the
//   RabbitMQ management API for its queues, every 3 seconds, and keeps the latest answers in memory.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Infrastructure telemetry for the Developer Console, not business logic.
//     - The Service Graph colours its nodes (up / degraded / down) from Nodes.
//     - Queue depths (Queues) feed the projection-lag card (S08 backlog) and the graph's edges.
//     - The Scenario Lab waits on it, e.g. S04: "is the broker really down / back up?".
//   /health/ready (shared/Demo/HealthEndpoints.cs) reports the service's own dependencies: its database
//   and its MassTransit bus connection.  It is a read-only HTTP call, allowed by the Web's rules.
//
// Registered in Program.cs as a singleton (so pages can read it) AND as the hosted service that runs
// this loop (same instance).
//
// Read next: ServiceClients.cs (HealthAsync and QueuesAsync, the two calls used here).
// ==================================================================================================

using MiniShop.Contracts;
using MiniShop.Web.Projections;

namespace MiniShop.Web.Services;

/// <summary>
/// Current health of one node of the service graph.
/// Service: node name.  Up: usable or not.  Status: "Healthy" / "Degraded" / "Unhealthy" / "Down".
/// CheckedAtUtc: time of the poll.  Detail: raw JSON body or the error message, shown on hover.
/// </summary>
public sealed record NodeStatus(string Service, bool Up, string Status, DateTime CheckedAtUtc, string? Detail);

/// <summary>
/// Polls /health/ready of every service and the RabbitMQ management API every few seconds and keeps
/// the latest answer in memory ("ServiceHealth" projection).  The Service Graph colours its nodes from
/// this; the Scenario Lab waits on it (S04: "is the broker really down?").
/// </summary>
public sealed class ServiceHealthMonitor(ServiceClients clients, UiNotifier notifier, ILogger<ServiceHealthMonitor> logger) : BackgroundService
{
    /// <summary>Latest status per node name.  Only touched under <see cref="_gate"/>.</summary>
    private readonly Dictionary<string, NodeStatus> _nodes = new();
    /// <summary>
    /// Lock object (.NET 9+ System.Threading.Lock).  Needed because the poller writes while many Blazor
    /// circuits read at the same time, and Dictionary is not thread-safe.
    /// </summary>
    private readonly Lock _gate = new();
    /// <summary>Latest list of MiniShop queues from RabbitMQ.  Replaced as a whole, never modified in place.</summary>
    private IReadOnlyList<QueueInfo> _queues = [];

    /// <summary>A COPY of the latest statuses, so callers can enumerate it while the poller keeps writing.</summary>
    public IReadOnlyDictionary<string, NodeStatus> Nodes { get { lock (_gate) return new Dictionary<string, NodeStatus>(_nodes); } }
    /// <summary>Latest queue list (immutable snapshot, so returning the reference is safe).</summary>
    public IReadOnlyList<QueueInfo> Queues { get { lock (_gate) return _queues; } }

    /// <summary>True when the last poll saw <paramref name="service"/> up.  False if it was never polled.</summary>
    public bool IsUp(string service) => Nodes.TryGetValue(service, out var n) && n.Up;

    /// <summary>The background loop: poll, wait 3 s, repeat until the app stops.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // A failing poll must never kill the loop (an unhandled exception would stop the hosted
            // service).  Logged at Debug only, because "service down" is an expected state in this demo.
            try { await PollAsync(stoppingToken); }
            catch (Exception ex) when (ex is not OperationCanceledException) { logger.LogDebug(ex, "health poll failed"); }
            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
        }
    }

    /// <summary>
    /// One poll of every node.  Public so the Scenario Lab can force a fresh reading instead of waiting
    /// up to 3 s for the next loop.
    /// </summary>
    public async Task PollAsync(CancellationToken ct)
    {
        // Same timestamp for every node of this poll.
        var now = DateTime.UtcNow;
        // Collect into a local list first; publish everything at once under the lock below.
        var results = new List<NodeStatus>();
        // The three business services: GET {url}/health/ready (HealthAsync never throws; it returns "Down").
        foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
        {
            var (up, status, detail) = await clients.HealthAsync(service, ct);
            results.Add(new NodeStatus(service, up, status, now, detail));
        }

        // RabbitMQ: queue list from the management API (empty list when unreachable).
        var queues = await clients.QueuesAsync(ct);
        // The broker counts as up when its management API answers; the services' bus checks say whether THEY are connected.
        results.Add(new NodeStatus(ServiceNames.RabbitMq, queues.Count > 0, queues.Count > 0 ? "Healthy" : "Down", now,
            queues.Count > 0 ? $"{queues.Count} queues" : "management API unreachable"));
        // The Web itself: if this code runs, the Web is up.
        results.Add(new NodeStatus(ServiceNames.Web, true, "Healthy", now, null));

        // Publish the new readings atomically, so a reader never sees half old / half new data.
        lock (_gate)
        {
            foreach (var r in results) _nodes[r.Service] = r;
            _queues = queues;
        }
        // Tell open pages (Service Graph, console) to re-render with the new health.
        notifier.Notify("health");
    }
}
