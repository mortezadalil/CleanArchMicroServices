using MiniShop.Contracts;
using MiniShop.Web.Projections;

namespace MiniShop.Web.Services;

/// <summary>Current health of one node of the service graph.</summary>
public sealed record NodeStatus(string Service, bool Up, string Status, DateTime CheckedAtUtc, string? Detail);

/// <summary>
/// Polls /health/ready of every service and the RabbitMQ management API every few seconds and keeps
/// the latest answer in memory ("ServiceHealth" projection).  The Service Graph colours its nodes from
/// this; the Scenario Lab waits on it (S04: "is the broker really down?").
/// </summary>
public sealed class ServiceHealthMonitor(ServiceClients clients, UiNotifier notifier, ILogger<ServiceHealthMonitor> logger) : BackgroundService
{
    private readonly Dictionary<string, NodeStatus> _nodes = new();
    private readonly Lock _gate = new();
    private IReadOnlyList<QueueInfo> _queues = [];

    public IReadOnlyDictionary<string, NodeStatus> Nodes { get { lock (_gate) return new Dictionary<string, NodeStatus>(_nodes); } }
    public IReadOnlyList<QueueInfo> Queues { get { lock (_gate) return _queues; } }

    public bool IsUp(string service) => Nodes.TryGetValue(service, out var n) && n.Up;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await PollAsync(stoppingToken); }
            catch (Exception ex) when (ex is not OperationCanceledException) { logger.LogDebug(ex, "health poll failed"); }
            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
        }
    }

    public async Task PollAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var results = new List<NodeStatus>();
        foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
        {
            var (up, status, detail) = await clients.HealthAsync(service, ct);
            results.Add(new NodeStatus(service, up, status, now, detail));
        }

        var queues = await clients.QueuesAsync(ct);
        // The broker counts as up when its management API answers; the services' bus checks say whether THEY are connected.
        results.Add(new NodeStatus(ServiceNames.RabbitMq, queues.Count > 0, queues.Count > 0 ? "Healthy" : "Down", now,
            queues.Count > 0 ? $"{queues.Count} queues" : "management API unreachable"));
        results.Add(new NodeStatus(ServiceNames.Web, true, "Healthy", now, null));

        lock (_gate)
        {
            foreach (var r in results) _nodes[r.Service] = r;
            _queues = queues;
        }
        notifier.Notify("health");
    }
}
