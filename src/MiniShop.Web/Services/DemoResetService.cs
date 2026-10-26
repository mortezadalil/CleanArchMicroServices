using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Demo;
using MiniShop.Web.Data;
using MiniShop.Web.Projections;
using MiniShop.Web.Scenarios;

namespace MiniShop.Web.Services;

/// <summary>What happened during a reset, shown to the user.</summary>
public sealed record ResetResult(bool Success, string Message, IReadOnlyList<string> Steps);

/// <summary>
/// "Reset demo data": brings the WHOLE system back to the seed, as if it had just been installed.
///
/// Order matters:
///   1. remove every fault rule        → a paused/failing consumer is released first
///   2. purge every MiniShop queue     → no old message (or *_error message) is delivered after the reset
///   3. empty the read model           → orders, timeline, scenario runs, projections are gone
///   4. reset Orders/Inventory/Payments → each service empties its tables and re-seeds
///      (catalog with 10 units each, demo-customer wallet with 2,000,000) and announces it via its outbox
///   5. wait until the read model shows the seed again (asking the owners for a snapshot if needed)
/// Development only, like the rest of the control plane.
/// </summary>
public sealed class DemoResetService(ServiceClients clients, IDbContextFactory<ReadModelDbContext> factory, IBus bus,
    ScenarioRunner scenarios, UiNotifier notifier, ILogger<DemoResetService> logger)
{
    private static readonly SemaphoreSlim OneAtATime = new(1, 1);

    public async Task<ResetResult> ResetAllAsync(CancellationToken ct = default)
    {
        var steps = new List<string>();
        if (scenarios.RunningId is not null)
            return new ResetResult(false, "A scenario is running; wait until it finishes, then reset.", steps);
        if (!await OneAtATime.WaitAsync(0, ct))
            return new ResetResult(false, "A reset is already in progress.", steps);

        try
        {
            await clients.RemoveFaultsEverywhereAsync(null, ct);
            steps.Add("fault rules removed in every service");
            await Task.Delay(500, ct);   // let a consumer that was paused by a fault finish its message

            var purged = await clients.PurgeQueuesAsync(ct);
            steps.Add($"RabbitMQ: {purged} MiniShop queue(s) purged (including *_error)");

            await using (var db = await factory.CreateDbContextAsync(ct))
                await DatabaseReset.ClearAllTablesAsync(db, ct);
            steps.Add("readmodel.db emptied");

            foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
            {
                await clients.ResetServiceAsync(service, ct);
                steps.Add($"{service}: tables emptied and seed restored");
            }

            var ready = await WaitForSeedInReadModelAsync(ct);
            steps.Add(ready ? "read model shows the seed again" : "read model not refilled yet (it will catch up within seconds)");

            notifier.Notify("readmodel");
            notifier.Notify("scenario");
            logger.LogWarning("Demo data reset: {Steps}", string.Join("; ", steps));
            return new ResetResult(true, "All data was reset to the initial seed.", steps);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Demo reset failed");
            steps.Add($"failed: {ex.Message}");
            return new ResetResult(false, $"Reset failed: {ex.Message}. Is every service running?", steps);
        }
        finally
        {
            OneAtATime.Release();
        }
    }

    /// <summary>Wait (max ~15 s) for the seeded catalog and wallet to be projected; re-ask the owners every 2 s.</summary>
    private async Task<bool> WaitForSeedInReadModelAsync(CancellationToken ct)
    {
        for (var attempt = 0; attempt < 30; attempt++)
        {
            await using (var db = await factory.CreateDbContextAsync(ct))
            {
                var products = await db.ProductCards.CountAsync(ct);
                var wallets = await db.WalletBalances.CountAsync(ct);
                if (products >= 3 && wallets >= 1) return true;
            }

            if (attempt % 4 == 0)
                await bus.Publish(new ReadModelSnapshotRequested(ServiceNames.Web, "demo reset") { Producer = ServiceNames.Web }, ct);
            await Task.Delay(500, ct);
        }
        return false;
    }
}
