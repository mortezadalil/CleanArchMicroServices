// ==================================================================================================
// FILE: src/MiniShop.Web/Services/DemoResetService.cs
//
// WHAT IT IS
//   The "Reset demo data" button: puts the WHOLE distributed system (four databases + the broker) back
//   to the initial seed, so a student can start a lesson from a clean, known state.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Development-only control plane of MiniShop.Web.  The Web still never opens another service's
//   database: it asks each owner to reset ITSELF through its protected /demo/reset endpoint, and the
//   owner then re-announces its seed through its own outbox as ordinary events.
//
// WHY THE STEP ORDER MATTERS (see the class comment for the list)
//   Resetting a distributed system is a small exercise in ordering: messages already in flight can
//   recreate state you just deleted.  So we first stop the trouble-makers (fault rules), then drain the
//   pipes (queues), then clear the read side, then reset the owners (whose fresh events must ARRIVE
//   after the read model was emptied), and finally wait for the read model to catch up.
//
// Read next: ServiceClients.cs (the HTTP calls used here), Projections/ReadModelBootstrapper.cs.
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Demo;
using MiniShop.Web.Data;
using MiniShop.Web.Projections;
using MiniShop.Web.Scenarios;

namespace MiniShop.Web.Services;

/// <summary>
/// What happened during a reset, shown to the user.
/// Success: overall result.  Message: one-line summary.  Steps: what was done, in order (also on failure,
/// so the user sees how far it got).
/// </summary>
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
///
/// What would go wrong with another order?  Purging before removing faults: a paused consumer would
/// still hold (and later process) the message it was stuck on.  Emptying the read model before purging:
/// queued old events would be projected again into the empty tables.  Emptying the read model AFTER
/// resetting the owners: we would delete the fresh seed events the owners just announced.
/// </summary>
public sealed class DemoResetService(ServiceClients clients, IDbContextFactory<ReadModelDbContext> factory, IBus bus,
    ScenarioRunner scenarios, UiNotifier notifier, ILogger<DemoResetService> logger)
{
    /// <summary>
    /// STATIC on purpose: the service is scoped (one instance per Blazor circuit), but two browser tabs
    /// must not reset at the same time, so the lock is shared by every instance in the process.
    /// </summary>
    private static readonly SemaphoreSlim OneAtATime = new(1, 1);

    /// <summary>Runs the five reset steps.  Never throws (except on cancellation): failures are returned in the result.</summary>
    public async Task<ResetResult> ResetAllAsync(CancellationToken ct = default)
    {
        var steps = new List<string>();
        // Refuse while a scenario runs: the reset would delete the data it is about to assert on and
        // remove the faults it relies on, and the run would end with a meaningless verdict.
        if (scenarios.RunningId is not null)
            return new ResetResult(false, "A scenario is running; wait until it finishes, then reset.", steps);
        // WaitAsync(0) = "take the lock only if it is free right now"; a double click gets a polite answer
        // instead of queueing a second reset.
        if (!await OneAtATime.WaitAsync(0, ct))
            return new ResetResult(false, "A reset is already in progress.", steps);

        try
        {
            // Step 1: remove every fault rule in every service (null = all rules, not just one scenario's).
            await clients.RemoveFaultsEverywhereAsync(null, ct);
            steps.Add("fault rules removed in every service");
            await Task.Delay(500, ct);   // let a consumer that was paused by a fault finish its message

            // Step 2: empty every minishop.* queue through the RabbitMQ management API, including the
            // *_error queues (dead letters), so nothing from before the reset is delivered afterwards.
            var purged = await clients.PurgeQueuesAsync(ct);
            steps.Add($"RabbitMQ: {purged} MiniShop queue(s) purged (including *_error)");

            // Step 3: delete every row of readmodel.db (projections, EventAudit, inbox, checkpoints,
            // scenario runs).  Rows, not the file: the app holds open connections to it.
            // The braces-less "await using (...)" disposes the context right after this one statement.
            await using (var db = await factory.CreateDbContextAsync(ct))
                await DatabaseReset.ClearAllTablesAsync(db, ct);
            steps.Add("readmodel.db emptied");

            // Step 4: ask each owner to reset ITSELF over HTTP (POST /demo/reset, token protected).  Each one
            // clears its tables, re-seeds, and publishes the seed via its outbox, which refills our read model.
            foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
            {
                await clients.ResetServiceAsync(service, ct);
                steps.Add($"{service}: tables emptied and seed restored");
            }

            // Step 5: wait (bounded) until the projections show the seed again.  Not fatal if slow: the
            // events are on their way and the page updates by itself when they land.
            var ready = await WaitForSeedInReadModelAsync(ct);
            steps.Add(ready ? "read model shows the seed again" : "read model not refilled yet (it will catch up within seconds)");

            // Refresh every open page: the storefront/orders ("readmodel") and the Scenario Lab ("scenario").
            notifier.Notify("readmodel");
            notifier.Notify("scenario");
            // Warning level on purpose: wiping all data should stand out in the logs.
            logger.LogWarning("Demo data reset: {Steps}", string.Join("; ", steps));
            return new ResetResult(true, "All data was reset to the initial seed.", steps);
        }
        // Typical failure: a service is not running, so its HTTP call throws.  Report, don't crash the page.
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Demo reset failed");
            steps.Add($"failed: {ex.Message}");
            return new ResetResult(false, $"Reset failed: {ex.Message}. Is every service running?", steps);
        }
        finally
        {
            // Always release, even on failure, or no reset could ever run again until restart.
            OneAtATime.Release();
        }
    }

    /// <summary>Wait (max ~15 s) for the seeded catalog and wallet to be projected; re-ask the owners every 2 s.</summary>
    private async Task<bool> WaitForSeedInReadModelAsync(CancellationToken ct)
    {
        // 30 attempts x 500 ms = about 15 seconds.
        for (var attempt = 0; attempt < 30; attempt++)
        {
            // A fresh context each time, so we see rows committed by the projection consumers meanwhile.
            await using (var db = await factory.CreateDbContextAsync(ct))
            {
                var products = await db.ProductCards.CountAsync(ct);
                var wallets = await db.WalletBalances.CountAsync(ct);
                // The seed has (at least) 3 products and the demo-customer wallet.
                if (products >= 3 && wallets >= 1) return true;
            }

            // Every 4th attempt (every 2 s, starting immediately) ask the owners for a snapshot, in case a
            // seed event was lost or published before a queue binding existed (same idea as the
            // ReadModelBootstrapper).  Duplicated answers are harmless: the projections are idempotent upserts.
            if (attempt % 4 == 0)
                await bus.Publish(new ReadModelSnapshotRequested(ServiceNames.Web, "demo reset") { Producer = ServiceNames.Web }, ct);
            await Task.Delay(500, ct);
        }
        return false;
    }
}
