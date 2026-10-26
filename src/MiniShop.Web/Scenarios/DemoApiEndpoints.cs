using MiniShop.Contracts;
using MiniShop.Shared.Faults;
using MiniShop.Web.Data;
using MiniShop.Web.Projections;
using MiniShop.Web.Queries;
using MiniShop.Web.Services;

namespace MiniShop.Web.Scenarios;

/// <summary>
/// HTTP face of the Scenario Lab, used by the E2E test runner (tests/MiniShop.E2E) and by scripts.
/// The Blazor pages call the same services directly.  Development-only, token protected.
/// </summary>
public static class DemoApiEndpoints
{
    public static void MapDemoApi(this RouteGroupBuilder group)
    {
        group.MapGet("/scenarios", () => Results.Ok(ScenarioCatalog.All.Select(s => new
        {
            s.Code, s.Title, s.Summary, s.Lesson, s.ExpectedTrace, s.TimeoutSeconds, s.Requirement,
        })));

        group.MapPost("/scenarios/{code}/run", async (string code, ScenarioRunner runner) =>
        {
            if (ScenarioCatalog.Find(code) is null) return Results.NotFound(new { error = $"unknown scenario {code}" });
            var run = await runner.StartAsync(code);
            return run is null ? Results.Conflict(new { error = "another scenario is running" }) : Results.Accepted($"/demo/scenarios/runs/{run.Id}", run);
        });

        group.MapGet("/scenarios/runs", async (ReadModelQueries queries, int take = 30) => Results.Ok(await queries.GetScenarioRunsAsync(take)));

        group.MapGet("/scenarios/runs/{runId:guid}", async (Guid runId, ReadModelQueries queries) =>
        {
            var (run, assertions) = await queries.GetScenarioRunAsync(runId);
            return run is null ? Results.NotFound() : Results.Ok(new { run, assertions });
        });

        group.MapPost("/scenarios/{runId:guid}/reset", async (Guid runId, ServiceClients clients) =>
        {
            await clients.RemoveFaultsEverywhereAsync(runId);
            return Results.Ok(new { faultsCleared = true });
        });

        // ---- faults (toggle by hand from the UI or scripts) ----------------------------------------
        group.MapGet("/faults", async (ServiceClients clients) =>
        {
            var all = new Dictionary<string, IReadOnlyList<FaultRule>>();
            foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments, ServiceNames.Web })
                all[service] = await clients.ListFaultsAsync(service);
            return Results.Ok(all);
        });
        group.MapPost("/faults/{service}", async (string service, FaultRule rule, ServiceClients clients) =>
            Results.Ok(await clients.AddFaultAsync(service, rule)));
        group.MapDelete("/faults/{service}/{id:guid}", async (string service, Guid id, ServiceClients clients) =>
        {
            await clients.RemoveFaultAsync(service, id);
            return Results.NoContent();
        });
        group.MapDelete("/faults", async (ServiceClients clients) =>
        {
            await clients.RemoveFaultsEverywhereAsync(null);
            return Results.NoContent();
        });

        // ---- resets -------------------------------------------------------------------------------
        group.MapPost("/readmodel/rebuild", async (ReadModelRebuilder rebuilder) => Results.Ok(new { replayed = await rebuilder.RebuildAsync(CancellationToken.None) }));

        group.MapPost("/reset-all", async (ServiceClients clients, ReadModelDbContext db, UiNotifier notifier, MassTransit.IBus bus) =>
        {
            await clients.RemoveFaultsEverywhereAsync(null);
            foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments })
                await clients.ResetServiceAsync(service);
            await db.Database.EnsureDeletedAsync();
            await db.Database.EnsureCreatedAsync();
            // the services re-seed and announce their catalog/wallets; ask once more in case we raced them
            await bus.Publish(new ReadModelSnapshotRequested(ServiceNames.Web, "demo reset") { Producer = ServiceNames.Web });
            notifier.Notify("readmodel");
            return Results.Ok(new { reset = true });
        });

        group.MapGet("/health-map", (ServiceHealthMonitor monitor) => Results.Ok(new { nodes = monitor.Nodes.Values, queues = monitor.Queues }));
    }
}
