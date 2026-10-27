// =====================================================================================================
//  DemoApiEndpoints.cs  —  the HTTP "remote control" of the Scenario Lab
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    Minimal-API endpoints under /demo that let a script or the automated E2E tests do over HTTP what
//    a student does by clicking on the Developer page: list scenarios, run one, read the verdict,
//    toggle faults, rebuild the read model, reset everything.
//
//  Where the student sees it:
//    Nowhere on screen: the Blazor pages call the same services (ScenarioRunner, ServiceClients...)
//    directly, in-process.  This file is only the HTTP face of those services.
//
//  Safety (why it cannot hurt production):
//    * Program.cs only maps this group when Demo:Enabled = true, which only appsettings.Development.json
//      sets.  In Production the routes do not exist at all (404).
//    * Every endpoint of the group requires the X-Demo-Token header (checked by MapDemoGroup).
//
//  Data sources:
//    The Web's own read model (scenario runs, audit) and the OTHER services through their own
//    Development-only HTTP endpoints (faults, diagnostics).  Never another service's database.
//
//  Read next: ScenarioRunner.cs (what "run" actually does), then tests/MiniShop.E2E for a caller.
// =====================================================================================================

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
    /// <summary>
    /// Adds the Scenario Lab routes to an existing route group (the "/demo" group created in Program.cs,
    /// which already carries the Development-only + token protection).  Being an extension method on
    /// RouteGroupBuilder means every route below inherits that protection automatically.
    /// </summary>
    public static void MapDemoApi(this RouteGroupBuilder group)
    {
        // GET /demo/scenarios — the catalogue.  We project to an anonymous object because the
        // definition's Body (a delegate) cannot and should not be serialized to JSON.
        group.MapGet("/scenarios", () => Results.Ok(ScenarioCatalog.All.Select(s => new
        {
            s.Code, s.Title, s.Summary, s.Lesson, s.ExpectedTrace, s.TimeoutSeconds, s.Requirement,
        })));

        // POST /demo/scenarios/{code}/run — start a scenario in the background.
        //   404 unknown code, 409 if another scenario is running (one at a time!),
        //   202 Accepted + Location of the run resource otherwise.  The caller then polls that URL:
        //   the scenario itself may take minutes, so the request must not block until it ends.
        group.MapPost("/scenarios/{code}/run", async (string code, ScenarioRunner runner) =>
        {
            if (ScenarioCatalog.Find(code) is null) return Results.NotFound(new { error = $"unknown scenario {code}" });
            var run = await runner.StartAsync(code);
            return run is null ? Results.Conflict(new { error = "another scenario is running" }) : Results.Accepted($"/demo/scenarios/runs/{run.Id}", run);
        });

        // GET /demo/scenarios/runs?take=30 — most recent runs (from readmodel.db).
        group.MapGet("/scenarios/runs", async (ReadModelQueries queries, int take = 30) => Results.Ok(await queries.GetScenarioRunsAsync(take)));

        // GET /demo/scenarios/runs/{runId} — one run with its assertions; this is what callers poll
        // until Status becomes PASSED / FAILED / INCONCLUSIVE.
        group.MapGet("/scenarios/runs/{runId:guid}", async (Guid runId, ReadModelQueries queries) =>
        {
            var (run, assertions) = await queries.GetScenarioRunAsync(runId);
            return run is null ? Results.NotFound() : Results.Ok(new { run, assertions });
        });

        // POST /demo/scenarios/{runId}/reset — emergency clean-up: remove that run's fault rules
        // from every service (the runner's teardown normally does this already).
        group.MapPost("/scenarios/{runId:guid}/reset", async (Guid runId, ServiceClients clients) =>
        {
            await clients.RemoveFaultsEverywhereAsync(runId);
            return Results.Ok(new { faultsCleared = true });
        });

        // ---- faults (toggle by hand from the UI or scripts) ----------------------------------------
        // Each service keeps its fault rules in memory and exposes its own /demo/faults endpoints;
        // these routes just fan the call out to the right service through ServiceClients.

        // GET /demo/faults — active rules of every service, keyed by service name.
        group.MapGet("/faults", async (ServiceClients clients) =>
        {
            var all = new Dictionary<string, IReadOnlyList<FaultRule>>();
            foreach (var service in new[] { ServiceNames.Orders, ServiceNames.Inventory, ServiceNames.Payments, ServiceNames.Web })
                all[service] = await clients.ListFaultsAsync(service);
            return Results.Ok(all);
        });
        // POST /demo/faults/{service} — arm a rule (JSON body = FaultRule) in one service.
        group.MapPost("/faults/{service}", async (string service, FaultRule rule, ServiceClients clients) =>
            Results.Ok(await clients.AddFaultAsync(service, rule)));
        // DELETE /demo/faults/{service}/{id} — disarm one rule.
        group.MapDelete("/faults/{service}/{id:guid}", async (string service, Guid id, ServiceClients clients) =>
        {
            await clients.RemoveFaultAsync(service, id);
            return Results.NoContent();
        });
        // DELETE /demo/faults — disarm ALL rules everywhere (null = not limited to one scenario).
        group.MapDelete("/faults", async (ServiceClients clients) =>
        {
            await clients.RemoveFaultsEverywhereAsync(null);
            return Results.NoContent();
        });

        // ---- resets -------------------------------------------------------------------------------

        // POST /demo/readmodel/rebuild — throw the read model away and re-project it from the stored
        // events (CQRS: the read side is disposable and can always be rebuilt).  Returns how many
        // events were replayed.
        group.MapPost("/readmodel/rebuild", async (ReadModelRebuilder rebuilder) => Results.Ok(new { replayed = await rebuilder.RebuildAsync(CancellationToken.None) }));

        // POST /demo/reset-all — ask every service to reset its demo data (stock, wallets, orders...).
        // 409 Conflict when the reset could not be done (e.g. a scenario is running or a service is down).
        group.MapPost("/reset-all", async (DemoResetService reset) =>
        {
            var result = await reset.ResetAllAsync();
            return result.Success ? Results.Ok(result) : Results.Conflict(result);
        });

        // GET /demo/health-map — the same health picture the service graph draws: which nodes are
        // up and how deep the RabbitMQ queues are.
        group.MapGet("/health-map", (ServiceHealthMonitor monitor) => Results.Ok(new { nodes = monitor.Nodes.Values, queues = monitor.Queues }));
    }
}
