// =====================================================================================================
//  ScenarioRunner.cs  —  starts a scenario, runs it in the background and records the verdict
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    A singleton service.  The Developer page (button "Run") and the /demo HTTP API both call
//    StartAsync(code).  The runner creates a ScenarioRun row, launches the scenario body on a
//    background task, and when the body ends it writes PASSED / FAILED / INCONCLUSIVE plus summary
//    numbers (delivery attempts, dedup hits, final saga state) into readmodel.db.
//
//  Where the student sees it:
//    Developer page → Scenario Lab: the run's status badge, live log, assertions, expected-vs-actual
//    trace and the "Scenario runs" table; /scenarios/{runId} shows the full report afterwards.
//    The UI updates live because the runner calls UiNotifier.Notify("scenario").
//
//  Data sources:
//    Writes only the Web's own readmodel.db (ScenarioRuns).  Owner state is read through
//    ScenarioContext, i.e. through the owner services' diagnostics endpoints.
//
//  Read next: ScenarioCatalog.cs (the scenario bodies), then Components/Pages/DeveloperPage.razor.
// =====================================================================================================

using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Web.Data;
using MiniShop.Web.Projections;
using MiniShop.Web.Queries;
using MiniShop.Web.ReadModel;
using MiniShop.Web.Services;

namespace MiniShop.Web.Scenarios;

/// <summary>
/// Executes scenarios in the background and records the verdict:
///   PASSED       — every invariant passed AND the expected trace appears, in order, in the actual trace.
///   FAILED       — an invariant failed, the trace differs, or the body threw.
///   INCONCLUSIVE — a wait timed out (e.g. nobody stopped the broker in S04) or the run was cancelled.
/// One scenario runs at a time, so experiments cannot disturb each other.  Teardown always removes
/// the scenario's fault rules from every service.
/// </summary>
/// <remarks>
/// Constructor dependencies:
///   scopes   — the runner is a singleton but ServiceClients / ReadModelQueries are scoped services, so
///              each run creates its own DI scope (see ExecuteAsync) instead of capturing them here.
///   factory  — creates short-lived ReadModelDbContext instances for readmodel.db.
///   notifier — in-process event that tells open Blazor pages "something changed, re-read".
///   logger   — normal ASP.NET Core logging (crashes and verdicts also go to the console).
/// </remarks>
public sealed class ScenarioRunner(IServiceScopeFactory scopes, IDbContextFactory<ReadModelDbContext> factory,
    UiNotifier notifier, ILogger<ScenarioRunner> logger)
{
    // A semaphore with ONE slot = a lock that also works with async code.  Two scenarios at once would
    // fight over stock, faults and the broker and their verdicts would mean nothing.
    private readonly SemaphoreSlim _oneAtATime = new(1, 1);
    /// <summary>Id of the run in progress, or null when idle.  The UI uses it to disable "Run" buttons;
    /// DemoResetService refuses to reset while it is set.</summary>
    public Guid? RunningId { get; private set; }

    /// <summary>
    /// Starts scenario <paramref name="code"/> and returns its run row immediately (the body keeps
    /// running in the background).  Returns null when the code is unknown or another scenario runs.
    /// </summary>
    public async Task<ScenarioRun?> StartAsync(string code)
    {
        var definition = ScenarioCatalog.Find(code);
        if (definition is null) return null;
        // WaitAsync(0) = "take the slot only if it is free RIGHT NOW"; never queue behind another run.
        if (!await _oneAtATime.WaitAsync(0)) return null;   // another scenario is running

        var run = new ScenarioRun
        {
            Id = Guid.NewGuid(),
            Code = definition.Code,
            Title = definition.Title,
            StartedAtUtc = DateTime.UtcNow,
            // Stored with the run so the report page can show expected vs actual later on.
            ExpectedTraceJson = JsonSerializer.Serialize(definition.ExpectedTrace),
        };
        // A brand-new customer for every run (e.g. "scn-s03-1a2b3c"): its wallet and orders cannot be
        // influenced by earlier runs or by the student shopping by hand.
        run.CustomerId = $"scn-{definition.Code.ToLowerInvariant()}-{run.Id.ToString("N")[..6]}";

        // Persist the row BEFORE starting, so the UI can show the run as "running" straight away.
        await using (var db = await factory.CreateDbContextAsync())
        {
            db.ScenarioRuns.Add(run);
            await db.SaveChangesAsync();
        }
        RunningId = run.Id;
        notifier.Notify("scenario");

        // Fire-and-forget on purpose: the caller (a button click or an HTTP request) must not wait
        // minutes.  ExecuteAsync catches everything itself and always releases the semaphore.
        _ = Task.Run(() => ExecuteAsync(definition, run));
        return run;
    }

    /// <summary>Runs the body, decides the verdict, tears down faults and saves the result.</summary>
    private async Task ExecuteAsync(ScenarioDefinition definition, ScenarioRun run)
    {
        // Our own DI scope for the whole run: scoped services live exactly as long as the run.
        using var scope = scopes.CreateScope();
        var clients = scope.ServiceProvider.GetRequiredService<ServiceClients>();
        var queries = scope.ServiceProvider.GetRequiredService<ReadModelQueries>();
        var health = scope.ServiceProvider.GetRequiredService<ServiceHealthMonitor>();
        // The overall time limit: when it fires, every wait inside the body throws OperationCanceledException.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(definition.TimeoutSeconds));
        var ctx = new ScenarioContext(run, clients, queries, factory, health, cts.Token);

        string status;
        string summary;
        try
        {
            await ctx.LogAsync($"{definition.Code} {definition.Title} started (scenarioId {run.Id}, customer {run.CustomerId})");
            // Common ARRANGE step for every scenario: give the fresh customer its starting balance.
            await ctx.SetWalletAsync(definition.WalletBalance);
            // The scenario itself (see ScenarioCatalog).  Assertions inside never throw; they set AllPassed.
            await definition.Body(ctx);

            // PASSED needs BOTH: all invariants true AND the expected messages in the right order.
            var traceOk = await CheckTraceAsync(ctx, definition);
            status = ctx.AllPassed && traceOk ? "PASSED" : "FAILED";
            summary = status == "PASSED" ? "all invariants passed and the trace matched" : "see failed assertions / trace mismatch";
        }
        catch (ScenarioTimeoutException ex)
        {
            // A wait did not see the expected durable state in time: we cannot say pass or fail.
            status = "INCONCLUSIVE";
            summary = ex.Message;
        }
        catch (OperationCanceledException)
        {
            // The overall TimeoutSeconds expired (the cts above was cancelled).
            status = "INCONCLUSIVE";
            summary = $"scenario exceeded its overall time limit of {definition.TimeoutSeconds}s";
        }
        catch (Exception ex)
        {
            // Anything else is a real problem (bug, unexpected HTTP error...): FAILED, with the message.
            status = "FAILED";
            summary = $"scenario crashed: {ex.Message}";
            logger.LogError(ex, "Scenario {Code} crashed", definition.Code);
        }

        try
        {
            // Teardown runs even after a timeout: faults must never outlive their scenario.
            // CancellationToken.None because the run's own token may already be cancelled.
            await clients.RemoveFaultsEverywhereAsync(run.Id, CancellationToken.None);
            await FinishAsync(ctx, run, status, summary);
        }
        finally
        {
            // Whatever happened, free the slot so the next scenario can run, and refresh the UI.
            RunningId = null;
            _oneAtATime.Release();
            notifier.Notify("scenario");
        }
    }

    /// <summary>The expected business messages must appear in order in the actual (collapsed) trace.</summary>
    private static async Task<bool> CheckTraceAsync(ScenarioContext ctx, ScenarioDefinition definition)
    {
        // Nothing to compare (S15 has no expected trace, or no order was created).
        if (definition.ExpectedTrace.Count == 0 || ctx.OrderIds.Count == 0) return true;

        // Telemetry travels on its own queue and can lag behind the business state (e.g. right after a
        // reset or a restart, when that queue is busy).  So keep reading the trace until the expected
        // sequence is there, for at most 10 seconds; a genuine mismatch is still reported as FAILED.
        List<string> actual = [];
        var ok = false;
        var deadline = DateTime.UtcNow.AddSeconds(10);
        do
        {
            await Task.Delay(500, CancellationToken.None);
            // The actual trace = message types published for the first order, from the Web's event audit.
            actual = Collapse(await ctx.Queries.GetActualTraceAsync(ctx.OrderIds[0]));
            ok = IsSubsequence(definition.ExpectedTrace, actual);
        }
        while (!ok && DateTime.UtcNow < deadline);
        await ctx.LogAsync($"expected trace: {string.Join(" → ", definition.ExpectedTrace)}");
        await ctx.LogAsync($"actual trace:   {string.Join(" → ", actual)}  → {(ok ? "match" : "MISMATCH")}");
        return ok;
    }

    /// <summary>
    /// Removes CONSECUTIVE repeats: [A, A, B, A] → [A, B, A].  A duplicate delivery (S06, S11) produces
    /// the same message type twice in a row; collapsing keeps the trace readable and comparable, while a
    /// genuine later repetition (S12 sends ReleaseStock again after RequiresAttention) is kept.
    /// </summary>
    private static List<string> Collapse(IEnumerable<string> trace)
    {
        var result = new List<string>();
        foreach (var item in trace)
            // result[^1] = last element (C# index-from-end operator).
            if (result.Count == 0 || result[^1] != item) result.Add(item);
        return result;
    }

    /// <summary>
    /// True when every item of <paramref name="expected"/> appears in <paramref name="actual"/> in the
    /// same order, possibly with other items in between.  Extra messages (telemetry-only types, retries)
    /// therefore do not break the check, but a missing or out-of-order business message does.
    /// </summary>
    private static bool IsSubsequence(IReadOnlyList<string> expected, IReadOnlyList<string> actual)
    {
        // i = how many expected items have been matched so far.
        var i = 0;
        foreach (var item in actual)
            if (i < expected.Count && expected[i] == item) i++;
        return i == expected.Count;
    }

    /// <summary>Writes the verdict and the summary numbers onto the run row in readmodel.db.</summary>
    private async Task FinishAsync(ScenarioContext ctx, ScenarioRun run, string status, string summary)
    {
        await using var db = await factory.CreateDbContextAsync();
        // Load the tracked row (it was modified by ExecuteUpdate calls during the run, so re-read it).
        var stored = await db.ScenarioRuns.FirstAsync(r => r.Id == run.Id);
        // Guid.Empty when the scenario never placed an order (e.g. it timed out during setup).
        var orderId = ctx.OrderIds.FirstOrDefault();

        stored.Status = status;
        stored.Summary = summary;
        stored.FinishedAtUtc = DateTime.UtcNow;
        stored.ActualTraceJson = JsonSerializer.Serialize(orderId == Guid.Empty ? [] : Collapse(await ctx.Queries.GetActualTraceAsync(orderId)));
        // How many times any consumer received a message of this run (retries and duplicates included).
        stored.DeliveryAttempts = await db.EventAudit.CountAsync(a => a.ScenarioId == run.Id && a.Kind == DiagnosticKind.MessageConsumed);
        // How many duplicates an Inbox detected and skipped.
        stored.DedupHits = await db.EventAudit.CountAsync(a => a.ScenarioId == run.Id && a.Kind == DiagnosticKind.InboxDuplicateIgnored);
        // The saga's final state, read from orders.db through the Orders diagnostics endpoint.
        stored.FinalSagaState = orderId == Guid.Empty ? null : await ctx.SagaStateAsync(orderId);
        await db.SaveChangesAsync();
        logger.LogInformation("Scenario {Code} finished: {Status} — {Summary}", run.Code, status, summary);
    }
}
