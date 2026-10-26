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
public sealed class ScenarioRunner(IServiceScopeFactory scopes, IDbContextFactory<ReadModelDbContext> factory,
    UiNotifier notifier, ILogger<ScenarioRunner> logger)
{
    private readonly SemaphoreSlim _oneAtATime = new(1, 1);
    public Guid? RunningId { get; private set; }

    public async Task<ScenarioRun?> StartAsync(string code)
    {
        var definition = ScenarioCatalog.Find(code);
        if (definition is null) return null;
        if (!await _oneAtATime.WaitAsync(0)) return null;   // another scenario is running

        var run = new ScenarioRun
        {
            Id = Guid.NewGuid(),
            Code = definition.Code,
            Title = definition.Title,
            StartedAtUtc = DateTime.UtcNow,
            ExpectedTraceJson = JsonSerializer.Serialize(definition.ExpectedTrace),
        };
        run.CustomerId = $"scn-{definition.Code.ToLowerInvariant()}-{run.Id.ToString("N")[..6]}";

        await using (var db = await factory.CreateDbContextAsync())
        {
            db.ScenarioRuns.Add(run);
            await db.SaveChangesAsync();
        }
        RunningId = run.Id;
        notifier.Notify("scenario");

        _ = Task.Run(() => ExecuteAsync(definition, run));
        return run;
    }

    private async Task ExecuteAsync(ScenarioDefinition definition, ScenarioRun run)
    {
        using var scope = scopes.CreateScope();
        var clients = scope.ServiceProvider.GetRequiredService<ServiceClients>();
        var queries = scope.ServiceProvider.GetRequiredService<ReadModelQueries>();
        var health = scope.ServiceProvider.GetRequiredService<ServiceHealthMonitor>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(definition.TimeoutSeconds));
        var ctx = new ScenarioContext(run, clients, queries, factory, health, cts.Token);

        string status;
        string summary;
        try
        {
            await ctx.LogAsync($"{definition.Code} {definition.Title} started (scenarioId {run.Id}, customer {run.CustomerId})");
            await ctx.SetWalletAsync(definition.WalletBalance);
            await definition.Body(ctx);

            var traceOk = await CheckTraceAsync(ctx, definition);
            status = ctx.AllPassed && traceOk ? "PASSED" : "FAILED";
            summary = status == "PASSED" ? "all invariants passed and the trace matched" : "see failed assertions / trace mismatch";
        }
        catch (ScenarioTimeoutException ex)
        {
            status = "INCONCLUSIVE";
            summary = ex.Message;
        }
        catch (OperationCanceledException)
        {
            status = "INCONCLUSIVE";
            summary = $"scenario exceeded its overall time limit of {definition.TimeoutSeconds}s";
        }
        catch (Exception ex)
        {
            status = "FAILED";
            summary = $"scenario crashed: {ex.Message}";
            logger.LogError(ex, "Scenario {Code} crashed", definition.Code);
        }

        try
        {
            // Teardown runs even after a timeout: faults must never outlive their scenario.
            await clients.RemoveFaultsEverywhereAsync(run.Id, CancellationToken.None);
            await FinishAsync(ctx, run, status, summary);
        }
        finally
        {
            RunningId = null;
            _oneAtATime.Release();
            notifier.Notify("scenario");
        }
    }

    /// <summary>The expected business messages must appear in order in the actual (collapsed) trace.</summary>
    private static async Task<bool> CheckTraceAsync(ScenarioContext ctx, ScenarioDefinition definition)
    {
        if (definition.ExpectedTrace.Count == 0 || ctx.OrderIds.Count == 0) return true;
        await Task.Delay(1500, CancellationToken.None);          // let the last telemetry records land
        var actual = Collapse(await ctx.Queries.GetActualTraceAsync(ctx.OrderIds[0]));
        var ok = IsSubsequence(definition.ExpectedTrace, actual);
        await ctx.LogAsync($"expected trace: {string.Join(" → ", definition.ExpectedTrace)}");
        await ctx.LogAsync($"actual trace:   {string.Join(" → ", actual)}  → {(ok ? "match" : "MISMATCH")}");
        return ok;
    }

    private static List<string> Collapse(IEnumerable<string> trace)
    {
        var result = new List<string>();
        foreach (var item in trace)
            if (result.Count == 0 || result[^1] != item) result.Add(item);
        return result;
    }

    private static bool IsSubsequence(IReadOnlyList<string> expected, IReadOnlyList<string> actual)
    {
        var i = 0;
        foreach (var item in actual)
            if (i < expected.Count && expected[i] == item) i++;
        return i == expected.Count;
    }

    private async Task FinishAsync(ScenarioContext ctx, ScenarioRun run, string status, string summary)
    {
        await using var db = await factory.CreateDbContextAsync();
        var stored = await db.ScenarioRuns.FirstAsync(r => r.Id == run.Id);
        var orderId = ctx.OrderIds.FirstOrDefault();

        stored.Status = status;
        stored.Summary = summary;
        stored.FinishedAtUtc = DateTime.UtcNow;
        stored.ActualTraceJson = JsonSerializer.Serialize(orderId == Guid.Empty ? [] : Collapse(await ctx.Queries.GetActualTraceAsync(orderId)));
        stored.DeliveryAttempts = await db.EventAudit.CountAsync(a => a.ScenarioId == run.Id && a.Kind == DiagnosticKind.MessageConsumed);
        stored.DedupHits = await db.EventAudit.CountAsync(a => a.ScenarioId == run.Id && a.Kind == DiagnosticKind.InboxDuplicateIgnored);
        stored.FinalSagaState = orderId == Guid.Empty ? null : await ctx.SagaStateAsync(orderId);
        await db.SaveChangesAsync();
        logger.LogInformation("Scenario {Code} finished: {Status} — {Summary}", run.Code, status, summary);
    }
}
