// =====================================================================================================
// ScenarioE2ETests.cs  -  END-TO-END tests (the top, narrowest layer of the test pyramid)
// =====================================================================================================
//
// WHAT THIS FILE PROVES
//   The WHOLE system works together: Web + Orders + Inventory + Payments + RabbitMQ + four SQLite
//   databases, over real HTTP and real messaging.  For every Scenario Lab scenario (S01..S16) it asks
//   the Web to run the scenario and checks that the scenario ended as PASSED.
//
//   Some guarantees can ONLY be tested here, because they live between processes:
//     S04 broker outage, S09 Orders restarted mid-saga, S11 crash after publish, S14 the same
//     Idempotency-Key used twice on the HTTP API, S15 two orders truly racing for the last unit, ...
//   The unit and integration tests prove each piece; this file proves the pieces fit.
//
// HOW IT WORKS ("evidence based")
//   This test does not inspect databases itself.  The Web's ScenarioRunner (src/MiniShop.Web/Scenarios)
//   starts the scenario, injects its faults, waits, and then checks the scenario's invariants against
//   the owner services' data and the expected message trace.  It stores a verdict: PASSED, FAILED or
//   INCONCLUSIVE (a wait ran out of time).  The test only reads that verdict and, on failure, prints the
//   full report so you can see WHICH invariant broke.
//
// INFRASTRUCTURE NEEDED
//   A RUNNING stack (docker compose, or every service started with `dotnet run`), reachable at
//   MINISHOP_WEB_URL.  Without it every test fails with "connection refused" - that is expected.
//   A full run takes minutes, which is why there are few tests at this level and many below it.
//
// CONFIGURATION (environment variables)
//   MINISHOP_WEB_URL          base URL of the Web            (default http://localhost:5100)
//   MINISHOP_DEMO_TOKEN       value of the X-Demo-Token header (default minishop-dev)
//   MINISHOP_E2E_INCLUDE_S04  set to 1 to also run S04 (it needs the "runner" container that can stop
//                             and restart RabbitMQ)
//
// Read next: README.md section "5. Tests", and src/MiniShop.Web/Scenarios/ScenarioCatalog.cs for what
//            each scenario does and asserts.
// =====================================================================================================

// JSON extension methods for HttpClient (GetFromJsonAsync, PostAsJsonAsync ...).  Not strictly needed by
// the code below (it parses responses with JsonNode), but handy if you add a test that posts JSON.
using System.Net.Http.Json;
// JsonSerializer - used to pretty-print the run report in failure messages.
using System.Text.Json;
// JsonNode - a loosely typed JSON tree.  We use it instead of C# DTO classes on purpose: an E2E test is
// a black-box client and should not depend on the Web's internal types.
using System.Text.Json.Nodes;
// [Fact], [Theory], [MemberData], Assert.
using Xunit;

namespace MiniShop.E2E;

/// <summary>
/// End-to-end runner: drives a RUNNING MiniShop stack (docker compose or dotnet run) through the Web's
/// Development API and asserts that every scenario reports PASSED.  Each run is evidence based: the
/// Web verifies the invariants inside the owner databases, this test only reads the verdict.
///
///   MINISHOP_WEB_URL          base URL of the Web (default http://localhost:5100)
///   MINISHOP_DEMO_TOKEN       X-Demo-Token (default minishop-dev)
///   MINISHOP_E2E_INCLUDE_S04  set to 1 when the runner container is available (S04 stops the broker)
/// </summary>
public class ScenarioE2ETests
{
    // Read once when the class is first used.  `??` = "if the variable is not set, use this default".
    private static readonly string BaseUrl = Environment.GetEnvironmentVariable("MINISHOP_WEB_URL") ?? "http://localhost:5100";
    // The Development API (/demo/...) is protected by a shared demo token sent in the X-Demo-Token header.
    private static readonly string Token = Environment.GetEnvironmentVariable("MINISHOP_DEMO_TOKEN") ?? "minishop-dev";
    // ONE HttpClient for all tests (static).  Creating a new HttpClient per request is a classic .NET
    // mistake that can exhaust sockets.  BaseAddress lets the tests use relative paths like "/developer";
    // the token header is added to every request; 30 s is the limit for a single HTTP call (the scenario
    // itself may run longer - we poll for it below).
    private static readonly HttpClient Http = new() { BaseAddress = new Uri(BaseUrl), DefaultRequestHeaders = { { "X-Demo-Token", Token } }, Timeout = TimeSpan.FromSeconds(30) };

    // Data source for the [Theory] below, referenced by [MemberData(nameof(Scenarios))].
    // xUnit calls this method, and every `object[]` it yields becomes ONE test case; the array's items are
    // the method's arguments (here a single string, the scenario code).  So the test runner lists
    // "Scenario_reports_PASSED(code: "S01")", "...S02" and so on as separate tests.
    // `yield return` produces the items lazily, one at a time.
    public static IEnumerable<object[]> Scenarios()
    {
        // S04 needs a helper container that can stop RabbitMQ, so it is opt-in.
        var includeS04 = Environment.GetEnvironmentVariable("MINISHOP_E2E_INCLUDE_S04") == "1";
        foreach (var code in new[] { "S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09", "S10", "S11", "S12", "S13", "S14", "S15", "S16" })
            if (code != "S04" || includeS04)
                // `[code]` is a C# collection expression; here it creates an object[] with one element.
                yield return [code];
    }

    // Smoke test - the cheapest possible "is the stack up and serving pages?" check.
    //
    // Arrange : a running stack.
    // Act     : GET the storefront ("/") and the Developer Console ("/developer").
    // Assert  : both pages contain their expected title text.
    // Invariant: the Web starts, its pages render, and it can reach what it needs to render them.
    // Bug caught: a broken deployment or configuration (wrong port, crashing Web, missing static files).
    //           If this fails, every scenario below will fail too - look here first.
    [Fact]
    public async Task Storefront_and_developer_console_render()
    {
        // GetStringAsync throws (failing the test) on a non-success HTTP status.
        var home = await Http.GetStringAsync("/");
        Assert.Contains("MiniShop", home);
        var dev = await Http.GetStringAsync("/developer");
        Assert.Contains("Developer Console", dev);
    }

    // One test case per scenario code (see Scenarios() above).  Covers ALL Scenario Lab scenarios,
    // including the ones referred to by the lower-level tests: S06 duplicate delivery, S10 payment timeout,
    // S13 late payment, S14 same Idempotency-Key, S15 last-unit race.
    //
    // Arrange : a running stack; no other scenario running.
    // Act     : POST /demo/scenarios/{code}/run, then poll GET /demo/scenarios/runs/{id} until the run is
    //           no longer "Running" (or 5 minutes pass).
    // Assert  : the final status is "PASSED".  Otherwise the failure message contains the whole JSON report
    //           (assertions and trace) so the cause is visible right in the test output.
    // Invariant: each scenario's own invariants (no double debit, no oversold stock, no order stuck,
    //           ...) hold on the REAL system, with real processes, real broker and real timing.
    // Bug caught: anything that only shows up when the parts run together: wrong queue bindings, wrong
    //           configuration, serialization differences between services, timing assumptions, lost
    //           messages across a restart or an outage.
    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_reports_PASSED(string code)
    {
        // ---- Act, part 1: start the run --------------------------------------------------------------
        // scenarios run one at a time: wait for a free slot
        // The Web refuses to start a second scenario while one is running (scenarios inject faults that
        // would disturb each other) and answers 409 Conflict.  A slot can be busy because someone started a
        // scenario from the Developer Console or scripts/run-scenarios.sh, or because a previous test case
        // gave up polling while its run was still going.  So we retry every 2 s, at most 120 times
        // (about 4 minutes).
        JsonNode? run = null;
        for (var i = 0; i < 120 && run is null; i++)
        {
            // POST with no body (null content).  The Web answers 202 Accepted with the new run as JSON.
            var response = await Http.PostAsync($"/demo/scenarios/{code}/run", null);
            // Busy: wait and try again.  `continue` jumps to the next loop iteration.
            if (response.StatusCode == System.Net.HttpStatusCode.Conflict) { await Task.Delay(2000); continue; }
            // Any other non-2xx status (401 bad token, 404 unknown scenario, 500 ...) is a real failure.
            response.EnsureSuccessStatusCode();
            run = JsonNode.Parse(await response.Content.ReadAsStringAsync());
        }
        // If we never got a slot, fail here with a clear message instead of a NullReferenceException later.
        Assert.NotNull(run);
        // run["id"] reads a JSON property; `!` tells the compiler we know it is not null.
        var runId = run!["id"]!.GetValue<string>();

        // ---- Act, part 2: poll until the run has a verdict ----------------------------------------
        // Scenarios are asynchronous (sagas, timeouts, retries), so we POLL instead of expecting an
        // instant answer.  Every 2 s we fetch the run; we stop as soon as its status is no longer
        // "Running", or when the 5-minute deadline passes (then the last status, still "Running", is
        // reported as the failure).
        JsonNode? result = null;
        var deadline = DateTime.UtcNow.AddMinutes(5);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(2000);
            // The response looks like { "run": { "status": ..., ... }, "assertions": [ ... ] }.
            result = JsonNode.Parse(await Http.GetStringAsync($"/demo/scenarios/runs/{runId}"));
            if (result!["run"]!["status"]!.GetValue<string>() != "Running") break;
        }

        // ---- Assert --------------------------------------------------------------------------------
        var status = result!["run"]!["status"]!.GetValue<string>();
        // Pretty-print the whole report so a failing test shows every assertion and the message trace.
        var report = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
        // Assert.True(condition, message): the message is only shown when the assertion fails.
        // Only PASSED counts; FAILED and INCONCLUSIVE (a wait inside the scenario timed out) both fail.
        Assert.True(status == "PASSED", $"{code} ended as {status}\n{report}");
    }
}
