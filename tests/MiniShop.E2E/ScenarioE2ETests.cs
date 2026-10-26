using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
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
    private static readonly string BaseUrl = Environment.GetEnvironmentVariable("MINISHOP_WEB_URL") ?? "http://localhost:5100";
    private static readonly string Token = Environment.GetEnvironmentVariable("MINISHOP_DEMO_TOKEN") ?? "minishop-dev";
    private static readonly HttpClient Http = new() { BaseAddress = new Uri(BaseUrl), DefaultRequestHeaders = { { "X-Demo-Token", Token } }, Timeout = TimeSpan.FromSeconds(30) };

    public static IEnumerable<object[]> Scenarios()
    {
        var includeS04 = Environment.GetEnvironmentVariable("MINISHOP_E2E_INCLUDE_S04") == "1";
        foreach (var code in new[] { "S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09", "S10", "S11", "S12", "S13", "S14", "S15", "S16" })
            if (code != "S04" || includeS04)
                yield return [code];
    }

    [Fact]
    public async Task Storefront_and_developer_console_render()
    {
        var home = await Http.GetStringAsync("/");
        Assert.Contains("MiniShop", home);
        var dev = await Http.GetStringAsync("/developer");
        Assert.Contains("Developer Console", dev);
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_reports_PASSED(string code)
    {
        // scenarios run one at a time: wait for a free slot
        JsonNode? run = null;
        for (var i = 0; i < 120 && run is null; i++)
        {
            var response = await Http.PostAsync($"/demo/scenarios/{code}/run", null);
            if (response.StatusCode == System.Net.HttpStatusCode.Conflict) { await Task.Delay(2000); continue; }
            response.EnsureSuccessStatusCode();
            run = JsonNode.Parse(await response.Content.ReadAsStringAsync());
        }
        Assert.NotNull(run);
        var runId = run!["id"]!.GetValue<string>();

        JsonNode? result = null;
        var deadline = DateTime.UtcNow.AddMinutes(5);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(2000);
            result = JsonNode.Parse(await Http.GetStringAsync($"/demo/scenarios/runs/{runId}"));
            if (result!["run"]!["status"]!.GetValue<string>() != "Running") break;
        }

        var status = result!["run"]!["status"]!.GetValue<string>();
        var report = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
        Assert.True(status == "PASSED", $"{code} ended as {status}\n{report}");
    }
}
