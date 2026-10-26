namespace MiniShop.Web.Scenarios;

/// <summary>
/// One Scenario Lab scenario.  The body is a plain async function over <see cref="ScenarioContext"/>,
/// so a student can read the scenario top-to-bottom like a test: arrange → act → wait → assert.
/// </summary>
public sealed record ScenarioDefinition(
    string Code,
    string Title,
    string Summary,
    string Lesson,
    IReadOnlyList<string> ExpectedTrace,
    Func<ScenarioContext, Task> Body,
    int TimeoutSeconds = 90,
    long WalletBalance = 5_000_000,
    string? Requirement = null);
