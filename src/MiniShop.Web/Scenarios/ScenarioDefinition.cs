// =====================================================================================================
//  ScenarioDefinition.cs  —  the "shape" of one Scenario Lab experiment
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    A tiny immutable record that describes ONE scenario (S01..S16): its code, the texts the student
//    reads on the Developer page, the business messages we expect to see, and the async function that
//    actually performs the experiment.
//
//  Where the student sees it:
//    On the Developer page (/developer) every scenario card shows Code, Title, Summary and Lesson, and
//    the "Expected vs actual" panel shows ExpectedTrace next to the trace that really happened.
//
//  Data source:
//    None by itself: it is pure configuration.  All sixteen instances live in ScenarioCatalog.All.
//
//  Read next: ScenarioCatalog.cs (the sixteen instances and their bodies), then ScenarioContext.cs.
// =====================================================================================================

namespace MiniShop.Web.Scenarios;

/// <summary>
/// One Scenario Lab scenario.  The body is a plain async function over <see cref="ScenarioContext"/>,
/// so a student can read the scenario top-to-bottom like a test: arrange → act → wait → assert.
/// </summary>
/// <param name="Code">Short id such as "S04"; used in URLs (/demo/scenarios/S04/run) and in customer ids.</param>
/// <param name="Title">Human title shown on the scenario card.</param>
/// <param name="Summary">One sentence: WHAT the experiment does to the system.</param>
/// <param name="Lesson">The take-away: WHICH pattern the scenario proves and why it matters.</param>
/// <param name="ExpectedTrace">
/// Business message types that the OWNER services must have published for the order, in this order.
/// The runner checks that they appear as a subsequence of the actual trace (extra messages are allowed).
/// An empty list means "no trace check" (S15 races two orders, so one single trace makes no sense).
/// </param>
/// <param name="Body">The experiment itself: arrange, act, wait on durable state, assert evidence.</param>
/// <param name="TimeoutSeconds">
/// Hard upper limit for the whole run.  When it expires the run is cancelled and ends INCONCLUSIVE.
/// Slow scenarios (broker outage, restarts, saga timeouts) raise it.
/// </param>
/// <param name="WalletBalance">
/// The runner sets the scenario customer's wallet to this amount before the body starts.
/// Scenarios that must FAIL the payment (S03, S12) use a small balance.
/// </param>
/// <param name="Requirement">
/// Optional note about something outside the app that the scenario needs (e.g. a way to stop RabbitMQ).
/// Shown on the card so the student knows before pressing "Run".
/// </param>
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
