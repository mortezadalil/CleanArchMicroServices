// ==========================================================================================
// FILE: shared/Demo/DemoEndpoints.cs
//
// WHAT THIS FILE IS
//   Minimal-API endpoint mappings shared by every service:
//     - a token-protected route group (MapDemoGroup),
//     - the fault-rule control plane (/demo/faults),
//     - read-only views of the Outbox and Inbox tables (/diagnostics/outbox, /diagnostics/inbox).
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Each service's Program.cs calls app.MapDemoGroup(...) and then
//   MapFaultEndpoints() / MapOutboxInboxEndpoints<TDbContext>() on the returned group.
//   The callers are the Web's Scenario Lab (src/MiniShop.Web/Scenarios) and the Developer Console,
//   through src/MiniShop.Web/Services/ServiceClients.cs.
//
// WHICH PATTERN IT IMPLEMENTS
//   A separate "control plane" next to the business API: tooling to observe and perturb the system,
//   kept apart (different URL prefix, token, Development-only switch) from what customers use.
//
// Read next: shared/Faults/FaultRegistry.cs (what /demo/faults manipulates) and
//            src/MiniShop.Web/Scenarios/ScenarioContext.cs (the caller).
// ==========================================================================================

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Shared.Demo;

/// <summary>
/// Endpoints shared by every service:
///   /demo/faults        — manage fault rules (control plane)
///   /diagnostics/outbox — read-only view of the outbox table
///   /diagnostics/inbox  — read-only view of the inbox table
/// All of them require Demo:Enabled = true and the X-Demo-Token header, and are meant for localhost only.
/// Services add their own read-only endpoints (orders, stock, wallets) on the same group.
/// </summary>
/// <remarks>
/// Minimal APIs: <c>group.MapGet("/path", (Dep a, Dep b) =&gt; ...)</c> maps a URL to a lambda.  ASP.NET Core
/// fills the lambda's parameters automatically: services from DI (FaultRegistry, the DbContext), route values
/// (<c>{id:guid}</c>), query-string values (<c>orderId</c>, <c>take</c>) and the JSON body (FaultRule).
/// </remarks>
public static class DemoEndpoints
{
    /// <summary>Name of the HTTP header that must carry <see cref="DemoOptions.Token"/>.</summary>
    public const string TokenHeader = "X-Demo-Token";

    /// <summary>Creates the protected route group, or returns null when the control plane is disabled.</summary>
    /// <remarks>
    /// Returning null (instead of mapping endpoints that answer 404/403) means that, when disabled, the endpoints
    /// simply do not exist in the app.  Callers write <c>if (app.MapDemoGroup("/demo") is { } demo) { ... }</c>
    /// — the property pattern <c>is { } demo</c> means "if not null, call it demo", so nothing is mapped when disabled.
    /// </remarks>
    /// <param name="app">The web application being configured.</param>
    /// <param name="prefix">URL prefix of the group, e.g. "/demo" or "/diagnostics".</param>
    public static RouteGroupBuilder? MapDemoGroup(this WebApplication app, string prefix)
    {
        // Read the bound "Demo" settings once at startup.
        var options = app.Services.GetRequiredService<IOptions<DemoOptions>>().Value;
        // Control plane switched off (the default outside Development): map nothing.
        if (!options.Enabled)
            return null;

        // An endpoint FILTER runs before every endpoint in the group (like a tiny middleware): check the token,
        // and only call `next` (the real endpoint) when it matches.
        return app.MapGroup(prefix).AddEndpointFilter(async (ctx, next) =>
        {
            var header = ctx.HttpContext.Request.Headers[TokenHeader].ToString();
            // Ordinal = exact, case-sensitive byte comparison, the right choice for secrets.
            if (!string.Equals(header, options.Token, StringComparison.Ordinal))
                return Results.Unauthorized();
            return await next(ctx);
        });
    }

    /// <summary>Maps GET/POST/DELETE /faults on the group: list, add/replace and remove fault rules.</summary>
    /// <param name="group">The protected group returned by <see cref="MapDemoGroup"/>.</param>
    public static void MapFaultEndpoints(this RouteGroupBuilder group)
    {
        // List all rules of this service (with their Hits counters) — the Scenario Lab shows them.
        group.MapGet("/faults", (FaultRegistry faults) => Results.Ok(faults.List()));

        // Add (or replace, same Id) a rule.  The body is a FaultRule in JSON.
        group.MapPost("/faults", (FaultRule rule, FaultRegistry faults) =>
        {
            // Reject typos early: a rule at a non-existent point would silently never fire.
            if (!FaultPoints.All.Contains(rule.ActivationPoint))
                return Results.BadRequest($"Unknown activation point '{rule.ActivationPoint}'.");
            // Safety guard: a rule never lives longer than 10 minutes, even if the UI forgets to remove it.
            // `??=` assigns only when the left side is null (the caller did not choose an expiry).
            rule.ExpiresAtUtc ??= DateTime.UtcNow.AddMinutes(10);
            return Results.Ok(faults.Upsert(rule));
        });

        // Remove one rule by id.  {id:guid} is a route constraint: non-GUID values do not match this route.
        // 204 No Content when deleted, 404 when it did not exist.
        group.MapDelete("/faults/{id:guid}", (Guid id, FaultRegistry faults) =>
            faults.Remove(id) ? Results.NoContent() : Results.NotFound());

        // Remove many rules: DELETE /faults?scenarioId=... tears down one scenario (keeps experiments isolated);
        // DELETE /faults without a query removes everything.  Returns { removed = n }.
        group.MapDelete("/faults", (Guid? scenarioId, FaultRegistry faults) =>
            Results.Ok(new { removed = scenarioId is null ? faults.Clear() : faults.RemoveByScenario(scenarioId.Value) }));
    }

    /// <summary>Maps read-only GET /outbox and GET /inbox on the group, for any DbContext that has both tables.</summary>
    /// <remarks>
    /// A generic method with constraints: <c>where TDbContext : DbContext, IOutboxDbContext, IInboxDbContext</c>
    /// guarantees at compile time that <c>db.Outbox</c> and <c>db.Inbox</c> exist, so one implementation works for
    /// Orders, Inventory and Payments.
    /// </remarks>
    /// <typeparam name="TDbContext">The service's DbContext.</typeparam>
    /// <param name="group">The protected group.</param>
    public static void MapOutboxInboxEndpoints<TDbContext>(this RouteGroupBuilder group)
        where TDbContext : DbContext, IOutboxDbContext, IInboxDbContext
    {
        // GET /outbox?orderId=...&take=100 : newest rows first, plus the total number still pending.
        // The console uses `pending` to show the backlog growing during a broker outage (S04) and draining after.
        group.MapGet("/outbox", async (TDbContext db, Guid? orderId, int take = 100) =>
        {
            // AsNoTracking: read-only query, EF does not need to track the entities (faster, no accidental writes).
            var query = db.Outbox.AsNoTracking().AsQueryable();
            // Optional filter by order; the query is only built here and executed by ToListAsync below.
            if (orderId is not null) query = query.Where(m => m.CorrelationId == orderId);
            var rows = await query.OrderByDescending(m => m.Id).Take(take).ToListAsync();
            var pending = await db.Outbox.CountAsync(m => m.PublishedAtUtc == null);
            // Anonymous object → JSON { "pending": n, "rows": [...] }.
            return Results.Ok(new { pending, rows });
        });

        // GET /inbox?orderId=...&take=100 : which messages this service has processed (evidence of de-duplication, S06/S11).
        group.MapGet("/inbox", async (TDbContext db, Guid? orderId, int take = 100) =>
        {
            var query = db.Inbox.AsNoTracking().AsQueryable();
            if (orderId is not null) query = query.Where(m => m.CorrelationId == orderId);
            return Results.Ok(await query.OrderByDescending(m => m.ReceivedAtUtc).Take(take).ToListAsync());
        });
    }
}
