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
public static class DemoEndpoints
{
    public const string TokenHeader = "X-Demo-Token";

    /// <summary>Creates the protected route group, or returns null when the control plane is disabled.</summary>
    public static RouteGroupBuilder? MapDemoGroup(this WebApplication app, string prefix)
    {
        var options = app.Services.GetRequiredService<IOptions<DemoOptions>>().Value;
        if (!options.Enabled)
            return null;

        return app.MapGroup(prefix).AddEndpointFilter(async (ctx, next) =>
        {
            var header = ctx.HttpContext.Request.Headers[TokenHeader].ToString();
            if (!string.Equals(header, options.Token, StringComparison.Ordinal))
                return Results.Unauthorized();
            return await next(ctx);
        });
    }

    public static void MapFaultEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/faults", (FaultRegistry faults) => Results.Ok(faults.List()));

        group.MapPost("/faults", (FaultRule rule, FaultRegistry faults) =>
        {
            if (!FaultPoints.All.Contains(rule.ActivationPoint))
                return Results.BadRequest($"Unknown activation point '{rule.ActivationPoint}'.");
            // Safety guard: a rule never lives longer than 10 minutes, even if the UI forgets to remove it.
            rule.ExpiresAtUtc ??= DateTime.UtcNow.AddMinutes(10);
            return Results.Ok(faults.Upsert(rule));
        });

        group.MapDelete("/faults/{id:guid}", (Guid id, FaultRegistry faults) =>
            faults.Remove(id) ? Results.NoContent() : Results.NotFound());

        group.MapDelete("/faults", (Guid? scenarioId, FaultRegistry faults) =>
            Results.Ok(new { removed = scenarioId is null ? faults.Clear() : faults.RemoveByScenario(scenarioId.Value) }));
    }

    public static void MapOutboxInboxEndpoints<TDbContext>(this RouteGroupBuilder group)
        where TDbContext : DbContext, IOutboxDbContext, IInboxDbContext
    {
        group.MapGet("/outbox", async (TDbContext db, Guid? orderId, int take = 100) =>
        {
            var query = db.Outbox.AsNoTracking().AsQueryable();
            if (orderId is not null) query = query.Where(m => m.CorrelationId == orderId);
            var rows = await query.OrderByDescending(m => m.Id).Take(take).ToListAsync();
            var pending = await db.Outbox.CountAsync(m => m.PublishedAtUtc == null);
            return Results.Ok(new { pending, rows });
        });

        group.MapGet("/inbox", async (TDbContext db, Guid? orderId, int take = 100) =>
        {
            var query = db.Inbox.AsNoTracking().AsQueryable();
            if (orderId is not null) query = query.Where(m => m.CorrelationId == orderId);
            return Results.Ok(await query.OrderByDescending(m => m.ReceivedAtUtc).Take(take).ToListAsync());
        });
    }
}
