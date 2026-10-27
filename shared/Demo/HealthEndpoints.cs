// ==========================================================================================
// FILE: shared/Demo/HealthEndpoints.cs
//
// WHAT THIS FILE IS
//   The two health endpoints every service exposes: /health/live and /health/ready.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Called from each Program.cs (app.MapHealthEndpoints(ServiceNames.X)).
//   Polled by the Web's ServiceHealthMonitor (src/MiniShop.Web/Services/ServiceHealthMonitor.cs) to
//   colour the nodes of the Developer Console graph, and usable by docker compose / Kubernetes probes.
//
// WHICH PATTERN IT IMPLEMENTS
//   Liveness vs readiness probes:
//     - live  : "the process is running" — if this fails, restart the process.
//     - ready : "the process can do useful work" (DB reachable, RabbitMQ connected) — if this fails,
//               stop sending it traffic, but restarting would not help (e.g. during S04 the broker is
//               down: every service becomes not-ready, yet they must NOT be restarted).
//
// Read next: src/MiniShop.Web/Services/ServiceHealthMonitor.cs.
// ==========================================================================================

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MiniShop.Shared.Demo;

/// <summary>
/// /health/live  — the process is running.
/// /health/ready — the process AND its dependencies (database, RabbitMQ connection) are usable.
/// The ready endpoint returns JSON with every check so the Web's service graph can show "up / degraded / down".
/// MassTransit registers its own bus health check automatically.
/// </summary>
public static class HealthEndpoints
{
    /// <summary>Maps /health/live and /health/ready.</summary>
    /// <param name="app">The web application.</param>
    /// <param name="serviceName">Included in the JSON so a caller can see which service answered.</param>
    public static void MapHealthEndpoints(this WebApplication app, string serviceName)
    {
        // Liveness: no dependency checks at all — answering at all proves the process is alive.
        app.MapGet("/health/live", () => Results.Ok(new { service = serviceName, status = "Healthy" }));

        // Readiness: runs every registered health check (the DbContext check added in Program.cs, and
        // MassTransit's bus check) and reports the combined result.
        app.MapHealthChecks("/health/ready", new HealthCheckOptions
        {
            // Custom JSON writer: the default response is a bare word ("Healthy"); we also list each check so
            // the console can say WHICH dependency is down.
            ResponseWriter = async (context, report) =>
            {
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsJsonAsync(new
                {
                    service = serviceName,
                    status = report.Status.ToString(),
                    // One entry per check: name, status and description.  `e.Value.Description` without a name
                    // creates a property called "Description" (anonymous-type member name inferred from the expression).
                    checks = report.Entries.Select(e => new { name = e.Key, status = e.Value.Status.ToString(), e.Value.Description }),
                });
            },
            // HTTP status per health status.  Degraded still returns 200 (it can serve, with reduced quality);
            // only Unhealthy returns 503, which tells probes/load balancers "do not send traffic here".
            // `[key] = value` inside an object initializer is an index initializer: it sets dictionary entries.
            ResultStatusCodes =
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status200OK,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
            },
        });
    }
}
