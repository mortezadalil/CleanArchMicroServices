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
    public static void MapHealthEndpoints(this WebApplication app, string serviceName)
    {
        app.MapGet("/health/live", () => Results.Ok(new { service = serviceName, status = "Healthy" }));

        app.MapHealthChecks("/health/ready", new HealthCheckOptions
        {
            ResponseWriter = async (context, report) =>
            {
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsJsonAsync(new
                {
                    service = serviceName,
                    status = report.Status.ToString(),
                    checks = report.Entries.Select(e => new { name = e.Key, status = e.Value.Status.ToString(), e.Value.Description }),
                });
            },
            ResultStatusCodes =
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status200OK,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
            },
        });
    }
}
