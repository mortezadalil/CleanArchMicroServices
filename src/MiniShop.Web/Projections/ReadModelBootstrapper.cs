// ==================================================================================================
// FILE: src/MiniShop.Web/Projections/ReadModelBootstrapper.cs
//
// WHAT IT IS
//   A background job that runs at startup and fills an EMPTY read model with reference data (the
//   product catalog and the wallet balances) by asking the owner services to re-send it.
//
// THE PROBLEM IT SOLVES: the "late subscriber"
//   In RabbitMQ a published event is copied only to the queues that are bound to its exchange AT THAT
//   MOMENT.  If Inventory starts first, seeds its catalog and publishes ProductCatalogChanged before the
//   Web has ever created its minishop.web queue, those events go nowhere: they are dropped.  The Web would
//   then show an empty shop forever, because nothing changes in the catalog until someone buys.
//
// THE SOLUTION
//   While the tables are empty, publish ReadModelSnapshotRequested every few seconds.  Inventory and
//   Payments subscribe to it and answer by publishing (through their outboxes) the CURRENT state as
//   normal ProductCatalogChanged / WalletBalanceChanged events.  Our ordinary projections then fill the
//   tables.  Because the projections are idempotent upserts with ordering guards, receiving the same
//   snapshot twice, or a snapshot mixed with live events, is harmless.
//   ReadModelSnapshotRequested is the only non-telemetry message the Web ever publishes (here and in
//   DemoResetService); everything else the Web sends on the bus is diagnostics.
//
// Read next: Services/DemoResetService.cs (which uses the same request after a reset).
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Web.Data;

namespace MiniShop.Web.Projections;

/// <summary>
/// Makes sure the read model gets its reference data even when the Web starts AFTER the owners.
///
/// Every 5 seconds: if the product cards or the wallet balances are still empty, publish
/// <see cref="ReadModelSnapshotRequested"/>.  Inventory and Payments answer through their outboxes and
/// the normal projections fill the tables.  Once both tables have data the job stops.
/// A request may get lost (broker down, owner not started yet); that is fine because it is repeated.
/// Registered with AddHostedService, so it starts with the app and stops when the app shuts down.
/// </summary>
public sealed class ReadModelBootstrapper(IServiceScopeFactory scopes, IBus bus, ILogger<ReadModelBootstrapper> logger) : BackgroundService
{
    /// <summary>Pause between two checks/requests.</summary>
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    /// <summary>The loop: check → (if empty) request a snapshot → wait → repeat, until the data is there.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Give our own receive endpoints a moment to bind their queues, so the answers have somewhere to go.
        // (Asking before minishop.web is bound would recreate the very late-subscriber problem we are fixing.)
        await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Done as soon as both catalog and wallets exist: the job ends and never runs again.
                if (await HasReferenceDataAsync(stoppingToken))
                {
                    logger.LogInformation("Read model has catalog and wallets; bootstrap finished");
                    return;
                }

                // Publish with a 5 s timeout: when RabbitMQ is down, Publish could otherwise wait a long
                // time and block the loop.  The linked token still honours application shutdown.
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                // Published directly on the bus (no outbox): the Web has no business state to keep
                // consistent with this message, and if it is lost the next loop simply asks again.
                // CorrelationId = Guid.Empty because it belongs to no order.
                await bus.Publish(new ReadModelSnapshotRequested(ServiceNames.Web, "read model is empty")
                {
                    CorrelationId = Guid.Empty,
                    Producer = ServiceNames.Web,
                }, timeout.Token);
                logger.LogInformation("Read model is empty; snapshot requested from the owners");
            }
            // Swallow failures (broker unreachable, our 5 s timeout fired, database busy) and try again
            // later.  The only exception NOT swallowed is the cancellation caused by app shutdown.
            catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning("Snapshot request failed ({Error}); retrying in {Interval}s", ex.Message, Interval.TotalSeconds);
            }

            await Task.Delay(Interval, stoppingToken);
        }
    }

    /// <summary>True when readmodel.db has at least one product card AND one wallet balance.</summary>
    private async Task<bool> HasReferenceDataAsync(CancellationToken ct)
    {
        // A BackgroundService is a singleton and cannot take the scoped DbContext in its constructor,
        // so it opens a short-lived scope for each check.
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
        return await db.ProductCards.AnyAsync(ct) && await db.WalletBalances.AnyAsync(ct);
    }
}
