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
/// </summary>
public sealed class ReadModelBootstrapper(IServiceScopeFactory scopes, IBus bus, ILogger<ReadModelBootstrapper> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Give our own receive endpoints a moment to bind their queues, so the answers have somewhere to go.
        await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (await HasReferenceDataAsync(stoppingToken))
                {
                    logger.LogInformation("Read model has catalog and wallets; bootstrap finished");
                    return;
                }

                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                timeout.CancelAfter(TimeSpan.FromSeconds(5));
                await bus.Publish(new ReadModelSnapshotRequested(ServiceNames.Web, "read model is empty")
                {
                    CorrelationId = Guid.Empty,
                    Producer = ServiceNames.Web,
                }, timeout.Token);
                logger.LogInformation("Read model is empty; snapshot requested from the owners");
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning("Snapshot request failed ({Error}); retrying in {Interval}s", ex.Message, Interval.TotalSeconds);
            }

            await Task.Delay(Interval, stoppingToken);
        }
    }

    private async Task<bool> HasReferenceDataAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
        return await db.ProductCards.AnyAsync(ct) && await db.WalletBalances.AnyAsync(ct);
    }
}
