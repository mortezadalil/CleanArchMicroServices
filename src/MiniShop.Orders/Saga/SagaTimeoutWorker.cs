using Microsoft.EntityFrameworkCore;
using MiniShop.Orders.Data;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Saga;

/// <summary>
/// Polls the saga table once per second for instances whose DeadlineUtc has passed and lets the state
/// machine decide what to do (<see cref="OrderSaga.OnTimeout"/>).  Deadlines are stored in the database,
/// so a restart of the Orders service does not lose them (S09).
///
/// A simple poll is used instead of a message scheduler on purpose: it is easy to read and needs no
/// extra RabbitMQ plugin.
/// </summary>
public sealed class SagaTimeoutWorker(IServiceScopeFactory scopes, OutboxSignal signal, ILogger<SagaTimeoutWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessExpiredAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Saga timeout worker failed; will retry");
            }
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    private async Task ProcessExpiredAsync(CancellationToken ct)
    {
        using var scope = scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
        var runner = scope.ServiceProvider.GetRequiredService<SagaRunner>();

        var now = DateTime.UtcNow;
        var expiredIds = await db.SagaInstances
            .Where(s => s.DeadlineUtc != null && s.DeadlineUtc <= now)
            .Select(s => s.CorrelationId)
            .ToListAsync(ct);

        foreach (var orderId in expiredIds)
        {
            try
            {
                await using var tx = await db.Database.BeginTransactionAsync(ct);
                var loaded = await runner.LoadAsync(orderId, ct);
                if (loaded is null) continue;
                var (saga, order) = loaded.Value;
                if (saga.DeadlineUtc is null || saga.DeadlineUtc > DateTime.UtcNow) continue; // answered meanwhile

                using var _ = FaultContext.Enter(order.ScenarioId, order.Id);
                var decision = OrderSaga.OnTimeout(saga, order, runner.Options);
                var result = runner.Apply(saga, order, decision, trigger: "Timeout", causation: null);
                await db.SaveChangesAsync(ct);
                await tx.CommitAsync(ct);
                result.AfterCommit?.Invoke();
                signal.Notify();
                logger.LogWarning("Saga {OrderId} timed out: {Detail}", orderId, decision.Detail);
            }
            catch (DbUpdateConcurrencyException)
            {
                // A message for this order was processed at the same time; the next poll re-evaluates.
                db.ChangeTracker.Clear();
            }
        }
    }
}
