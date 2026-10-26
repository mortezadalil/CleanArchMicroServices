using MassTransit;
using MassTransit.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Inventory.Consumers;
using MiniShop.Inventory.Data;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;

namespace MiniShop.Inventory.Tests;

/// <summary>
/// Integration test host: the REAL consumers, the REAL EF model on an in-memory SQLite database,
/// and MassTransit's in-memory transport instead of RabbitMQ.  This tests the Inbox/Outbox/transaction
/// behaviour of the consumers without any infrastructure.
/// </summary>
public sealed class ConsumerTestHost : IAsyncDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    public ServiceProvider Provider { get; }
    public ITestHarness Harness => Provider.GetRequiredService<ITestHarness>();

    public ConsumerTestHost()
    {
        _connection.Open();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddMiniShopBuildingBlocks(new ConfigurationBuilder().Build(), ServiceNames.Inventory);
        services.AddDbContext<InventoryDbContext>((sp, o) => o.UseSqlite(_connection)
            .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));
        services.AddMassTransitTestHarness(x =>
        {
            x.AddConsumer<ReserveStockConsumer>();
            x.AddConsumer<ReleaseStockConsumer>();
            x.AddConsumer<OrderConfirmedConsumer>();
            x.UsingInMemory((context, cfg) =>
            {
                cfg.ReceiveEndpoint(Topology.InventoryQueue, e =>
                {
                    e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
                    e.ConfigureConsumers(context);
                });
            });
        });
        Provider = services.BuildServiceProvider(true);
    }

    public async Task StartAsync()
    {
        using (var scope = Provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
            await db.Database.EnsureCreatedAsync();
            await CatalogSeed.SeedAsync(db, CancellationToken.None);
        }
        await Harness.Start();
    }

    public async Task SendAsync<T>(T command) where T : Message
    {
        var endpoint = await Harness.Bus.GetSendEndpoint(Topology.QueueAddress(Topology.InventoryQueue));
        await endpoint.Send(command);
        await Harness.Consumed.Any<T>(x => x.Context.Message.MessageId == command.MessageId);
        await Task.Delay(100); // let the consumer's transaction finish
    }

    public async Task<T> QueryAsync<T>(Func<InventoryDbContext, Task<T>> query)
    {
        using var scope = Provider.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<InventoryDbContext>());
    }

    public async ValueTask DisposeAsync()
    {
        await Harness.Stop();
        await Provider.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
