using MassTransit;
using MassTransit.Testing;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Payments.Consumers;
using MiniShop.Payments.Data;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;

namespace MiniShop.Payments.Tests;

/// <summary>Real consumers + real EF model on in-memory SQLite + MassTransit in-memory transport.</summary>
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
        services.AddMiniShopBuildingBlocks(new ConfigurationBuilder().Build(), ServiceNames.Payments);
        services.AddDbContext<PaymentsDbContext>((sp, o) => o.UseSqlite(_connection)
            .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));
        services.AddMassTransitTestHarness(x =>
        {
            x.AddConsumer<ChargeWalletConsumer>();
            x.AddConsumer<CancelPaymentConsumer>();
            x.UsingInMemory((context, cfg) =>
            {
                cfg.ReceiveEndpoint(Topology.PaymentsQueue, e =>
                {
                    e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
                    e.ConfigureConsumers(context);
                });
            });
        });
        Provider = services.BuildServiceProvider(true);
    }

    public async Task StartAsync(long balance)
    {
        using (var scope = Provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Wallets.Add(new Domain.Wallet { CustomerId = "c1", Balance = balance, Version = 1 });
            await db.SaveChangesAsync();
        }
        await Harness.Start();
    }

    public async Task SendAsync<T>(T command) where T : Message
    {
        var endpoint = await Harness.Bus.GetSendEndpoint(Topology.QueueAddress(Topology.PaymentsQueue));
        await endpoint.Send(command);
        await Harness.Consumed.Any<T>(x => x.Context.Message.MessageId == command.MessageId);
        await Task.Delay(100);
    }

    public async Task<T> QueryAsync<T>(Func<PaymentsDbContext, Task<T>> query)
    {
        using var scope = Provider.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<PaymentsDbContext>());
    }

    public async ValueTask DisposeAsync()
    {
        await Harness.Stop();
        await Provider.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
