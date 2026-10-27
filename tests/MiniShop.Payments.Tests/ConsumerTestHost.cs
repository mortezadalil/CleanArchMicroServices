// =====================================================================================================
// ConsumerTestHost.cs (Payments)  -  test INFRASTRUCTURE for INTEGRATION tests (no tests in here)
// =====================================================================================================
//
// WHAT THIS FILE IS
//   A miniature "Payments service" inside the test process: the REAL consumers (ChargeWalletConsumer,
//   CancelPaymentConsumer), the REAL EF Core model (PaymentsDbContext) on a REAL SQLite engine in memory,
//   the REAL shared building blocks (Inbox/Outbox handling, fault interceptor), and MassTransit's
//   IN-MEMORY transport + test harness instead of RabbitMQ.
//
//   It is the twin of tests/MiniShop.Inventory.Tests/ConsumerTestHost.cs - that file has the longer
//   explanations; the differences here are:
//     - it targets Topology.PaymentsQueue and registers the Payments consumers;
//     - StartAsync(balance) seeds ONE wallet (customer "c1") with the balance the test wants, instead of
//       the product catalog.  Choosing the balance per test makes "enough money" and "not enough money"
//       cases trivial to set up.
//
// WHAT IS DELIBERATELY NOT HERE
//   The OutboxDispatcher background service is not registered, so Outbox rows are never published.
//   They stay in the table and the tests count them as evidence of what the consumer decided.
//
// INFRASTRUCTURE NEEDED
//   None (no RabbitMQ, no Docker, no files on disk).
//
// Read next: LedgerTests.cs (in this folder)
// =====================================================================================================

// MassTransit core: AddConsumer, UsingInMemory, ReceiveEndpoint, UseMessageRetry, ConfigureConsumers.
using MassTransit;
// MassTransit's testing API: AddMassTransitTestHarness and ITestHarness (part of the MassTransit package).
using MassTransit.Testing;
// SqliteConnection - we create and own the in-memory database connection ourselves.
using Microsoft.Data.Sqlite;
// UseSqlite, AddInterceptors, EnsureCreatedAsync.
using Microsoft.EntityFrameworkCore;
// ConfigurationBuilder: an empty configuration, every option keeps its default.
using Microsoft.Extensions.Configuration;
// ServiceCollection / ServiceProvider: the standard .NET dependency-injection container.
using Microsoft.Extensions.DependencyInjection;
// AddLogging / LogLevel.
using Microsoft.Extensions.Logging;
// Topology (queue names), ServiceNames, Message.
using MiniShop.Contracts;
// The consumers under test.
using MiniShop.Payments.Consumers;
// PaymentsDbContext (Wallets, Ledger, Inbox, Outbox tables).
using MiniShop.Payments.Data;
// FaultingSaveChangesInterceptor (the S07 "transient database failure" hook).
using MiniShop.Shared.Faults;
// AddMiniShopBuildingBlocks and RetryPolicy.
using MiniShop.Shared.Messaging;

namespace MiniShop.Payments.Tests;

/// <summary>Real consumers + real EF model on in-memory SQLite + MassTransit in-memory transport.</summary>
/// <remarks>
/// <see cref="IAsyncDisposable"/> lets tests write <c>await using var host = ...</c>; the host (bus,
/// services and in-memory database) is torn down automatically when the test ends, pass or fail, so
/// every test starts from a clean, isolated database.
/// </remarks>
public sealed class ConsumerTestHost : IAsyncDisposable
{
    // A SQLite ":memory:" database lives only while the connection that created it is OPEN, and each new
    // ":memory:" connection would get a different, empty database.  So we keep ONE connection open for
    // the host's whole life and give it to every DbContext; seed, consumers and test queries all see the
    // same data.  It is closed (and the database deleted) in DisposeAsync.
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    // The built DI container.
    public ServiceProvider Provider { get; }

    // MassTransit's test harness: the in-memory bus plus recorders of sent/published/consumed messages.
    public ITestHarness Harness => Provider.GetRequiredService<ITestHarness>();

    public ConsumerTestHost()
    {
        // Opening the connection creates the (still empty) in-memory database.
        _connection.Open();
        var services = new ServiceCollection();
        // Warnings and errors only, to keep the test output readable.
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        // Same building blocks as the real Payments Program.cs; empty configuration = no fault rules.
        services.AddMiniShopBuildingBlocks(new ConfigurationBuilder().Build(), ServiceNames.Payments);
        // The real DbContext on our shared open connection, with the same SaveChanges interceptor as
        // production (inactive here because no fault rule is registered).
        services.AddDbContext<PaymentsDbContext>((sp, o) => o.UseSqlite(_connection)
            .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));
        // MassTransit configured like the real service, but with the in-memory transport and the harness.
        services.AddMassTransitTestHarness(x =>
        {
            // The consumers under test.  (Production also registers SnapshotRequestedConsumer, which only
            // re-announces wallet balances to the Web; not needed here.)
            x.AddConsumer<ChargeWalletConsumer>();
            x.AddConsumer<CancelPaymentConsumer>();
            x.UsingInMemory((context, cfg) =>
            {
                // Same queue name as production, so commands are addressed exactly like the saga does it.
                cfg.ReceiveEndpoint(Topology.PaymentsQueue, e =>
                {
                    // Same retry policy as production: a throwing consumer (e.g. a concurrency conflict on
                    // the wallet's Version, or the unique ledger index) is retried.
                    e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
                    // Connect the registered consumers to this queue.
                    e.ConfigureConsumers(context);
                });
            });
        });
        // validateScopes = true: fail loudly if a scoped service (DbContext) is resolved from the root.
        Provider = services.BuildServiceProvider(true);
    }

    /// <summary>Create the schema, give customer "c1" a wallet with <paramref name="balance"/> Toman, start the bus.</summary>
    public async Task StartAsync(long balance)
    {
        // A short-lived scope for setup; disposed before the bus starts.
        using (var scope = Provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
            // Build the tables from the EF model (no migrations needed for a throw-away database).
            await db.Database.EnsureCreatedAsync();
            // Exactly one wallet.  Version = 1 is the starting value of the optimistic-concurrency token.
            // `Domain.Wallet` is written with a partial namespace because this file has no
            // `using MiniShop.Payments.Domain;` - it resolves to MiniShop.Payments.Domain.Wallet.
            db.Wallets.Add(new Domain.Wallet { CustomerId = "c1", Balance = balance, Version = 1 });
            await db.SaveChangesAsync();
        }
        // Start the in-memory bus (and registered background services).
        await Harness.Start();
    }

    /// <summary>Send a command to the Payments queue and wait until the consumer has processed it.</summary>
    public async Task SendAsync<T>(T command) where T : Message
    {
        // A send endpoint addresses exactly one queue (queue:minishop.payments): how commands travel.
        var endpoint = await Harness.Bus.GetSendEndpoint(Topology.QueueAddress(Topology.PaymentsQueue));
        await endpoint.Send(command);
        // Send only puts the message in the queue.  Wait until the harness has recorded the consumption
        // of a T with OUR MessageId (or the harness timeout expires).  When a test sends the very same
        // message twice, this is already satisfied by the first consumption - the delay below covers it.
        await Harness.Consumed.Any<T>(x => x.Context.Message.MessageId == command.MessageId);
        // A small safety margin: "consumed" is recorded by pipeline observers, which can fire a moment
        // before everything around the consumer has settled.  The pause keeps the database assertions
        // that follow deterministic.
        await Task.Delay(100);
    }

    /// <summary>Run a query in a fresh scope (a fresh DbContext), so the test sees committed data only.</summary>
    public async Task<T> QueryAsync<T>(Func<PaymentsDbContext, Task<T>> query)
    {
        using var scope = Provider.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<PaymentsDbContext>());
    }

    /// <summary>Stop the bus, dispose the services, and close the connection LAST (that deletes the database).</summary>
    public async ValueTask DisposeAsync()
    {
        await Harness.Stop();
        await Provider.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
