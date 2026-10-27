// =====================================================================================================
// ConsumerTestHost.cs (Inventory)  -  test INFRASTRUCTURE for INTEGRATION tests (no tests in here)
// =====================================================================================================
//
// WHAT THIS FILE IS
//   A miniature "Inventory service" that lives entirely inside the test process.  It wires together:
//     - the REAL consumers (ReserveStockConsumer, ReleaseStockConsumer, OrderConfirmedConsumer),
//     - the REAL EF Core model (InventoryDbContext) on a REAL SQLite engine, but in memory,
//     - the REAL shared building blocks (Inbox/Outbox handling, fault interceptor, diagnostics),
//     - MassTransit's IN-MEMORY transport instead of RabbitMQ, plus its test harness.
//   So when a test sends ReserveStock, the message travels through MassTransit's real pipeline
//   (including the retry policy), reaches the real consumer, which runs the real Inbox + business logic
//   + Outbox write in one real SQLite transaction.  Only the "outside world" is faked.
//
// WHY THIS LEVEL OF THE PYRAMID
//   The interesting bugs in a message consumer are rarely in one method: they are in how the Inbox
//   check, the business change, the Outbox row and the transaction fit together.  A unit test with
//   mocks cannot see those; a full stack (RabbitMQ + Docker) is slow and flaky.  This host is the
//   middle ground: real code, real database, fake transport, a few hundred milliseconds per test.
//
// WHAT IS DELIBERATELY NOT HERE
//   The OutboxDispatcher background service is NOT registered (production registers it in
//   src/MiniShop.Inventory/Program.cs).  Outbox rows are therefore never published and never marked
//   as published - they simply stay in the table, where the tests can count them.  That is how the tests
//   check "the consumer wanted to publish X" without needing a subscriber.
//
// HOW TESTS USE IT
//   await using var host = new ConsumerTestHost();   // build the services
//   await host.StartAsync();                          // create schema, seed catalog, start the bus
//   await host.SendAsync(new ReserveStock(...));      // deliver a message and wait until it is consumed
//   var x = await host.QueryAsync(db => ...);         // look inside the database
//
// INFRASTRUCTURE NEEDED
//   None (no RabbitMQ, no Docker, no files on disk).
//
// Read next: ReservationTests.cs (in this folder)
// =====================================================================================================

// MassTransit core: AddConsumer, UsingInMemory, ReceiveEndpoint, UseMessageRetry, ConfigureConsumers.
using MassTransit;
// MassTransit's testing API: AddMassTransitTestHarness and ITestHarness.  It ships inside the normal
// MassTransit package, so no extra NuGet package is needed.
using MassTransit.Testing;
// SqliteConnection - we create and own the connection ourselves (see the field below for why).
using Microsoft.Data.Sqlite;
// UseSqlite, AddInterceptors, EnsureCreatedAsync.
using Microsoft.EntityFrameworkCore;
// ConfigurationBuilder: an empty configuration is enough, every option keeps its default value.
using Microsoft.Extensions.Configuration;
// ServiceCollection / ServiceProvider: the same dependency-injection container ASP.NET Core uses.
using Microsoft.Extensions.DependencyInjection;
// AddLogging / LogLevel - to keep test output quiet.
using Microsoft.Extensions.Logging;
// Topology (queue names), ServiceNames, Message (base type of every contract).
using MiniShop.Contracts;
// The consumers under test.
using MiniShop.Inventory.Consumers;
// InventoryDbContext and CatalogSeed (the fixed demo catalog).
using MiniShop.Inventory.Data;
// FaultingSaveChangesInterceptor (the S07 "transient database failure" hook).
using MiniShop.Shared.Faults;
// AddMiniShopBuildingBlocks and RetryPolicy.
using MiniShop.Shared.Messaging;

namespace MiniShop.Inventory.Tests;

/// <summary>
/// Integration test host: the REAL consumers, the REAL EF model on an in-memory SQLite database,
/// and MassTransit's in-memory transport instead of RabbitMQ.  This tests the Inbox/Outbox/transaction
/// behaviour of the consumers without any infrastructure.
/// </summary>
/// <remarks>
/// Implements <see cref="IAsyncDisposable"/> so tests can write <c>await using var host = ...</c>:
/// when the test method ends - passed OR failed - C# calls <see cref="DisposeAsync"/> automatically,
/// which stops the bus and throws the in-memory database away.  Every test therefore gets its own,
/// completely fresh service and database; tests cannot influence each other.
/// </remarks>
public sealed class ConsumerTestHost : IAsyncDisposable
{
    // WHY WE KEEP ONE CONNECTION OPEN FOR THE WHOLE LIFETIME OF THE HOST
    // A SQLite ":memory:" database exists only as long as the connection that created it stays OPEN,
    // and every new connection to ":memory:" gets its OWN, empty database.  EF Core normally opens and
    // closes a connection around each operation, and every DbContext scope would get a new connection -
    // so the tables created in StartAsync would be gone (or invisible) by the time the consumer runs.
    // Solution: open ONE connection in the constructor, give that same connection object to every
    // DbContext, and close it only in DisposeAsync.  All scopes (seed, consumer, test queries) then see
    // the same database.
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    // The built DI container.  Public so a test could resolve something special if it ever needed to.
    public ServiceProvider Provider { get; }

    // The MassTransit test harness: the in-memory bus plus "recorders" of everything that was sent,
    // published and consumed.  Resolved from DI on every access (it is a singleton, so it is cheap).
    public ITestHarness Harness => Provider.GetRequiredService<ITestHarness>();

    public ConsumerTestHost()
    {
        // Open the connection now - this is the moment the in-memory database is born.
        _connection.Open();
        var services = new ServiceCollection();
        // Only warnings and errors: otherwise MassTransit and EF Core would flood the test output.
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        // The same registration call the real Inventory Program.cs uses (diagnostics, fault registry,
        // FaultingSaveChangesInterceptor, OutboxSignal, demo options).  An empty configuration means
        // "all defaults"; no fault rules are active, so faults never fire in these tests.
        services.AddMiniShopBuildingBlocks(new ConfigurationBuilder().Build(), ServiceNames.Inventory);
        // The real DbContext, pointed at our shared, already-open in-memory connection.
        // The interceptor is added exactly like in production, so the test runs the same SaveChanges
        // pipeline (it does nothing here because no fault rule is registered).
        services.AddDbContext<InventoryDbContext>((sp, o) => o.UseSqlite(_connection)
            .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));
        // AddMassTransitTestHarness = AddMassTransit + the ITestHarness recorders.  Inside the lambda we
        // configure MassTransit exactly as the service would, only with a different transport.
        services.AddMassTransitTestHarness(x =>
        {
            // Register the consumers under test.  Production (src/MiniShop.Inventory/Program.cs) also
            // registers SnapshotRequestedConsumer, which only re-announces the catalog to the Web and is
            // not needed here.  MassTransit will create a consumer per message, inside a DI scope,
            // so each message gets its own InventoryDbContext - just like in production.
            x.AddConsumer<ReserveStockConsumer>();
            x.AddConsumer<ReleaseStockConsumer>();
            x.AddConsumer<OrderConfirmedConsumer>();
            // In-memory transport: queues are plain in-process structures; no network, no broker.
            x.UsingInMemory((context, cfg) =>
            {
                // Use the SAME queue name as production (Topology.InventoryQueue), so SendAsync below can
                // address it exactly the way the Orders saga does.
                cfg.ReceiveEndpoint(Topology.InventoryQueue, e =>
                {
                    // The SAME retry policy as production (shared/Messaging/MessagingOptions.cs).  If a
                    // consumer throws (e.g. an optimistic-concurrency conflict), MassTransit retries it,
                    // and the test exercises that path too.
                    e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
                    // Attach every consumer registered above to this endpoint.
                    e.ConfigureConsumers(context);
                });
            });
        });
        // Build the container.  `true` = validateScopes: the container throws if a scoped service
        // (like a DbContext) is ever resolved from the ROOT provider.  That would be a real bug in
        // production (one DbContext shared by all messages), so we want the tests to catch it.
        Provider = services.BuildServiceProvider(true);
    }

    /// <summary>Create the schema, seed the demo catalog (10 units of each product) and start the bus.</summary>
    public async Task StartAsync()
    {
        // A short-lived DI scope just for the setup work; the braces make sure the scope (and its
        // DbContext) is disposed BEFORE the bus starts.
        using (var scope = Provider.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
            // Create all tables from the EF model (no migrations needed for a throw-away database).
            await db.Database.EnsureCreatedAsync();
            // The real production seed: Headphones, Keyboard, Mouse with FIXED ids, 10 on hand each.
            // Note: it also writes ProductCatalogChanged rows into the Outbox, which is why tests always
            // filter Outbox queries by Type and CorrelationId instead of counting all rows.
            await CatalogSeed.SeedAsync(db, CancellationToken.None);
        }
        // Start the in-memory bus (and the registered background services) - from now on messages flow.
        await Harness.Start();
    }

    /// <summary>
    /// Send a command to the Inventory queue and wait until the consumer has processed it.
    /// After this returns, the consumer's transaction has committed, so the test can query the database.
    /// </summary>
    /// <remarks>
    /// <c>where T : Message</c> is a generic constraint: only MiniShop contracts can be sent, which
    /// guarantees the <c>MessageId</c> property used in the filter below exists.
    /// </remarks>
    public async Task SendAsync<T>(T command) where T : Message
    {
        // Get a "send endpoint" for the queue by its address (e.g. queue:minishop.inventory).  A SEND
        // goes to exactly one queue - the way commands travel in MiniShop.
        var endpoint = await Harness.Bus.GetSendEndpoint(Topology.QueueAddress(Topology.InventoryQueue));
        await endpoint.Send(command);
        // Sending is asynchronous: Send returns as soon as the message is in the queue, NOT when it was
        // handled.  Consumed.Any<T>(filter) waits until the harness has recorded a consumption of a T
        // that matches the filter (or until the harness timeout expires).  We match on OUR contract's
        // MessageId so we wait for THIS message and not some earlier one.
        // (Note: when a test sends the SAME message twice, the second wait is already satisfied by the
        // first consumption.  The short delay below is what gives the duplicate time to be processed.)
        await Harness.Consumed.Any<T>(x => x.Context.Message.MessageId == command.MessageId);
        // The harness records "consumed" from MassTransit's pipeline observers, which can fire a moment
        // before everything around the consumer has settled.  A small pause makes the following database
        // assertions deterministic instead of occasionally racing with the end of the transaction.
        await Task.Delay(100); // let the consumer's transaction finish
    }

    /// <summary>
    /// Run a query against the database in a fresh DI scope (= a fresh DbContext, nothing cached), so
    /// the test sees exactly what was committed, not entities remembered by an earlier context.
    /// </summary>
    /// <remarks>
    /// <c>Func&lt;InventoryDbContext, Task&lt;T&gt;&gt;</c> means "a function that receives the DbContext and
    /// asynchronously returns a T".  Tests pass lambdas such as <c>db =&gt; db.Stock.FirstAsync(...)</c>.
    /// </remarks>
    public async Task<T> QueryAsync<T>(Func<InventoryDbContext, Task<T>> query)
    {
        using var scope = Provider.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<InventoryDbContext>());
    }

    /// <summary>
    /// Called automatically at the end of <c>await using</c>.  Order matters: stop the bus first (no more
    /// messages can arrive), then dispose all services (DbContexts etc.), and close the connection LAST -
    /// closing it is what deletes the in-memory database.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await Harness.Stop();
        await Provider.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
