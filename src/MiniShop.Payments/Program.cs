// ==================================================================================================
// FILE: src/MiniShop.Payments/Program.cs
//
// WHAT THIS FILE IS
//   The entry point and composition root of the Payments service: dependency injection, the MassTransit
//   connection to RabbitMQ, SQLite preparation, and the few HTTP endpoints.  Written with top-level
//   statements: the code below IS the Main method.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   MiniShop has four services: Orders (saga), Inventory (stock), Payments (THIS ONE: wallets + ledger)
//   and Web (UI + read model).  Payments is a WORKER: no business HTTP API.  All work arrives as messages
//   in ONE RabbitMQ queue, "minishop.payments":
//     - ChargeWallet / CancelPayment : commands SENT to this queue by the Orders saga
//     - ReadModelSnapshotRequested   : an event PUBLISHED by the Web; our queue is subscribed to it
//   All answers leave through the Transactional Outbox (OutboxDispatcher below).
//   HTTP is used only for health checks and the Development-only control plane (reset, set a wallet
//   balance, fault injection, read-only diagnostics) driven by the Web's Scenario Lab / Developer Console.
//
// Read next: Domain/Wallet.cs, then the Consumers/ folder.
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Consumers;
using MiniShop.Payments.Data;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

// ==================================================================================================
// MiniShop.Payments — wallets and ledger.  Owns payments.db.
// ==================================================================================================

// Reads configuration (appsettings.json, appsettings.{Environment}.json, environment variables such as
// RabbitMq__Host from compose, command-line args) and holds the DI container.
var builder = WebApplication.CreateBuilder(args);

// ---------------------------------------------------------------------------------------------------
// 1. DEPENDENCY INJECTION
// ---------------------------------------------------------------------------------------------------

// Shared building blocks (shared/Messaging/ServiceHostingExtensions.cs): Demo + RabbitMq options, telemetry
// publisher, in-memory fault registry, fault-injecting SaveChanges interceptor, OutboxSignal.
builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Payments);

// Path of the SQLite file: "data/payments.db" locally; compose sets Database__Path=/data/payments.db (a volume).
var dbPath = builder.Configuration["Database:Path"] ?? "data/payments.db";
// Register PaymentsDbContext as SCOPED: one instance per HTTP request / per consumed message.
builder.Services.AddDbContext<PaymentsDbContext>((sp, options) => options
    // SQLite: one file per service, no database server needed for the demo.
    .UseSqlite($"Data Source={dbPath}")
    // Lets the Scenario Lab make SaveChanges throw on demand (transient database failure).
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// MassTransit: serialization, queue/exchange topology, retries and error queues on top of RabbitMQ.
builder.Services.AddMassTransit(x =>
{
    // The message handlers of this service.
    x.AddConsumer<ChargeWalletConsumer>();
    x.AddConsumer<CancelPaymentConsumer>();
    x.AddConsumer<SnapshotRequestedConsumer>();   // a late subscriber asks for the wallets again

    x.UsingRabbitMq((context, cfg) =>
    {
        // Host, port, virtual host, credentials from the "RabbitMq" configuration section.
        cfg.ConfigureRabbitMqHost(context);
        // One explicitly named queue, "minishop.payments" (fixed in contracts/Topology.cs so the saga knows where
        // to SEND ChargeWallet / CancelPayment).  For the EVENT consumer (ReadModelSnapshotRequested) MassTransit
        // also binds this queue to that event's exchange, so published copies land here as well.
        cfg.ReceiveEndpoint(Topology.PaymentsQueue, e =>
        {
            // Shared settings: retries (500 ms, 1 s, 2 s, then the "minishop.payments_error" queue) and one
            // message at a time (prefetch 1), so a paused consumer leaves a visible backlog in RabbitMQ (S05).
            e.UseMiniShopDefaults();
            // Attach all consumers registered above to this queue.
            e.ConfigureConsumers(context);
        });
    });
});

// Background service that publishes the Outbox rows of payments.db to RabbitMQ (with retry/backoff).
// Without it, PaymentCompleted / PaymentFailed would be stored but never leave the service.
builder.Services.AddHostedService<OutboxDispatcher<PaymentsDbContext>>();
// Health checks: can payments.db be opened?  (MassTransit adds its own bus health check.)
builder.Services.AddHealthChecks().AddDbContextCheck<PaymentsDbContext>();

// Freeze the registrations and build the application.
var app = builder.Build();

// ---------------------------------------------------------------------------------------------------
// 2. DATABASE PREPARATION (once, before messages are consumed)
// ---------------------------------------------------------------------------------------------------

// No request/message scope exists at start-up, so create one to get a (scoped) DbContext.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
    // Create the "data/" folder if needed; SQLite cannot create the file in a missing folder.
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    // Create the schema from the EF model if the database does not exist.  Fine for a DEMO; a real system uses
    // EF Core migrations, because EnsureCreated never updates an existing schema.
    await db.Database.EnsureCreatedAsync();
    // Write-Ahead Logging: readers (diagnostics, outbox dispatcher) and the writer (consumers) do not block
    // each other.  The mode is persisted in the file.
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
    // Create the demo wallet if missing and announce all balances through the outbox.
    await WalletSeed.SeedAsync(db, CancellationToken.None);
}

// /health/live and /health/ready for compose and the Web's service graph.
app.MapHealthEndpoints(ServiceNames.Payments);

// ---------------------------------------------------------------------------------------------------
// 3. DEVELOPMENT-ONLY CONTROL PLANE
// MapDemoGroup returns null unless Demo:Enabled = true, and every endpoint requires the X-Demo-Token header.
// In production these endpoints do not exist at all.
// ---------------------------------------------------------------------------------------------------
if (app.MapDemoGroup("/demo") is { } demo)
{
    // GET/POST/DELETE /demo/faults: fault-injection rules of this service.
    demo.MapFaultEndpoints();

    // POST /demo/reset: back to the seed (demo-customer with 2,000,000; empty ledger, inbox and outbox).
    demo.MapPost("/reset", async (PaymentsDbContext db, FaultRegistry faults, OutboxSignal signal) =>
    {
        // Drop all fault rules so nothing from the previous experiment lingers.
        faults.Clear();
        // Empty every table with DELETE statements instead of deleting payments.db: the running service (its
        // connection pool, the OutboxDispatcher) still holds open connections to the file, and writes through
        // those handles to a deleted file would silently be lost.  Row deletes inside one transaction are safe.
        await DatabaseReset.ClearAllTablesAsync(db);
        await WalletSeed.SeedAsync(db, CancellationToken.None);    // demo-customer with 2,000,000, announced via the outbox
        // Publish the fresh balance now instead of on the next dispatcher poll.
        signal.Notify();
        return Results.Ok(new { reset = true });
    });

    // Scenario setup: give a (possibly new) customer an exact balance.
    // POST /demo/wallets with body { customerId, balance }  (S03 sets 100,000 to provoke "insufficient funds").
    demo.MapPost("/wallets", async (SetWalletRequest request, PaymentsDbContext db, OutboxSignal signal) =>
    {
        // Existing wallet, or a new empty one for an unknown customer.
        var wallet = await db.GetOrCreateWalletAsync(request.CustomerId, CancellationToken.None);
        // Overwrite the balance (a manual setup, not a ledger operation).
        wallet.Balance = request.Balance;
        // Bump the concurrency token like every other writer.
        wallet.Version++;
        // Announce the new balance through the outbox in the same SaveChanges, so the Web's wallet summary follows.
        db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, null));
        await db.SaveChangesAsync();
        // Publish it right away.
        signal.Notify();
        return Results.Ok(wallet);
    });
}
// Read-only diagnostics for the Developer Console (same protection).
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    // GET /diagnostics/outbox and /diagnostics/inbox (optionally filtered by order).
    diagnostics.MapOutboxInboxEndpoints<PaymentsDbContext>();
    // GET /diagnostics/wallets: every wallet.  AsNoTracking: read-only, no change tracking needed.
    diagnostics.MapGet("/wallets", async (PaymentsDbContext db) => Results.Ok(await db.Wallets.AsNoTracking().ToListAsync()));
    // GET /diagnostics/wallets/{customerId}: one wallet, or 404.  "is { } w" = "if found, call it w".
    diagnostics.MapGet("/wallets/{customerId}", async (string customerId, PaymentsDbContext db) =>
        await db.Wallets.AsNoTracking().FirstOrDefaultAsync(w => w.CustomerId == customerId) is { } w ? Results.Ok(w) : Results.NotFound());
    // GET /diagnostics/ledger?orderId=...&customerId=...&take=50: newest ledger rows first.
    diagnostics.MapGet("/ledger", async (PaymentsDbContext db, Guid? orderId, string? customerId, int take = 50) =>
    {
        // Compose the query; nothing runs until ToListAsync.
        var query = db.Ledger.AsNoTracking().AsQueryable();
        // Optional filters.
        if (orderId is not null) query = query.Where(l => l.OrderId == orderId);
        if (customerId is not null) query = query.Where(l => l.CustomerId == customerId);
        return Results.Ok(await query.OrderByDescending(l => l.Id).Take(take).ToListAsync());
    });
}

// Start Kestrel, the MassTransit bus (consuming from minishop.payments) and the hosted services
// (OutboxDispatcher, telemetry worker).  Blocks until shutdown.
app.Run();

/// <summary>Body of POST /demo/wallets.</summary>
/// <param name="CustomerId">Whose wallet to set (created if it does not exist).</param>
/// <param name="Balance">The exact new balance in Toman.</param>
public sealed record SetWalletRequest(string CustomerId, long Balance);
