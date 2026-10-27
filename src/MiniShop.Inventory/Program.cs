// ==================================================================================================
// FILE: src/MiniShop.Inventory/Program.cs
//
// WHAT THIS FILE IS
//   The entry point and composition root of the Inventory service: it registers every dependency
//   (dependency injection), connects to RabbitMQ through MassTransit, prepares the SQLite database,
//   and maps the few HTTP endpoints the service has.  Written with "top-level statements": the code
//   below IS the Main method.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   MiniShop has four services: Orders (saga), Inventory (THIS ONE: catalog, stock, reservations),
//   Payments (wallets) and Web (UI + read model).  Inventory is a WORKER: it has no business HTTP API.
//   All business work arrives as messages in ONE RabbitMQ queue, "minishop.inventory":
//     - ReserveStock / ReleaseStock : commands SENT to this queue by the Orders saga
//     - OrderConfirmed              : an event PUBLISHED by Orders; our queue is subscribed to it
//     - ReadModelSnapshotRequested  : an event PUBLISHED by the Web; our queue is subscribed to it
//   All answers leave through the Transactional Outbox (OutboxDispatcher below).
//   The only HTTP endpoints are health checks and a Development-only control plane used by the Web's
//   Scenario Lab / Developer Console (reset, set stock, fault injection, read-only diagnostics).
//
// Read next: Domain/Catalog.cs (the data model), then the Consumers/ folder.
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Consumers;
using MiniShop.Inventory.Data;
using MiniShop.Inventory.Domain;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

// ==================================================================================================
// MiniShop.Inventory — catalog, stock, reservations.  Owns inventory.db.
// It is a worker (no business HTTP API); the HTTP endpoints are only health + Development diagnostics.
// ==================================================================================================

// The builder reads configuration (appsettings.json, appsettings.{Environment}.json, environment
// variables such as RabbitMq__Host set by Docker compose, command-line args) and holds the DI container.
var builder = WebApplication.CreateBuilder(args);

// ---------------------------------------------------------------------------------------------------
// 1. DEPENDENCY INJECTION
// ---------------------------------------------------------------------------------------------------

// Shared building blocks used by every service (see shared/Messaging/ServiceHostingExtensions.cs):
// Demo + RabbitMq options, the telemetry publisher, the in-memory fault registry, the fault-injecting
// SaveChanges interceptor and the OutboxSignal.  The service name tags every telemetry record.
builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Inventory);

// Where the SQLite file lives.  Relative to the working directory locally ("data/inventory.db");
// compose can override it with the environment variable Database__Path (e.g. to a mounted volume).
var dbPath = builder.Configuration["Database:Path"] ?? "data/inventory.db";
// Register InventoryDbContext as a SCOPED service: one instance per HTTP request or per consumed message,
// which is what EF Core expects (a DbContext is not thread-safe and must not live forever).
builder.Services.AddDbContext<InventoryDbContext>((sp, options) => options
    // SQLite: a single file, no server to install. Perfect for a demo; each service has its own file.
    .UseSqlite($"Data Source={dbPath}")
    // Fault injection hook (Development scenarios): the interceptor can make SaveChanges throw on demand
    // (S07 "transient database failure") to prove that a failed transaction leaves no partial writes.
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// MassTransit = the library between our code and RabbitMQ (serialization, topology, retries, error queues).
builder.Services.AddMassTransit(x =>
{
    // Register the consumers (message handlers).  For each consumer MassTransit also knows which message
    // type it handles, which decides the queue bindings created below.
    x.AddConsumer<ReserveStockConsumer>();
    x.AddConsumer<ReleaseStockConsumer>();
    x.AddConsumer<OrderConfirmedConsumer>();
    x.AddConsumer<SnapshotRequestedConsumer>();   // a late subscriber asks for the catalog again

    // Use RabbitMQ as the transport.
    x.UsingRabbitMq((context, cfg) =>
    {
        // Host, port, virtual host and credentials from the "RabbitMq" configuration section.
        cfg.ConfigureRabbitMqHost(context);
        // Commands are SENT to this queue; OrderConfirmed is an event bound to it by subscription.
        // One explicitly named queue ("minishop.inventory") instead of MassTransit's default one-queue-per-consumer
        // naming: the name is fixed and documented in contracts/Topology.cs so the saga knows where to send.
        // For each EVENT a consumer handles (OrderConfirmed, ReadModelSnapshotRequested), MassTransit binds this
        // queue to that event's exchange, so published copies land here.  Commands arrive directly.
        cfg.ReceiveEndpoint(Topology.InventoryQueue, e =>
        {
            // Shared endpoint settings: retry policy (500 ms, 1 s, 2 s, then the "_error" queue), and
            // prefetch/concurrency of 1, so messages are handled one at a time and a paused consumer shows
            // a real backlog in RabbitMQ.
            e.UseMiniShopDefaults();
            // Attach all consumers registered above to this one queue.
            e.ConfigureConsumers(context);
        });
    });
});

// The Transactional Outbox dispatcher: a background service that reads unpublished rows from
// inventory.db's Outbox table and publishes/sends them to RabbitMQ, retrying with backoff.
// This is how every event produced by the consumers actually leaves the service.
builder.Services.AddHostedService<OutboxDispatcher<InventoryDbContext>>();
// Health checks: "can we open inventory.db?" (MassTransit adds its own bus check automatically).
builder.Services.AddHealthChecks().AddDbContextCheck<InventoryDbContext>();

// Freeze the registrations and build the application.
var app = builder.Build();

// ---------------------------------------------------------------------------------------------------
// 2. DATABASE PREPARATION (runs once, before the service starts handling messages)
// ---------------------------------------------------------------------------------------------------

// A DbContext is scoped, and at start-up there is no request/message scope, so we create one ourselves.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    // Make sure the folder of the database file exists ("data/"), otherwise SQLite cannot create the file.
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    // Create the database and all tables from the EF model if they do not exist yet.
    // EnsureCreated is fine for a DEMO; a real system would use EF Core MIGRATIONS so the schema can
    // evolve without deleting data (EnsureCreated never changes an existing database).
    await db.Database.EnsureCreatedAsync();
    // Write-Ahead Logging: readers (diagnostics endpoints, the outbox dispatcher) no longer block the writer
    // and vice versa, which matters because several components use the same file at the same time.
    // The setting is stored in the file, so setting it once per start is enough.
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
    // Insert the three demo products if missing and announce the catalog through the outbox.
    await CatalogSeed.SeedAsync(db, CancellationToken.None);
}

// /health/live and /health/ready, used by Docker compose and by the Web's service graph.
app.MapHealthEndpoints(ServiceNames.Inventory);

// ---------------------------------------------------------------------------------------------------
// 3. DEVELOPMENT-ONLY CONTROL PLANE
// MapDemoGroup returns null unless Demo:Enabled = true (appsettings.Development.json / compose), and every
// endpoint in the group requires the X-Demo-Token header.  In production these endpoints simply do not exist.
// The "is { } demo" pattern means: "if not null, call it demo and run the block".
// ---------------------------------------------------------------------------------------------------
if (app.MapDemoGroup("/demo") is { } demo)
{
    // GET/POST/DELETE /demo/faults: manage fault-injection rules for this service (Scenario Lab).
    demo.MapFaultEndpoints();

    // Reset to the seed values (stock 10 of each, reservations removed).
    // POST /demo/reset: "Reset demo data" button in the Web.
    demo.MapPost("/reset", async (InventoryDbContext db, FaultRegistry faults, OutboxSignal signal) =>
    {
        // Remove every fault rule first, so a forgotten rule cannot disturb the next experiment.
        faults.Clear();
        // Empty every table (products, stock, reservations, inbox, outbox) with DELETE statements.
        // We deliberately do NOT delete the inventory.db file: the service is running, the connection pool and
        // the OutboxDispatcher still hold open connections, and on Linux a deleted file keeps receiving writes
        // through those open handles (the data would silently go nowhere).  Deleting rows inside a transaction
        // is safe while the service runs: other writers just wait for SQLite's write lock.
        await DatabaseReset.ClearAllTablesAsync(db);
        await CatalogSeed.SeedAsync(db, CancellationToken.None);   // 3 products, 10 units each, announced via the outbox
        // Wake the dispatcher so the fresh catalog reaches the Web right away instead of on the next poll.
        signal.Notify();
        return Results.Ok(new { reset = true });
    });

    // Scenario setup: put a product at an exact level ("last unit" for S15, "0" for S02).
    // POST /demo/stock with body { productId, onHand, reserved? }.
    demo.MapPost("/stock", async (SetStockRequest request, InventoryDbContext db, OutboxSignal signal) =>
    {
        // Load the stock row and the product (both are needed to publish ProductCatalogChanged).
        var stock = await db.Stock.FirstOrDefaultAsync(s => s.ProductId == request.ProductId);
        var product = await db.Products.FirstOrDefaultAsync(p => p.Id == request.ProductId);
        // Unknown product id → 404 instead of an exception.
        if (stock is null || product is null) return Results.NotFound();
        // Overwrite the numbers with the requested ones (Reserved defaults to 0 when not given).
        stock.OnHand = request.OnHand;
        stock.Reserved = request.Reserved ?? 0;
        // Bump the concurrency token like every other writer, so a consumer that read the old row cannot
        // overwrite this manual change.
        stock.Version++;
        // Same rule as the consumers: the change is announced through the outbox, in the same SaveChanges,
        // so the Web's product card is updated too (causation null: no message caused this).
        db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, null));
        await db.SaveChangesAsync();
        // Publish the outbox row immediately.
        signal.Notify();
        return Results.Ok(stock);
    });
}
// Read-only diagnostics used by the Developer Console (same Demo:Enabled + token protection).
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    // GET /diagnostics/outbox and /diagnostics/inbox: look inside the messaging tables (optionally per order).
    diagnostics.MapOutboxInboxEndpoints<InventoryDbContext>();
    // GET /diagnostics/stock: OnHand / Reserved / Available of every product.
    diagnostics.MapGet("/stock", async (InventoryDbContext db) => Results.Ok(await db.StockLevelsAsync(CancellationToken.None)));
    // GET /diagnostics/products: the catalog.  AsNoTracking: read-only query, no change tracking needed.
    diagnostics.MapGet("/products", async (InventoryDbContext db) => Results.Ok(await db.Products.AsNoTracking().OrderBy(p => p.Name).ToListAsync()));
    // GET /diagnostics/reservations?orderId=...&take=50: newest reservations first, with their lines.
    diagnostics.MapGet("/reservations", async (InventoryDbContext db, Guid? orderId, int take = 50) =>
    {
        // Build the query step by step; nothing runs against the database until ToListAsync.
        var query = db.Reservations.AsNoTracking().Include(r => r.Lines).AsQueryable();
        // Optional filter: only one order's reservation.
        if (orderId is not null) query = query.Where(r => r.OrderId == orderId);
        return Results.Ok(await query.OrderByDescending(r => r.CreatedAtUtc).Take(take).ToListAsync());
    });
}

// Start the web server, the MassTransit bus (it starts consuming from minishop.inventory) and the
// hosted services (OutboxDispatcher, telemetry worker).  Blocks until the process is asked to stop.
app.Run();

/// <summary>Body of POST /demo/stock.</summary>
/// <param name="ProductId">Which product to change (use the fixed ids from CatalogSeed).</param>
/// <param name="OnHand">New number of units physically in the warehouse.</param>
/// <param name="Reserved">New number of held units; null means 0.</param>
public sealed record SetStockRequest(Guid ProductId, int OnHand, int? Reserved);
