// ==================================================================================================
// MiniShop.Orders — Program.cs (the composition root of the Orders service)
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The entry point of the Orders microservice. A "composition root" is the ONE place where all the
//   pieces of a service are wired together: configuration, the database, the message bus, background
//   workers and HTTP endpoints. Nothing in here contains business logic; it only says "who talks to whom".
//
// WHERE IT SITS IN THE ARCHITECTURE
//   MiniShop has four services: Web (storefront + read model), Orders, Inventory and Payments.
//   Orders plays two roles:
//     1. It exposes the "Create Order" HTTP API (POST /api/orders) that the Web calls.
//     2. It is the ORCHESTRATOR of the order saga: it listens to events from Inventory and Payments
//        and decides which command to send next (reserve stock → charge wallet → confirm, or compensate).
//   Orders owns exactly ONE database file, orders.db. No other service ever opens it ("database per
//   service"); other services only learn about orders through messages on RabbitMQ.
//
// PATTERNS WIRED UP HERE
//   - Transactional Outbox : OutboxDispatcher<OrdersDbContext> (shared/Outbox) publishes rows that
//                            the API and the saga stored in the same transaction as their state change.
//   - Inbox / idempotent consumer : every saga consumer derives from IdempotentConsumer (shared/Messaging).
//   - Orchestrated Saga    : OrderSaga (pure decisions) + SagaRunner (persistence) + SagaTimeoutWorker.
//   - CQRS                 : this service owns the WRITE side; the Web builds a separate READ model.
//
// Read next: Api/OrdersEndpoints.cs (the HTTP API), then Saga/OrderSaga.cs (the state machine).
// ==================================================================================================

// MassTransit: the messaging library that talks to RabbitMQ for us (consumers, publishing, retries).
using MassTransit;
// EF Core: the ORM we use to read/write the SQLite database.
using Microsoft.EntityFrameworkCore;
// Shared message contracts (OrderPlaced, StockReserved, ...), queue names (Topology) and ServiceNames.
using MiniShop.Contracts;
// The HTTP API of this service (MapOrdersApi, MapOrdersDiagnostics).
using MiniShop.Orders.Api;
// The EF Core DbContext for orders.db.
using MiniShop.Orders.Data;
// Saga pieces: consumers, runner, timeout worker, options.
using MiniShop.Orders.Saga;
// Development-only control plane helpers (MapDemoGroup, DatabaseReset, health endpoints).
using MiniShop.Shared.Demo;
// Fault injection used by the Scenario Lab (FaultRegistry, FaultingSaveChangesInterceptor).
using MiniShop.Shared.Faults;
// Shared hosting helpers (AddMiniShopBuildingBlocks, ConfigureRabbitMqHost, UseMiniShopDefaults).
using MiniShop.Shared.Messaging;
// The Transactional Outbox building block (OutboxDispatcher).
using MiniShop.Shared.Outbox;

// Creates the ASP.NET Core host builder. It reads configuration from appsettings.json, then
// appsettings.{Environment}.json, then environment variables (e.g. RabbitMq__Host from compose.yaml),
// then command-line args. Later sources override earlier ones.
var builder = WebApplication.CreateBuilder(args);

// ---------------------------------------------------------------------------------------------------
// Building blocks shared by every MiniShop service (see shared/Messaging/ServiceHostingExtensions.cs):
// demo options, RabbitMQ options, the telemetry publisher (DiagnosticsPublisher, singleton), the
// fault registry (singleton), the SaveChanges fault interceptor and the OutboxSignal (singleton).
// Passing ServiceNames.Orders stamps "Orders" on every telemetry record this process emits.
// ---------------------------------------------------------------------------------------------------
builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Orders);

// Binds the "Saga" section of appsettings.json to SagaOptions (timeouts and max compensation attempts).
// Classes then receive IOptions<SagaOptions>; this is the standard .NET "options pattern".
builder.Services.Configure<SagaOptions>(builder.Configuration.GetSection(SagaOptions.Section));

// --- persistence: one SQLite file for this service only ------------------------------------------
// The path comes from configuration ("Database:Path"; in Docker it is /data/orders.db on a volume).
// The "??" fallback keeps `dotnet run` working even if the setting is missing.
var dbPath = builder.Configuration["Database:Path"] ?? "data/orders.db";

// Registers OrdersDbContext with the default SCOPED lifetime: one instance per HTTP request or per
// consumed message. A DbContext is not thread-safe and tracks changes for one unit of work, so it must
// never be a singleton. The (sp, options) overload gives us the service provider so we can resolve
// other services while configuring EF.
builder.Services.AddDbContext<OrdersDbContext>((sp, options) => options
    // Use SQLite with the configured file. SQLite keeps the demo dependency-free (no DB server).
    .UseSqlite($"Data Source={dbPath}")
    // The fault interceptor lets the Scenario Lab make SaveChanges throw on purpose (e.g. S07-style
    // transient DB failures, S12 failing compensation). It does nothing when no fault rule matches.
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// --- messaging ------------------------------------------------------------------------------------
// AddMassTransit registers the bus (IBus / IBusControl), the consumers and a hosted service that
// starts the bus when the app starts and stops it when the app stops.
builder.Services.AddMassTransit(x =>
{
    // One consumer class per event the saga reacts to (see Saga/SagaConsumers.cs). MassTransit
    // registers each as SCOPED: a new consumer instance (and a new DbContext) per message.
    // OrderPlaced is published by this very service (through its own outbox) and consumed again here:
    // that way even the FIRST saga step goes through the Inbox and is crash-safe.
    x.AddConsumer<OrderPlacedConsumer>();
    // Inventory's answers to ReserveStock / ReleaseStock.
    x.AddConsumer<StockReservedConsumer>();
    x.AddConsumer<StockReservationRejectedConsumer>();
    x.AddConsumer<StockReleasedConsumer>();
    // Payments' answers to ChargeWallet / CancelPayment.
    x.AddConsumer<PaymentCompletedConsumer>();
    x.AddConsumer<PaymentFailedConsumer>();
    x.AddConsumer<PaymentCancelledConsumer>();
    // The "you asked me to cancel, but the money was already taken" answer (S13 late payment).
    x.AddConsumer<PaymentAlreadyCompletedConsumer>();

    // Choose RabbitMQ as the transport.
    x.UsingRabbitMq((context, cfg) =>
    {
        // Host, port, user and password come from the "RabbitMq" configuration section.
        cfg.ConfigureRabbitMqHost(context);
        // The saga listens to integration events on its own queue; MassTransit binds the event exchanges to it.
        // ONE explicit queue (Topology.OrdersQueue) for all saga events means messages for one order are
        // handled in arrival order by one endpoint, and students can see exactly one queue in the
        // RabbitMQ UI for Orders.
        cfg.ReceiveEndpoint(Topology.OrdersQueue, e =>
        {
            // Shared endpoint defaults: retry policy (then the "_error" queue), PrefetchCount = 1 and
            // ConcurrentMessageLimit = 1 so a paused consumer shows a real backlog in the queue.
            e.UseMiniShopDefaults();
            // Attach every consumer registered above to this endpoint.
            e.ConfigureConsumers(context);
        });
    });
});

// SagaRunner is SCOPED because it uses the scoped OrdersDbContext: the runner must write into the SAME
// DbContext (and therefore the same transaction) as the consumer or worker that called it.
builder.Services.AddScoped<SagaRunner>();
// Hosted (background) service that moves Outbox rows to RabbitMQ. Hosted services are singletons
// that run for the whole life of the process; it creates its own scope per batch.
builder.Services.AddHostedService<OutboxDispatcher<OrdersDbContext>>();
// Hosted service that fires saga timeouts (deadlines are stored in the DB, so they survive restarts).
builder.Services.AddHostedService<SagaTimeoutWorker>();
// Health checks: "can we open orders.db?" MassTransit adds its own bus check automatically.
// Used by /health/ready (Web's service graph) and, via /health/live, by Docker's healthcheck.
builder.Services.AddHealthChecks().AddDbContextCheck<OrdersDbContext>();

// Freeze the service registrations and build the app (the HTTP pipeline + the DI container).
var app = builder.Build();

// Create the schema on first start (EnsureCreated keeps the demo simple; see README "Limitations").
// EnsureCreated creates all tables from the EF model if the file does not exist yet. It does NOT
// apply migrations, so in a real product you would use EF migrations instead. For a classroom demo
// it means "clone, run, it works" with no migration tooling to learn first.
// We need a scope because OrdersDbContext is scoped and there is no request scope at startup.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    // SQLite creates the file but not the folder; make sure "data/" (or "/data") exists first.
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    await db.Database.EnsureCreatedAsync();
    // WAL (Write-Ahead Logging) mode: readers no longer block the writer and vice versa. Inside this
    // ONE process the HTTP API, the saga consumers, the outbox dispatcher, the timeout worker and the
    // diagnostics endpoints all hit orders.db at the same time; with the default rollback journal they
    // would often see "database is locked". The setting is stored in the file, so running it on every
    // start is harmless.
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
}

// The public API of this service: POST /api/orders and the S12 retry-compensation action.
app.MapOrdersApi();
// /health/live (process is up) and /health/ready (process + database + RabbitMQ are usable).
app.MapHealthEndpoints(ServiceNames.Orders);

// Development-only control plane and read-only diagnostics.
// MapDemoGroup returns null unless Demo:Enabled is true (appsettings.Development.json or compose.yaml
// turn it on). When enabled, every route in the group requires the X-Demo-Token header. In production
// these endpoints simply do not exist, so they cannot be abused. The "is { } demo" pattern means
// "if not null, call it demo".
if (app.MapDemoGroup("/demo") is { } demo)
{
    // GET/POST/DELETE /demo/faults: the Scenario Lab adds and removes fault rules for this service.
    demo.MapFaultEndpoints();
    // POST /demo/reset: wipe this service's data so the class can start a fresh demo.
    demo.MapPost("/reset", async (OrdersDbContext db, FaultRegistry faults) =>
    {
        // Remove every fault rule first so the reset itself cannot be sabotaged by a leftover rule.
        faults.Clear();
        await DatabaseReset.ClearAllTablesAsync(db);   // no orders, no sagas, empty outbox/inbox
        return Results.Ok(new { reset = true });
    });
    // S09 without Docker: stop the bus (consumers + publishing) for a few seconds and start it again.
    // The saga continues from the rows in orders.db, exactly as it would after a process restart.
    // This is the default "restart" used when the services run with `dotnet run` (no container to kill).
    demo.MapPost("/restart-bus", (IBusControl bus, ILogger<Program> logger) =>
    {
        // Fire-and-forget on purpose: we must answer the HTTP call BEFORE the bus stops, otherwise the
        // caller would wait ~13 seconds. The discard "_ =" tells the compiler we do not await it.
        _ = Task.Run(async () =>
        {
            logger.LogWarning("In-process restart requested: stopping the bus");
            // Stop consuming and publishing; messages pile up in RabbitMQ / in the outbox meanwhile.
            await bus.StopAsync(TimeSpan.FromSeconds(10));
            // Stay "down" long enough for students to see the backlog in the RabbitMQ UI.
            await Task.Delay(TimeSpan.FromSeconds(3));
            // Reconnect: queued messages are delivered and the saga picks up where it stopped.
            await bus.StartAsync();
            logger.LogWarning("Bus started again; saga resumes from persisted state");
        });
        // 202 Accepted: "your request was accepted and will happen in the background".
        return Results.Accepted(value: new { restarting = "bus" });
    });

    // S09 with Docker Compose: the container exits and "restart: unless-stopped" brings it back.
    // This is a REAL process death: all in-memory state is lost, which proves that the saga state,
    // its deadlines and the outbox all live in orders.db and nowhere else.
    demo.MapPost("/crash", (IHostApplicationLifetime lifetime, ILogger<Program> logger) =>
    {
        logger.LogWarning("Crash requested by the demo control plane; exiting with code 1");
        // Wait 300 ms so the 202 response below reaches the caller, then kill the process.
        // Exit code 1 (failure) is what makes Docker treat it as a crash and restart the container.
        _ = Task.Run(async () => { await Task.Delay(300); Environment.Exit(1); });
        return Results.Accepted(value: new { crashing = true });
    });
}
// Read-only evidence endpoints (same protection as /demo). The Scenario Lab uses them to check
// invariants directly in the OWNER database, e.g. "exactly one order for this Idempotency-Key".
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    // GET /diagnostics/outbox and /diagnostics/inbox: raw rows of the two pattern tables.
    diagnostics.MapOutboxInboxEndpoints<OrdersDbContext>();
    // GET /diagnostics/orders, /orders/{id}, /sagas, /orders/count (see Api/OrdersEndpoints.cs).
    diagnostics.MapOrdersDiagnostics();
}

// Start Kestrel and block until the process is asked to shut down.
app.Run();

/// <summary>
/// Makes the compiler-generated Program class public and partial, so test projects can reference it
/// (for example with WebApplicationFactory&lt;Program&gt;) and so ILogger&lt;Program&gt; above has a
/// named category.
/// </summary>
public partial class Program;
