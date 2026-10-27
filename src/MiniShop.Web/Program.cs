// ==================================================================================================
// FILE: src/MiniShop.Web/Program.cs
//
// WHAT IT IS
//   The composition root (startup file) of MiniShop.Web: the storefront that customers use AND the
//   Developer Console / Scenario Lab that students use to watch the distributed system at work.
//   Everything the Web needs is registered here (dependency injection), then the HTTP pipeline and
//   endpoints are mapped, then the app runs.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   MiniShop has four services: Orders, Inventory, Payments (the WRITE side; each owns its own
//   SQLite database) and Web (this one).  The Web is the READ side of CQRS:
//     - it owns exactly one database, readmodel.db, which it fills ONLY from events it receives
//       from RabbitMQ (the "projections");
//     - it NEVER opens orders.db / inventory.db / payments.db.  Services share messages, not tables;
//     - its only command to the write side is the HTTP call POST /api/orders (see ServiceClients),
//       plus a Development-only control plane used by the Scenario Lab.
//
// PATTERNS YOU WILL SEE BEING WIRED UP HERE
//   - CQRS read model (Projections/, Queries/)
//   - Idempotent consumer with an Inbox (here called the ProjectionInbox)
//   - A separate telemetry queue for diagnostics (minishop.web.diagnostics)
//   - Late-subscriber bootstrap (ReadModelBootstrapper)
//   - Health checks (/health/live, /health/ready)
//
// Read next: Projections/EventConsumers.cs (how a business event becomes a read-model row),
//            then Projections/Projector.cs, then Queries/ReadModelQueries.cs.
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Web.Cart;
using MiniShop.Web.Components;
using MiniShop.Web.Data;
using MiniShop.Web.Projections;
using MiniShop.Web.Queries;
using MiniShop.Web.Scenarios;
using MiniShop.Web.Services;

// ==================================================================================================
// MiniShop.Web — storefront + Developer Console.  Owns readmodel.db (CQRS read side).
// ==================================================================================================

// The standard ASP.NET Core builder: reads appsettings.json, appsettings.{Environment}.json,
// environment variables (e.g. Services__Orders=http://orders:8080 in docker-compose) and the
// command line, and gives us a service collection to register things in.
var builder = WebApplication.CreateBuilder(args);

// Shared building blocks used by EVERY MiniShop service (see shared/Messaging/ServiceHostingExtensions.cs):
// DemoOptions, RabbitMqOptions, the DiagnosticsPublisher (telemetry sender) + its background worker,
// the FaultRegistry (fault injection for the Scenario Lab), the FaultingSaveChangesInterceptor and the
// OutboxSignal.  Passing ServiceNames.Web stamps "Web" on every diagnostic record this process emits.
builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Web);

// Bind the "Services" section of appsettings.json (base URLs of Orders, Inventory, Payments, RabbitMQ
// management...) to the ServiceUrls class, so ServiceClients can receive it as IOptions<ServiceUrls>.
builder.Services.Configure<ServiceUrls>(builder.Configuration.GetSection(ServiceUrls.Section));

// Blazor with the "Interactive Server" render mode: the UI runs on the SERVER, and the browser keeps a
// SignalR (WebSocket) connection called a "circuit".  When a component re-renders on the server, the
// DOM diff is pushed over that connection.  That is why UiNotifier can update every open browser tab
// without us writing any SignalR hub or JavaScript.
builder.Services.AddRazorComponents().AddInteractiveServerComponents();

// --- read model persistence --------------------------------------------------------------------------
// Where readmodel.db lives.  Configurable so Docker can put it on a volume; defaults to a local folder.
var dbPath = builder.Configuration["Database:Path"] ?? "data/readmodel.db";
// AddDbContextFactory registers BOTH a singleton factory (used by Blazor components, the scenario
// runner and background jobs for short-lived contexts) AND the scoped ReadModelDbContext itself.
//
// WHY BOTH?
//   - MassTransit creates a DI scope per message, so the projection consumers simply ask for the
//     SCOPED ReadModelDbContext: one context per message, disposed when the message is done.
//   - A Blazor Server "scope" is the whole circuit (it can live for hours) and components may run code
//     concurrently.  A DbContext is NOT thread-safe and grows its change tracker forever, so Blazor
//     code must NOT hold a scoped DbContext; it uses the factory to create a fresh, short-lived
//     context per query instead (see Queries/ReadModelQueries.cs).
// The FaultingSaveChangesInterceptor lets the Scenario Lab make SaveChanges throw on purpose
// (a simulated transient database failure) to prove that a failed transaction writes nothing.
builder.Services.AddDbContextFactory<ReadModelDbContext>((sp, options) => options
    .UseSqlite($"Data Source={dbPath}")
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// --- messaging: the Web SUBSCRIBES to business events + diagnostics; its only publication is ---------
// --- ReadModelSnapshotRequested (see ReadModelBootstrapper) ---------------------------------------------
// The complete list of business events the read model cares about.  Each entry is a tiny consumer class
// (see Projections/EventConsumers.cs).  We list them EXPLICITLY, instead of letting MassTransit scan the
// assembly and call ConfigureEndpoints, for two reasons:
//   1. We decide which queue each consumer lives on.  Automatic configuration would create one queue
//      per consumer; we want all projections on ONE queue (minishop.web) so they are processed one
//      at a time in arrival order, and the diagnostics consumer on a DIFFERENT queue.
//   2. Students can see at a glance exactly which events build the read model.  If you add a new
//      event projection and forget it here, it simply never receives anything.
Type[] ProjectionConsumers =
[
    typeof(ProductCatalogChangedProjection), typeof(WalletBalanceChangedProjection), typeof(OrderPlacedProjection),
    typeof(StockReservedProjection), typeof(StockReservationRejectedProjection), typeof(StockReleasedProjection),
    typeof(PaymentCompletedProjection), typeof(PaymentFailedProjection), typeof(PaymentCancelledProjection),
    typeof(PaymentAlreadyCompletedProjection), typeof(OrderConfirmedProjection), typeof(OrderRejectedProjection),
    typeof(OrderRequiresAttentionProjection),
];
// MassTransit: the library that talks to RabbitMQ for us (exchanges, queues, retries, serialization).
builder.Services.AddMassTransit(x =>
{
    // Register the projection consumers in DI (each will get a fresh scope per message).
    x.AddConsumers(ProjectionConsumers);
    // Register the consumer of telemetry records (DiagnosticEvent) sent by every service.
    x.AddConsumer<DiagnosticEventConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        // Host, port, username and password come from the "RabbitMq" configuration section.
        cfg.ConfigureRabbitMqHost(context);
        // Queue 1: minishop.web — business events that build the read model.
        cfg.ReceiveEndpoint(Topology.WebQueue, e =>
        {
            // Shared defaults: the common retry policy (then the *_error queue), PrefetchCount = 1 and
            // ConcurrentMessageLimit = 1.  One message at a time keeps the projection simple (no two
            // events for the same order race each other in this process) and keeps unprocessed
            // messages visibly IN the queue, so S08 (slow projection) shows a real backlog.
            e.UseMiniShopDefaults();
            // Attach every projection consumer to THIS queue.  MassTransit also binds the queue to the
            // exchange of each event type, which is what makes RabbitMQ copy those events to us.
            foreach (var consumerType in ProjectionConsumers)
                e.ConfigureConsumer(context, consumerType);
        });
        // Telemetry has its own queue so a slow/failing projection never delays the Developer Console.
        // Queue 2: minishop.web.diagnostics — DiagnosticEvent records from every service.
        //   - If telemetry shared minishop.web, then during S08 (projection delayed) or S16 (projection
        //     crashing) the console would ALSO freeze, exactly when the student wants to watch it.
        //   - Telemetry is high volume (several records per business message), so this queue gets a
        //     higher prefetch (16) and the default concurrency: throughput matters more here than
        //     strict one-by-one ordering, and each record is just an append to EventAudit.
        cfg.ReceiveEndpoint(Topology.WebDiagnosticsQueue, e =>
        {
            // Same retry intervals as everywhere else, so a locked SQLite file (busy writer) is retried.
            e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
            // Let RabbitMQ push up to 16 unacknowledged records to us at once.
            e.PrefetchCount = 16;
            e.ConfigureConsumer<DiagnosticEventConsumer>(context);
        });
    });
});

// --- application services -------------------------------------------------------------------------------
// Projector: applies one event to the read-model tables.  SCOPED because it uses the scoped
// ReadModelDbContext of the current message (or of the current rebuild).
builder.Services.AddScoped<Projector>();
// ReadModelRebuilder: wipes the projection tables and replays EventAudit.  Scoped for the same reason.
builder.Services.AddScoped<ReadModelRebuilder>();
builder.Services.AddHostedService<ReadModelBootstrapper>();   // asks the owners for a snapshot while the read model is empty
// ReadModelQueries: the QUERY side of CQRS.  It uses the DbContext FACTORY internally, so it is safe to
// inject into long-lived Blazor components.
builder.Services.AddScoped<ReadModelQueries>();
// CartService: SCOPED = one cart per Blazor circuit (per browser tab).  UI state, not domain state.
builder.Services.AddScoped<CartService>();
// DemoResetService: "Reset demo data" button (Development only).
builder.Services.AddScoped<DemoResetService>();
// UiNotifier: SINGLETON on purpose.  Message consumers (in MassTransit scopes) and Blazor circuits (in
// their own scopes) must see the SAME instance, otherwise a projection would notify nobody.
builder.Services.AddSingleton<UiNotifier>();
// ScenarioRunner: singleton because at most one scenario runs at a time across the whole app, and a
// run must survive the browser tab that started it being closed.
builder.Services.AddSingleton<ScenarioRunner>();
// ServiceClients as a "typed HttpClient": IHttpClientFactory manages the underlying handlers (no socket
// exhaustion, DNS refresh).  10 s timeout so a hung service fails a call instead of freezing a page.
builder.Services.AddHttpClient<ServiceClients>(c => c.Timeout = TimeSpan.FromSeconds(10));
// ServiceHealthMonitor is registered TWICE on purpose:
//   1. as a singleton, so pages and the Scenario Lab can inject it and read the latest health;
//   2. as a hosted service that resolves THAT SAME singleton, so its polling loop runs in background.
// Using AddHostedService<ServiceHealthMonitor>() alone would create a second, unreachable instance.
builder.Services.AddSingleton<ServiceHealthMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ServiceHealthMonitor>());
// Health checks for /health/ready: "can I open readmodel.db?".  MassTransit adds its own bus check
// automatically, so /health/ready also reports whether we are connected to RabbitMQ.
builder.Services.AddHealthChecks().AddDbContextCheck<ReadModelDbContext>();

// Freeze the service collection and build the application.
var app = builder.Build();


// --- create the database before anything uses it -----------------------------------------------------
// A temporary DI scope, because ReadModelDbContext is scoped and we are outside any request.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
    // SQLite creates the FILE on demand but not the FOLDER; without this the first start fails.
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    // EnsureCreated builds the schema from the model if the database does not exist yet.  This is a
    // teaching project with a disposable read model, so we skip EF migrations on purpose: if the schema
    // changes you can delete readmodel.db and it is rebuilt (the owners re-send a snapshot).
    await db.Database.EnsureCreatedAsync();
    // WAL (write-ahead logging) lets readers (Blazor pages) keep reading while a writer (a projection or
    // a telemetry consumer) is writing.  In the default journal mode readers would block on every write.
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
}

// --- HTTP pipeline ---------------------------------------------------------------------------------------
// Serve wwwroot/ (CSS, JS, images).
app.UseStaticFiles();
// Blazor forms require antiforgery tokens; this middleware validates them.
app.UseAntiforgery();
// Map the Blazor root component (Components/App.razor) and enable interactive server rendering.
app.MapRazorComponents<App>().AddInteractiveServerRenderMode();
// /health/live (process is up) and /health/ready (database + RabbitMQ usable), JSON body.
// ServiceHealthMonitor polls the SAME endpoints on the other services to colour the Service Graph.
app.MapHealthEndpoints(ServiceNames.Web);

// --- Development-only control plane ------------------------------------------------------------------
// MapDemoGroup returns null unless Demo:Enabled = true (only appsettings.Development.json sets it),
// and every endpoint in the group requires the X-Demo-Token header.  In Production these routes simply
// do not exist.
if (app.MapDemoGroup("/demo") is { } demo)
{
    demo.MapFaultEndpoints();   // the Web's own projection faults
    // /demo/scenarios..., used by the E2E tests and scripts to drive the Scenario Lab over HTTP.
    demo.MapDemoApi();
}

// Start Kestrel, the MassTransit bus and all hosted services; block until shutdown.
app.Run();
