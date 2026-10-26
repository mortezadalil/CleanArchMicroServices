using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Orders.Api;
using MiniShop.Orders.Data;
using MiniShop.Orders.Saga;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

// ==================================================================================================
// MiniShop.Orders — Create Order API + Saga orchestrator.  Owns orders.db.
// ==================================================================================================
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Orders);
builder.Services.Configure<SagaOptions>(builder.Configuration.GetSection(SagaOptions.Section));

// --- persistence: one SQLite file for this service only ------------------------------------------
var dbPath = builder.Configuration["Database:Path"] ?? "data/orders.db";
builder.Services.AddDbContext<OrdersDbContext>((sp, options) => options
    .UseSqlite($"Data Source={dbPath}")
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// --- messaging ------------------------------------------------------------------------------------
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<OrderPlacedConsumer>();
    x.AddConsumer<StockReservedConsumer>();
    x.AddConsumer<StockReservationRejectedConsumer>();
    x.AddConsumer<StockReleasedConsumer>();
    x.AddConsumer<PaymentCompletedConsumer>();
    x.AddConsumer<PaymentFailedConsumer>();
    x.AddConsumer<PaymentCancelledConsumer>();
    x.AddConsumer<PaymentAlreadyCompletedConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ConfigureRabbitMqHost(context);
        // The saga listens to integration events on its own queue; MassTransit binds the event exchanges to it.
        cfg.ReceiveEndpoint(Topology.OrdersQueue, e =>
        {
            e.UseMiniShopDefaults();
            e.ConfigureConsumers(context);
        });
    });
});

builder.Services.AddScoped<SagaRunner>();
builder.Services.AddHostedService<OutboxDispatcher<OrdersDbContext>>();
builder.Services.AddHostedService<SagaTimeoutWorker>();
builder.Services.AddHealthChecks().AddDbContextCheck<OrdersDbContext>();

var app = builder.Build();

// Create the schema on first start (EnsureCreated keeps the demo simple; see README "Limitations").
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    await db.Database.EnsureCreatedAsync();
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
}

app.MapOrdersApi();
app.MapHealthEndpoints(ServiceNames.Orders);

// Development-only control plane and read-only diagnostics.
if (app.MapDemoGroup("/demo") is { } demo)
{
    demo.MapFaultEndpoints();
    demo.MapPost("/reset", async (OrdersDbContext db, FaultRegistry faults) =>
    {
        faults.Clear();
        await DatabaseReset.ClearAllTablesAsync(db);   // no orders, no sagas, empty outbox/inbox
        return Results.Ok(new { reset = true });
    });
    // S09 without Docker: stop the bus (consumers + publishing) for a few seconds and start it again.
    // The saga continues from the rows in orders.db, exactly as it would after a process restart.
    demo.MapPost("/restart-bus", (IBusControl bus, ILogger<Program> logger) =>
    {
        _ = Task.Run(async () =>
        {
            logger.LogWarning("In-process restart requested: stopping the bus");
            await bus.StopAsync(TimeSpan.FromSeconds(10));
            await Task.Delay(TimeSpan.FromSeconds(3));
            await bus.StartAsync();
            logger.LogWarning("Bus started again; saga resumes from persisted state");
        });
        return Results.Accepted(value: new { restarting = "bus" });
    });

    // S09 with Docker Compose: the container exits and "restart: unless-stopped" brings it back.
    demo.MapPost("/crash", (IHostApplicationLifetime lifetime, ILogger<Program> logger) =>
    {
        logger.LogWarning("Crash requested by the demo control plane; exiting with code 1");
        _ = Task.Run(async () => { await Task.Delay(300); Environment.Exit(1); });
        return Results.Accepted(value: new { crashing = true });
    });
}
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    diagnostics.MapOutboxInboxEndpoints<OrdersDbContext>();
    diagnostics.MapOrdersDiagnostics();
}

app.Run();

public partial class Program;
