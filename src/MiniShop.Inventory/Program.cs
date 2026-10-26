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
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Inventory);

var dbPath = builder.Configuration["Database:Path"] ?? "data/inventory.db";
builder.Services.AddDbContext<InventoryDbContext>((sp, options) => options
    .UseSqlite($"Data Source={dbPath}")
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ReserveStockConsumer>();
    x.AddConsumer<ReleaseStockConsumer>();
    x.AddConsumer<OrderConfirmedConsumer>();
    x.AddConsumer<SnapshotRequestedConsumer>();   // a late subscriber asks for the catalog again

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ConfigureRabbitMqHost(context);
        // Commands are SENT to this queue; OrderConfirmed is an event bound to it by subscription.
        cfg.ReceiveEndpoint(Topology.InventoryQueue, e =>
        {
            e.UseMiniShopDefaults();
            e.ConfigureConsumers(context);
        });
    });
});

builder.Services.AddHostedService<OutboxDispatcher<InventoryDbContext>>();
builder.Services.AddHealthChecks().AddDbContextCheck<InventoryDbContext>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    await db.Database.EnsureCreatedAsync();
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
    await CatalogSeed.SeedAsync(db, CancellationToken.None);
}

app.MapHealthEndpoints(ServiceNames.Inventory);

if (app.MapDemoGroup("/demo") is { } demo)
{
    demo.MapFaultEndpoints();

    // Reset to the seed values (stock 10 of each, reservations removed).
    demo.MapPost("/reset", async (InventoryDbContext db, FaultRegistry faults, OutboxSignal signal) =>
    {
        faults.Clear();
        await db.Database.EnsureDeletedAsync();
        await db.Database.EnsureCreatedAsync();
        await CatalogSeed.SeedAsync(db, CancellationToken.None);
        signal.Notify();
        return Results.Ok(new { reset = true });
    });

    // Scenario setup: put a product at an exact level ("last unit" for S15, "0" for S02).
    demo.MapPost("/stock", async (SetStockRequest request, InventoryDbContext db, OutboxSignal signal) =>
    {
        var stock = await db.Stock.FirstOrDefaultAsync(s => s.ProductId == request.ProductId);
        var product = await db.Products.FirstOrDefaultAsync(p => p.Id == request.ProductId);
        if (stock is null || product is null) return Results.NotFound();
        stock.OnHand = request.OnHand;
        stock.Reserved = request.Reserved ?? 0;
        stock.Version++;
        db.PublishViaOutbox(CatalogSeed.CatalogChanged(product, stock, null));
        await db.SaveChangesAsync();
        signal.Notify();
        return Results.Ok(stock);
    });
}
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    diagnostics.MapOutboxInboxEndpoints<InventoryDbContext>();
    diagnostics.MapGet("/stock", async (InventoryDbContext db) => Results.Ok(await db.StockLevelsAsync(CancellationToken.None)));
    diagnostics.MapGet("/products", async (InventoryDbContext db) => Results.Ok(await db.Products.AsNoTracking().OrderBy(p => p.Name).ToListAsync()));
    diagnostics.MapGet("/reservations", async (InventoryDbContext db, Guid? orderId, int take = 50) =>
    {
        var query = db.Reservations.AsNoTracking().Include(r => r.Lines).AsQueryable();
        if (orderId is not null) query = query.Where(r => r.OrderId == orderId);
        return Results.Ok(await query.OrderByDescending(r => r.CreatedAtUtc).Take(take).ToListAsync());
    });
}

app.Run();

/// <summary>Body of POST /demo/stock.</summary>
public sealed record SetStockRequest(Guid ProductId, int OnHand, int? Reserved);
