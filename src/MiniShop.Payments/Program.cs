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
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Payments);

var dbPath = builder.Configuration["Database:Path"] ?? "data/payments.db";
builder.Services.AddDbContext<PaymentsDbContext>((sp, options) => options
    .UseSqlite($"Data Source={dbPath}")
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ChargeWalletConsumer>();
    x.AddConsumer<CancelPaymentConsumer>();
    x.AddConsumer<SnapshotRequestedConsumer>();   // a late subscriber asks for the wallets again

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ConfigureRabbitMqHost(context);
        cfg.ReceiveEndpoint(Topology.PaymentsQueue, e =>
        {
            e.UseMiniShopDefaults();
            e.ConfigureConsumers(context);
        });
    });
});

builder.Services.AddHostedService<OutboxDispatcher<PaymentsDbContext>>();
builder.Services.AddHealthChecks().AddDbContextCheck<PaymentsDbContext>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    await db.Database.EnsureCreatedAsync();
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
    await WalletSeed.SeedAsync(db, CancellationToken.None);
}

app.MapHealthEndpoints(ServiceNames.Payments);

if (app.MapDemoGroup("/demo") is { } demo)
{
    demo.MapFaultEndpoints();

    demo.MapPost("/reset", async (PaymentsDbContext db, FaultRegistry faults, OutboxSignal signal) =>
    {
        faults.Clear();
        await DatabaseReset.ClearAllTablesAsync(db);
        await WalletSeed.SeedAsync(db, CancellationToken.None);    // demo-customer with 2,000,000, announced via the outbox
        signal.Notify();
        return Results.Ok(new { reset = true });
    });

    // Scenario setup: give a (possibly new) customer an exact balance.
    demo.MapPost("/wallets", async (SetWalletRequest request, PaymentsDbContext db, OutboxSignal signal) =>
    {
        var wallet = await db.GetOrCreateWalletAsync(request.CustomerId, CancellationToken.None);
        wallet.Balance = request.Balance;
        wallet.Version++;
        db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, null));
        await db.SaveChangesAsync();
        signal.Notify();
        return Results.Ok(wallet);
    });
}
if (app.MapDemoGroup("/diagnostics") is { } diagnostics)
{
    diagnostics.MapOutboxInboxEndpoints<PaymentsDbContext>();
    diagnostics.MapGet("/wallets", async (PaymentsDbContext db) => Results.Ok(await db.Wallets.AsNoTracking().ToListAsync()));
    diagnostics.MapGet("/wallets/{customerId}", async (string customerId, PaymentsDbContext db) =>
        await db.Wallets.AsNoTracking().FirstOrDefaultAsync(w => w.CustomerId == customerId) is { } w ? Results.Ok(w) : Results.NotFound());
    diagnostics.MapGet("/ledger", async (PaymentsDbContext db, Guid? orderId, string? customerId, int take = 50) =>
    {
        var query = db.Ledger.AsNoTracking().AsQueryable();
        if (orderId is not null) query = query.Where(l => l.OrderId == orderId);
        if (customerId is not null) query = query.Where(l => l.CustomerId == customerId);
        return Results.Ok(await query.OrderByDescending(l => l.Id).Take(take).ToListAsync());
    });
}

app.Run();

/// <summary>Body of POST /demo/wallets.</summary>
public sealed record SetWalletRequest(string CustomerId, long Balance);
