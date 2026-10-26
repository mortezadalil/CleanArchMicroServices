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
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMiniShopBuildingBlocks(builder.Configuration, ServiceNames.Web);
builder.Services.Configure<ServiceUrls>(builder.Configuration.GetSection(ServiceUrls.Section));

builder.Services.AddRazorComponents().AddInteractiveServerComponents();

// --- read model persistence --------------------------------------------------------------------------
var dbPath = builder.Configuration["Database:Path"] ?? "data/readmodel.db";
// AddDbContextFactory registers BOTH a singleton factory (used by Blazor components, the scenario
// runner and background jobs for short-lived contexts) AND the scoped ReadModelDbContext itself.
builder.Services.AddDbContextFactory<ReadModelDbContext>((sp, options) => options
    .UseSqlite($"Data Source={dbPath}")
    .AddInterceptors(sp.GetRequiredService<FaultingSaveChangesInterceptor>()));

// --- messaging: the Web only SUBSCRIBES (business events + diagnostics) -----------------------------
Type[] ProjectionConsumers =
[
    typeof(ProductCatalogChangedProjection), typeof(WalletBalanceChangedProjection), typeof(OrderPlacedProjection),
    typeof(StockReservedProjection), typeof(StockReservationRejectedProjection), typeof(StockReleasedProjection),
    typeof(PaymentCompletedProjection), typeof(PaymentFailedProjection), typeof(PaymentCancelledProjection),
    typeof(PaymentAlreadyCompletedProjection), typeof(OrderConfirmedProjection), typeof(OrderRejectedProjection),
    typeof(OrderRequiresAttentionProjection),
];
builder.Services.AddMassTransit(x =>
{
    x.AddConsumers(ProjectionConsumers);
    x.AddConsumer<DiagnosticEventConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ConfigureRabbitMqHost(context);
        cfg.ReceiveEndpoint(Topology.WebQueue, e =>
        {
            e.UseMiniShopDefaults();
            foreach (var consumerType in ProjectionConsumers)
                e.ConfigureConsumer(context, consumerType);
        });
        // Telemetry has its own queue so a slow/failing projection never delays the Developer Console.
        cfg.ReceiveEndpoint(Topology.WebDiagnosticsQueue, e =>
        {
            e.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
            e.PrefetchCount = 16;
            e.ConfigureConsumer<DiagnosticEventConsumer>(context);
        });
    });
});

builder.Services.AddScoped<Projector>();
builder.Services.AddScoped<ReadModelRebuilder>();
builder.Services.AddScoped<ReadModelQueries>();
builder.Services.AddScoped<CartService>();
builder.Services.AddSingleton<UiNotifier>();
builder.Services.AddSingleton<ScenarioRunner>();
builder.Services.AddHttpClient<ServiceClients>(c => c.Timeout = TimeSpan.FromSeconds(10));
builder.Services.AddSingleton<ServiceHealthMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ServiceHealthMonitor>());
builder.Services.AddHealthChecks().AddDbContextCheck<ReadModelDbContext>();

var app = builder.Build();


using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
    await db.Database.EnsureCreatedAsync();
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");
}

app.UseStaticFiles();
app.UseAntiforgery();
app.MapRazorComponents<App>().AddInteractiveServerRenderMode();
app.MapHealthEndpoints(ServiceNames.Web);

if (app.MapDemoGroup("/demo") is { } demo)
{
    demo.MapFaultEndpoints();   // the Web's own projection faults
    demo.MapDemoApi();
}

app.Run();
