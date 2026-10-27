// ==========================================================================================
// FILE: shared/Messaging/ServiceHostingExtensions.cs
//
// WHAT THIS FILE IS
//   Extension methods that every service's Program.cs calls to register the shared building blocks
//   (telemetry, fault injection, outbox signal, demo options) and to configure MassTransit/RabbitMQ
//   the same way everywhere.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  It is the "composition root helper": it wires the classes from
//   shared/Diagnostics, shared/Faults, shared/Outbox and shared/Demo into dependency injection.
//
// WHICH PATTERN IT IMPLEMENTS
//   Consistent consumer endpoint configuration: retry policy + error queue (poison messages) +
//   one-message-at-a-time processing.
//
// C# NOTE — extension methods:
//   A static method in a static class whose first parameter has the <c>this</c> modifier
//   (<c>this IServiceCollection services</c>) can be called AS IF it were an instance method of that
//   type: <c>builder.Services.AddMiniShopBuildingBlocks(...)</c>.  It is only syntax sugar; the
//   compiler turns it into <c>ServiceHostingExtensions.AddMiniShopBuildingBlocks(builder.Services, ...)</c>.
//
// Read next: src/MiniShop.Orders/Program.cs (a caller), then shared/Diagnostics/DiagnosticsPublisher.cs.
// ==========================================================================================

using MassTransit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MiniShop.Shared.Demo;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Outbox;

namespace MiniShop.Shared.Messaging;

/// <summary>Registration helpers so every Program.cs reads the same way.</summary>
public static class ServiceHostingExtensions
{
    /// <summary>Registers telemetry, fault injection, outbox signal and demo options for a service.</summary>
    /// <param name="services">The DI container being built in Program.cs.</param>
    /// <param name="configuration">App configuration (appsettings + environment variables).</param>
    /// <param name="serviceName">One of <c>ServiceNames</c>; stamped on every diagnostic record and fault rule of this process.</param>
    /// <returns>The same collection, so calls can be chained.</returns>
    public static IServiceCollection AddMiniShopBuildingBlocks(this IServiceCollection services, IConfiguration configuration, string serviceName)
    {
        // Bind the "Demo" section (Enabled, Token) to DemoOptions: controls the Development-only control plane.
        services.Configure<DemoOptions>(configuration.GetSection(DemoOptions.Section));
        // Enums as names ("Confirmed", not 4) in every HTTP response: easier to read in the console and in tests.
        services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
        // Bind the "RabbitMq" section (Host, Port, ...) to RabbitMqOptions; read later by ConfigureRabbitMqHost.
        services.Configure<RabbitMqOptions>(configuration.GetSection("RabbitMq"));

        // ONE DiagnosticsPublisher per process (singleton): its in-memory queue must be shared by every
        // consumer, the outbox dispatcher and the fault registry.  A factory lambda is used because the
        // constructor needs the plain string serviceName, which DI cannot resolve by itself.
        services.AddSingleton(sp => new DiagnosticsPublisher(serviceName, sp.GetRequiredService<ILogger<DiagnosticsPublisher>>()));
        // The background worker that drains that queue to RabbitMQ (business code never waits for it).
        services.AddHostedService<DiagnosticsPublisherWorker>();

        // ONE FaultRegistry per process (singleton): fault rules live in memory and must be visible to every
        // consumer and to the /demo/faults endpoints at the same time.
        services.AddSingleton(sp => new FaultRegistry(serviceName,
            sp.GetRequiredService<DiagnosticsPublisher>(), sp.GetRequiredService<ILogger<FaultRegistry>>()));
        // The EF Core interceptor that can make SaveChanges fail on purpose (S07).  Each Program.cs adds it
        // to its DbContext with AddInterceptors(...).
        services.AddSingleton<FaultingSaveChangesInterceptor>();
        // The "wake up" signal between committed writers and the outbox dispatcher (singleton: both sides
        // must see the SAME semaphore).
        services.AddSingleton<OutboxSignal>();
        return services;
    }

    /// <summary>Connects MassTransit to RabbitMQ using the "RabbitMq" configuration section.</summary>
    /// <param name="cfg">MassTransit's RabbitMQ bus configurator (inside <c>x.UsingRabbitMq((context, cfg) => ...)</c>).</param>
    /// <param name="context">Gives access to DI so we can read the bound options.</param>
    public static void ConfigureRabbitMqHost(this IRabbitMqBusFactoryConfigurator cfg, IBusRegistrationContext context)
    {
        // IOptions<T>.Value returns the RabbitMqOptions instance filled from configuration.
        var options = context.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>().Value;
        // Tell MassTransit where the broker is; the lambda sets the credentials.  MassTransit keeps
        // reconnecting by itself if the broker goes away (S04), so no extra code is needed here.
        cfg.Host(options.Host, options.Port, options.VirtualHost, h =>
        {
            h.Username(options.Username);
            h.Password(options.Password);
        });
    }

    /// <summary>Standard endpoint settings: shared retry policy, one message at a time, error queue by convention.</summary>
    /// <remarks>
    /// The error queue needs no line of code: when the retry middleware gives up, MassTransit moves the
    /// message to "{queue}_error" automatically.
    /// </remarks>
    /// <param name="endpoint">The receive endpoint (one queue) being configured in Program.cs.</param>
    public static void UseMiniShopDefaults(this IRabbitMqReceiveEndpointConfigurator endpoint)
    {
        // In-process retries with the shared delays (0.5 s, 1 s, 2 s).  Each retry calls Consume again,
        // which opens a NEW database transaction, so a failed attempt never leaves partial writes (S07).
        endpoint.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
        // PrefetchCount 1 keeps unprocessed messages IN the queue, so a paused consumer shows a real backlog.
        endpoint.PrefetchCount = 1;
        // Process one message at a time per endpoint.  With SQLite there is only one writer anyway, and a
        // strictly sequential consumer makes the timeline in the Developer Console easy to follow.
        endpoint.ConcurrentMessageLimit = 1;
    }
}
