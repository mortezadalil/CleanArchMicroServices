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
    public static IServiceCollection AddMiniShopBuildingBlocks(this IServiceCollection services, IConfiguration configuration, string serviceName)
    {
        services.Configure<DemoOptions>(configuration.GetSection(DemoOptions.Section));
        // Enums as names ("Confirmed", not 4) in every HTTP response: easier to read in the console and in tests.
        services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));
        services.Configure<RabbitMqOptions>(configuration.GetSection("RabbitMq"));

        services.AddSingleton(sp => new DiagnosticsPublisher(serviceName, sp.GetRequiredService<ILogger<DiagnosticsPublisher>>()));
        services.AddHostedService<DiagnosticsPublisherWorker>();

        services.AddSingleton(sp => new FaultRegistry(serviceName,
            sp.GetRequiredService<DiagnosticsPublisher>(), sp.GetRequiredService<ILogger<FaultRegistry>>()));
        services.AddSingleton<FaultingSaveChangesInterceptor>();
        services.AddSingleton<OutboxSignal>();
        return services;
    }

    /// <summary>Connects MassTransit to RabbitMQ using the "RabbitMq" configuration section.</summary>
    public static void ConfigureRabbitMqHost(this IRabbitMqBusFactoryConfigurator cfg, IBusRegistrationContext context)
    {
        var options = context.GetRequiredService<Microsoft.Extensions.Options.IOptions<RabbitMqOptions>>().Value;
        cfg.Host(options.Host, options.Port, options.VirtualHost, h =>
        {
            h.Username(options.Username);
            h.Password(options.Password);
        });
    }

    /// <summary>Standard endpoint settings: shared retry policy, one message at a time, error queue by convention.</summary>
    public static void UseMiniShopDefaults(this IRabbitMqReceiveEndpointConfigurator endpoint)
    {
        endpoint.UseMessageRetry(r => r.Intervals(RetryPolicy.Intervals));
        // PrefetchCount 1 keeps unprocessed messages IN the queue, so a paused consumer shows a real backlog.
        endpoint.PrefetchCount = 1;
        endpoint.ConcurrentMessageLimit = 1;
    }
}
