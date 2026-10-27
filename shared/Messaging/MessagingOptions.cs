// ==========================================================================================
// FILE: shared/Messaging/MessagingOptions.cs
//
// WHAT THIS FILE IS
//   Two small pieces of messaging configuration shared by all services:
//     - RabbitMqOptions : how to connect to the broker (bound from appsettings / environment).
//     - RetryPolicy     : how often a failing message is retried before it is dead-lettered.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  RabbitMqOptions is registered in ServiceHostingExtensions.cs and read
//   when the bus connects.  RetryPolicy is used by every receive endpoint (UseMiniShopDefaults) and
//   by IdempotentConsumer to tell "will retry" apart from "moved to the error queue" in telemetry.
//
// WHICH PATTERN IT IMPLEMENTS
//   Retry with fixed intervals + dead-letter (poison message) queue.
//
// Read next: shared/Messaging/ServiceHostingExtensions.cs.
// ==========================================================================================

namespace MiniShop.Shared.Messaging;

/// <summary>Connection settings read from configuration section "RabbitMq".</summary>
/// <remarks>
/// The ASP.NET Core "options pattern": configuration values (appsettings.json, environment variables such
/// as <c>RabbitMq__Host=rabbitmq</c> in compose.yaml) are copied onto these properties by name.
/// The defaults below match a RabbitMQ started locally with its default "guest" account, so the project
/// runs with zero configuration on a developer machine.  Never use guest/guest in production.
/// </remarks>
public sealed class RabbitMqOptions
{
    /// <summary>Broker host name ("localhost" locally, "rabbitmq" inside docker compose).</summary>
    public string Host { get; set; } = "localhost";
    /// <summary>AMQP port (5672 is the RabbitMQ default; 15672 is the management UI, not this).</summary>
    public ushort Port { get; set; } = 5672;
    /// <summary>RabbitMQ virtual host, a namespace for queues/exchanges.  "/" is the default one.</summary>
    public string VirtualHost { get; set; } = "/";
    /// <summary>Broker user name (demo default).</summary>
    public string Username { get; set; } = "guest";
    /// <summary>Broker password (demo default).</summary>
    public string Password { get; set; } = "guest";
}

/// <summary>
/// Retry policy shared by every consumer.  A failing message is retried in-process after these
/// delays; when they are exhausted MassTransit moves the message to the "{queue}_error" queue
/// (dead-letter / parking).  Total attempts = Intervals.Length + 1.
/// </summary>
/// <remarks>
/// Why retry at all: many failures are TRANSIENT (a locked database, a brief network glitch — S07,
/// S16).  Trying again a moment later usually succeeds.  Why stop: a message that ALWAYS fails (a bug,
/// bad data — a "poison message", S10) would otherwise block the queue forever; parking it in the error
/// queue lets the following messages flow and keeps the bad one for a human to inspect or replay.
/// </remarks>
public static class RetryPolicy
{
    /// <summary>Wait 0.5 s, then 1 s, then 2 s between attempts (growing delays give the cause time to go away).</summary>
    /// <remarks>
    /// C# NOTE — collection expression (C# 12): <c>[a, b, c]</c> creates the array directly, the
    /// modern short form of <c>new TimeSpan[] { a, b, c }</c>.
    /// </remarks>
    public static readonly TimeSpan[] Intervals =
    [
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    /// <summary>First delivery + one attempt per interval = 4.  Computed so it can never drift from <see cref="Intervals"/>.</summary>
    public static int MaxAttempts => Intervals.Length + 1;
}
