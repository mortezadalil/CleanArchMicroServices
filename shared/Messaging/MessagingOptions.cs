namespace MiniShop.Shared.Messaging;

/// <summary>Connection settings read from configuration section "RabbitMq".</summary>
public sealed class RabbitMqOptions
{
    public string Host { get; set; } = "localhost";
    public ushort Port { get; set; } = 5672;
    public string VirtualHost { get; set; } = "/";
    public string Username { get; set; } = "guest";
    public string Password { get; set; } = "guest";
}

/// <summary>
/// Retry policy shared by every consumer.  A failing message is retried in-process after these
/// delays; when they are exhausted MassTransit moves the message to the "{queue}_error" queue
/// (dead-letter / parking).  Total attempts = Intervals.Length + 1.
/// </summary>
public static class RetryPolicy
{
    public static readonly TimeSpan[] Intervals =
    [
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    public static int MaxAttempts => Intervals.Length + 1;
}
