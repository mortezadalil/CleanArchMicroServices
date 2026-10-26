namespace MiniShop.Contracts;

/// <summary>
/// Fixed, documented RabbitMQ names.  MassTransit creates, for each receive endpoint, a queue with
/// this name, an exchange with the same name bound to it, and an error queue "{name}_error" where a
/// message is parked after all retries fail.  Events are published to an exchange named after the
/// message type ("MiniShop.Contracts:OrderPlaced") that fans out to every subscribed queue.
/// </summary>
public static class Topology
{
    public const string OrdersQueue = "minishop.orders";          // saga: consumes integration events
    public const string InventoryQueue = "minishop.inventory";    // commands: ReserveStock / ReleaseStock + OrderConfirmed
    public const string PaymentsQueue = "minishop.payments";      // commands: ChargeWallet / CancelPayment
    public const string WebQueue = "minishop.web";                // read-model projections (business events)
    public const string WebDiagnosticsQueue = "minishop.web.diagnostics"; // telemetry only

    /// <summary>MassTransit address of a queue, used when SENDING a command to one specific service.</summary>
    public static Uri QueueAddress(string queue) => new($"queue:{queue}");
}

/// <summary>Service names as they appear in messages, diagnostics and the UI.</summary>
public static class ServiceNames
{
    public const string Orders = "Orders";
    public const string Inventory = "Inventory";
    public const string Payments = "Payments";
    public const string Web = "Web";
    public const string RabbitMq = "RabbitMQ";
}
