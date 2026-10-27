// ==========================================================================================
// FILE: contracts/Topology.cs
//
// WHAT THIS FILE IS
//   The fixed names of the RabbitMQ queues used by MiniShop, and the names of the services.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Part of contracts/ (linked as source into every service).  Each service's Program.cs creates
//   its receive endpoint with one of these queue names; the Outbox stores a queue name in
//   OutboxMessage.DestinationQueue when a COMMAND must be sent to one specific service.
//
// WHICH PATTERN IT IMPLEMENTS
//   Explicit messaging topology: instead of letting the framework invent names, we write them down,
//   so students can open the RabbitMQ management UI (http://localhost:15672) and find exactly these
//   queues, plus the "_error" queues for poison messages.
//
// Read next: shared/Messaging/ServiceHostingExtensions.cs (how endpoints are configured), then any
//            src/*/Program.cs (where the queues below are declared).
// ==========================================================================================

namespace MiniShop.Contracts;

/// <summary>
/// Fixed, documented RabbitMQ names.  MassTransit creates, for each receive endpoint, a queue with
/// this name, an exchange with the same name bound to it, and an error queue "{name}_error" where a
/// message is parked after all retries fail.  Events are published to an exchange named after the
/// message type ("MiniShop.Contracts:OrderPlaced") that fans out to every subscribed queue.
/// </summary>
/// <remarks>
/// <c>static class</c>: only constants and static helpers, never instantiated.
/// <c>const string</c> values are baked into the calling code at compile time, which is fine here
/// because all services compile this same file.
/// </remarks>
public static class Topology
{
    /// <summary>Orders service: the saga consumes every integration event that concerns an order.</summary>
    public const string OrdersQueue = "minishop.orders";          // saga: consumes integration events
    /// <summary>Inventory service: receives the stock commands and the OrderConfirmed event.</summary>
    public const string InventoryQueue = "minishop.inventory";    // commands: ReserveStock / ReleaseStock + OrderConfirmed
    /// <summary>Payments service: receives the wallet commands.</summary>
    public const string PaymentsQueue = "minishop.payments";      // commands: ChargeWallet / CancelPayment
    /// <summary>Web service: builds the read models (CQRS query side) from business events.</summary>
    public const string WebQueue = "minishop.web";                // read-model projections (business events)
    /// <summary>
    /// Web service, telemetry only.  A SEPARATE queue so that a flood of diagnostic records can never
    /// delay the business projections (and a paused projection, S05/S08, does not hide telemetry).
    /// </summary>
    public const string WebDiagnosticsQueue = "minishop.web.diagnostics"; // telemetry only

    /// <summary>MassTransit address of a queue, used when SENDING a command to one specific service.</summary>
    /// <remarks>
    /// "queue:name" is MassTransit's short URI form meaning "the queue with this name on the current broker".
    /// <c>new(...)</c> without a type is a target-typed new (C# 9): the compiler knows the return type is Uri.
    /// The <c>$"..."</c> string is an interpolated string: <c>{queue}</c> is replaced by the parameter's value.
    /// </remarks>
    /// <param name="queue">One of the queue constants above.</param>
    public static Uri QueueAddress(string queue) => new($"queue:{queue}");
}

/// <summary>Service names as they appear in messages, diagnostics and the UI.</summary>
/// <remarks>Used as <see cref="Message.Producer"/> and as the "Service" of diagnostic records; one spelling everywhere avoids typos.</remarks>
public static class ServiceNames
{
    /// <summary>Order placement + the Saga (orchestrator).</summary>
    public const string Orders = "Orders";
    /// <summary>Product catalog and stock reservations.</summary>
    public const string Inventory = "Inventory";
    /// <summary>Wallets and debits.</summary>
    public const string Payments = "Payments";
    /// <summary>Blazor UI, read models, Scenario Lab and Developer Console.</summary>
    public const string Web = "Web";
    /// <summary>The broker itself (a node in the Developer Console graph, not a .NET service).</summary>
    public const string RabbitMq = "RabbitMQ";
}
