namespace MiniShop.Contracts;

/// <summary>
/// The common envelope carried by EVERY message in MiniShop (commands, events and diagnostics).
///
/// Why these fields exist:
///  - MessageId      : unique per message; consumers use it for de-duplication (Inbox).
///  - CorrelationId  : identifies the business flow. In MiniShop it is always the OrderId,
///                     so every message of one order can be traced end-to-end.
///  - CausationId    : the MessageId of the message that caused this one (parent in the chain).
///  - ScenarioId     : set only when the order was started from the Scenario Lab, so that
///                     fault injection can be scoped to a single experiment.
///  - OccurredAtUtc  : when the producer created the message (not when it was delivered).
///  - SchemaVersion  : lets consumers evolve contracts safely.
///  - Producer       : the service that created the message (Orders / Inventory / Payments / Web).
///
/// Messages are immutable C# records: once created they are never mutated.
/// </summary>
public abstract record Message
{
    public Guid MessageId { get; init; } = Guid.NewGuid();
    public Guid CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public Guid? ScenarioId { get; init; }
    public DateTime OccurredAtUtc { get; init; } = DateTime.UtcNow;
    public int SchemaVersion { get; init; } = 1;
    public string Producer { get; init; } = "";

    /// <summary>Short type name, e.g. "OrderPlaced". Used in logs, the outbox table and the UI.</summary>
    public string Type => GetType().Name;
}

/// <summary>One line of an order: product + quantity + the price the customer saw at checkout.</summary>
public sealed record OrderLine(Guid ProductId, string ProductName, long UnitPrice, int Quantity)
{
    public long LineTotal => UnitPrice * Quantity;
}
