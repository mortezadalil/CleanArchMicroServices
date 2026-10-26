namespace MiniShop.Orders.Domain;

/// <summary>
/// Lifecycle of an order as the CUSTOMER sees it.
///   Pending → Reserving → AwaitingPayment → Confirmed
///                       ↘ Compensating → Rejected
///   Reserving → Rejected (out of stock)
///   Compensating → RequiresAttention (compensation could not be confirmed)
/// </summary>
public enum OrderState
{
    Pending,
    Reserving,
    AwaitingPayment,
    Compensating,
    Confirmed,
    Rejected,
    RequiresAttention,
}

/// <summary>The Orders aggregate. Prices are a SNAPSHOT of what the customer saw; Inventory re-validates them.</summary>
public sealed class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = "";
    public long Amount { get; set; }
    public string Currency { get; set; } = "IRT";
    public OrderState State { get; set; }
    public string? FailureReason { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    /// <summary>Client-supplied key; a second POST with the same key returns the same order (S14).</summary>
    public string IdempotencyKey { get; set; } = "";
    public Guid? ScenarioId { get; set; }
    public List<OrderItem> Items { get; set; } = [];
}

public sealed class OrderItem
{
    public long Id { get; set; }
    public Guid OrderId { get; set; }
    public Guid ProductId { get; set; }
    public string ProductNameSnapshot { get; set; } = "";
    public long UnitPriceSnapshot { get; set; }
    public int Quantity { get; set; }
}

/// <summary>Append-only history of every order / saga transition (what the customer and the developer console read).</summary>
public sealed class OrderAudit
{
    public long Id { get; set; }
    public Guid OrderId { get; set; }
    public string Trigger { get; set; } = "";      // message type or "Timeout" / "Api"
    public string? OrderFrom { get; set; }
    public string? OrderTo { get; set; }
    public string? SagaFrom { get; set; }
    public string? SagaTo { get; set; }
    public string Detail { get; set; } = "";
    public DateTime AtUtc { get; set; }
}
