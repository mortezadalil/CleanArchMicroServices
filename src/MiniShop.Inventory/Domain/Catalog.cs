namespace MiniShop.Inventory.Domain;

/// <summary>Inventory is the ONLY source of truth for products, demo prices and stock.</summary>
public sealed class Product
{
    public Guid Id { get; set; }
    public string Sku { get; set; } = "";
    public string Name { get; set; } = "";
    public long Price { get; set; }
    public string Currency { get; set; } = "IRT";
    /// <summary>Bumped on every price change; the Web shows it and Orders' price snapshot is validated against the current price.</summary>
    public int Version { get; set; }
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// Stock numbers of one product.
///   Available = OnHand - Reserved
/// Reserve: Reserved += qty (only if Available >= qty).      Release: Reserved -= qty.
/// Commit (order confirmed): OnHand -= qty AND Reserved -= qty — the goods are really gone.
/// Version is an optimistic-concurrency token so two reservations can never both take the last unit (S15).
/// </summary>
public sealed class Stock
{
    public Guid ProductId { get; set; }
    public int OnHand { get; set; }
    public int Reserved { get; set; }
    public int Version { get; set; }
    public int Available => OnHand - Reserved;
}

public enum ReservationState
{
    /// <summary>Stock is held for the order.</summary>
    Reserved,
    /// <summary>The reservation could not be made (insufficient stock, price mismatch...).</summary>
    Rejected,
    /// <summary>The reservation was given back (compensation).</summary>
    Released,
    /// <summary>The order was confirmed: OnHand was reduced; nothing is held anymore.</summary>
    Committed,
    /// <summary>Tombstone: a release arrived BEFORE any reservation; a late ReserveStock for this order is refused.</summary>
    Cancelled,
}

/// <summary>One reservation per order (unique index on OrderId = the business idempotency key).</summary>
public sealed class Reservation
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public ReservationState State { get; set; }
    public string? Reason { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    public List<ReservationLine> Lines { get; set; } = [];
}

public sealed class ReservationLine
{
    public long Id { get; set; }
    public Guid ReservationId { get; set; }
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
}
