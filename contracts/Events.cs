namespace MiniShop.Contracts;

// ------------------------------------------------------------------------------------------
// INTEGRATION EVENTS
// An event is a fact that already happened ("this was done").  It is PUBLISHED, and any number
// of services may subscribe: the Orders saga reacts to them and the Web read model projects them.
// ------------------------------------------------------------------------------------------

/// <summary>Orders: a new order was accepted (state Pending) and the saga must start.</summary>
public sealed record OrderPlaced(Guid OrderId, string CustomerId, long Amount, IReadOnlyList<OrderLine> Lines) : Message;

/// <summary>Inventory: stock was reserved for the order. Carries the stock snapshot as evidence for the UI.</summary>
public sealed record StockReserved(Guid OrderId, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Inventory: the order could not be reserved (out of stock, price mismatch, cancelled...).</summary>
public sealed record StockReservationRejected(Guid OrderId, string Reason, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Inventory: the reservation was given back (or confirmed as never existing).</summary>
public sealed record StockReleased(Guid OrderId, string Reason, IReadOnlyList<StockLevel> StockAfter) : Message;

/// <summary>Payments: the wallet was debited exactly once for the order.</summary>
public sealed record PaymentCompleted(Guid OrderId, string CustomerId, long Amount, long BalanceAfter) : Message;

/// <summary>Payments: the debit was refused (insufficient funds, injected failure, cancelled window...).</summary>
public sealed record PaymentFailed(Guid OrderId, string CustomerId, long Amount, string Reason, long BalanceAfter) : Message;

/// <summary>Payments → Saga (answer to CancelPayment): no debit happened and none can happen anymore.</summary>
public sealed record PaymentCancelled(Guid OrderId, string Reason) : Message;

/// <summary>Payments → Saga (answer to CancelPayment): the debit already happened, so the order must be confirmed.</summary>
public sealed record PaymentAlreadyCompleted(Guid OrderId, long Amount, long BalanceAfter) : Message;

/// <summary>Orders: the saga finished successfully. Inventory turns the reservation into real consumption.</summary>
public sealed record OrderConfirmed(Guid OrderId, string CustomerId, long Amount) : Message;

/// <summary>Orders: the saga finished with a rejection (all compensations confirmed).</summary>
public sealed record OrderRejected(Guid OrderId, string CustomerId, string Reason) : Message;

/// <summary>Orders: compensation could not be confirmed after the allowed attempts; a human must look.</summary>
public sealed record OrderRequiresAttention(Guid OrderId, string CustomerId, string Reason) : Message;

/// <summary>Inventory: the catalog (price / availability) changed. The Web projects product cards from this.</summary>
public sealed record ProductCatalogChanged(Guid ProductId, string Sku, string Name, long Price, int OnHand, int Reserved, int Version) : Message
{
    public int Available => OnHand - Reserved;
}

/// <summary>Payments: wallet balance changed. The Web projects the wallet summary from this.</summary>
public sealed record WalletBalanceChanged(string CustomerId, long Balance) : Message;

/// <summary>Snapshot of one product's stock, embedded in Inventory events as evidence for Before/During/After views.</summary>
public sealed record StockLevel(Guid ProductId, string Name, int OnHand, int Reserved)
{
    public int Available => OnHand - Reserved;
}
