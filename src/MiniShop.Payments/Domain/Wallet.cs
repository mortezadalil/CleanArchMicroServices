namespace MiniShop.Payments.Domain;

/// <summary>A customer's demo wallet. Version is an optimistic-concurrency token.</summary>
public sealed class Wallet
{
    public string CustomerId { get; set; } = "";
    public long Balance { get; set; }
    public int Version { get; set; }
}

public enum LedgerDirection { Debit, Credit }

public enum LedgerState
{
    /// <summary>Money moved.</summary>
    Completed,
    /// <summary>The operation was refused (insufficient funds, injected failure). No money moved.</summary>
    Failed,
    /// <summary>Tombstone: the saga cancelled the payment window; a late ChargeWallet can never debit.</summary>
    Cancelled,
}

/// <summary>
/// Append-only ledger.  The UNIQUE index on (OrderId, OperationType) is the business idempotency key:
/// however many ChargeWallet messages arrive for one order, at most ONE "Debit" row can exist, so the
/// wallet can never be charged twice for the same order.
/// </summary>
public sealed class LedgerEntry
{
    public long Id { get; set; }
    public Guid OrderId { get; set; }
    public string CustomerId { get; set; } = "";
    public long Amount { get; set; }
    public LedgerDirection Direction { get; set; }
    /// <summary>"Debit" for the order payment; "Refund" would be the compensating credit.</summary>
    public string OperationType { get; set; } = "Debit";
    public LedgerState State { get; set; }
    public string? Reason { get; set; }
    public DateTime CreatedAtUtc { get; set; }

    public const string DebitOperation = "Debit";
}
