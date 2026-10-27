// ==================================================================================================
// FILE: src/MiniShop.Payments/Domain/Wallet.cs
//
// WHAT THIS FILE IS
//   The domain model of the Payments service: a customer's Wallet (current balance) and the
//   append-only Ledger that records every payment decision, plus two small enums.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Payments owns payments.db ("database per service"); nobody else reads these tables.  The Orders saga
//   asks Payments to take money by SENDING a ChargeWallet command, and Payments answers by PUBLISHING
//   PaymentCompleted or PaymentFailed.  If the saga gets no answer in time it sends CancelPayment, and
//   Payments answers PaymentCancelled or PaymentAlreadyCompleted.
//
// WHICH PATTERNS YOU WILL SEE HERE
//   - Wallet + LEDGER: the wallet holds the current balance (fast to read); the ledger is the history
//     (rows are only ever added, never edited away), so you can always explain how a balance came to be.
//   - Business idempotency key: UNIQUE (OrderId, OperationType).  At most ONE "Debit" row per order,
//     even if several DIFFERENT ChargeWallet messages arrive for that order (S06): money is never taken twice.
//   - Refusals are recorded too (state Failed): a refusal is a business ANSWER, not an exception, and it
//     must be repeatable word for word.
//   - Tombstone (state Cancelled): written by CancelPayment when no debit happened yet; it occupies the
//     single "Debit" slot so a ChargeWallet that arrives late can never debit (S10).
//   - Optimistic concurrency: Wallet.Version.
//
// Read next: Data/PaymentsDbContext.cs (tables, unique index, concurrency token).
// ==================================================================================================

namespace MiniShop.Payments.Domain;

/// <summary>
/// A customer's demo wallet (a prepaid balance; no real payment provider is involved).
/// Version is an optimistic-concurrency token: every change does <c>Version++</c>, and EF Core only updates
/// the row if the Version is still the one it read, so two concurrent debits cannot both spend the same money.
/// </summary>
public sealed class Wallet
{
    /// <summary>Primary key: the customer's id (e.g. "demo-customer"). One wallet per customer.</summary>
    public string CustomerId { get; set; } = "";

    /// <summary>Current balance in whole Toman.  A <c>long</c>, never a floating-point type, because money must be exact.</summary>
    public long Balance { get; set; }

    /// <summary>Optimistic-concurrency token (configured in PaymentsDbContext). Bumped on every balance change.</summary>
    public int Version { get; set; }
}

/// <summary>
/// Whether a ledger row takes money out of the wallet or puts money in:
///   Debit  = money leaves the wallet (the order payment);
///   Credit = money enters the wallet (e.g. a refund; not used by the demo yet).
/// </summary>
public enum LedgerDirection { Debit, Credit }

/// <summary>
/// The outcome recorded in a ledger row.  Every state is final: once written, the row answers every
/// later question about this (OrderId, OperationType) pair.
/// </summary>
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
/// The Inbox only de-duplicates the SAME MessageId; this index also protects against DIFFERENT messages
/// asking for the same thing (a saga retry, a duplicated dispatch in S06).
/// </summary>
public sealed class LedgerEntry
{
    /// <summary>Auto-increment primary key; also gives the insertion order (newest = highest).</summary>
    public long Id { get; set; }

    /// <summary>The order this payment decision belongs to. Part of the unique business key.</summary>
    public Guid OrderId { get; set; }

    /// <summary>Whose wallet.  Empty for a Cancelled tombstone (CancelPayment does not know the customer).</summary>
    public string CustomerId { get; set; } = "";

    /// <summary>The order amount in Toman (0 for a tombstone, because nothing was ever going to be charged by it).</summary>
    public long Amount { get; set; }

    /// <summary>Debit or Credit (stored as text in the database).</summary>
    public LedgerDirection Direction { get; set; }

    /// <summary>"Debit" for the order payment; "Refund" would be the compensating credit.</summary>
    /// <remarks>
    /// Part of the unique key together with OrderId: one Debit per order, and (in a future version) one Refund per
    /// order, which could coexist with the Debit because the OperationType differs.
    /// </remarks>
    public string OperationType { get; set; } = "Debit";

    /// <summary>Completed, Failed or Cancelled (stored as text). See <see cref="LedgerState"/>.</summary>
    public LedgerState State { get; set; }

    /// <summary>Why it failed or was cancelled; repeated word for word when the earlier answer is re-published.</summary>
    public string? Reason { get; set; }

    /// <summary>When the row was written (UTC).</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>The OperationType value of an order payment.  A constant so queries never contain a typo like "debit".</summary>
    public const string DebitOperation = "Debit";
}
