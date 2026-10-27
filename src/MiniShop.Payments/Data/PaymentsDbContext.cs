// ==================================================================================================
// FILE: src/MiniShop.Payments/Data/PaymentsDbContext.cs
//
// WHAT THIS FILE IS
//   The Entity Framework Core DbContext of the Payments service: the gateway to payments.db (SQLite).
//   It declares the tables and the database-level rules that protect the money even if code has a bug:
//   the unique (OrderId, OperationType) index and the Wallet.Version concurrency token.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   "Database per service": payments.db belongs to Payments ONLY.  Besides Wallets and Ledger it holds the
//   two messaging tables every MiniShop service has:
//     - Outbox: answers to publish, written in the SAME transaction as the debit (Transactional Outbox).
//     - Inbox : MessageIds already processed (idempotent consumer, see shared/Messaging/IdempotentConsumer.cs).
//   One database → one local transaction covers "debit + ledger row + inbox row + outbox rows".
//
// Read next: Data/WalletSeed.cs
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Data;

/// <summary>
/// payments.db — owned exclusively by the Payments service.
/// Implements <see cref="IOutboxDbContext"/> (used by the shared OutboxDispatcher and <c>PublishViaOutbox</c>)
/// and <see cref="IInboxDbContext"/> (used by the shared IdempotentConsumer).  The options (SQLite connection
/// string + fault-injection interceptor) are configured in Program.cs.
/// </summary>
public sealed class PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : DbContext(options), IOutboxDbContext, IInboxDbContext
{
    /// <summary>One wallet (current balance) per customer.</summary>
    public DbSet<Wallet> Wallets => Set<Wallet>();

    /// <summary>The append-only history of payment decisions (Completed / Failed / Cancelled).</summary>
    public DbSet<LedgerEntry> Ledger => Set<LedgerEntry>();

    /// <summary>Transactional Outbox: messages waiting to be published to RabbitMQ.</summary>
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    /// <summary>Inbox: MessageIds already processed by this service (de-duplication of redeliveries).</summary>
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    /// <summary>
    /// Fluent configuration of keys, indexes and conversions.  With <c>EnsureCreated</c> (Program.cs) these
    /// become the real SQLite schema, so the rules are enforced by the database itself.
    /// </summary>
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Wallet>(e =>
        {
            // The customer id is the key: one wallet per customer, guaranteed by the database.
            e.HasKey(w => w.CustomerId);
            // Optimistic concurrency: EF adds "AND Version = <value read>" to every UPDATE of a wallet.  If two
            // transactions both read a balance of 500 and both try to spend it, only the first UPDATE matches;
            // the second throws DbUpdateConcurrencyException, rolls back and is retried with the fresh balance.
            e.Property(w => w.Version).IsConcurrencyToken();
        });
        b.Entity<LedgerEntry>(e =>
        {
            // THE money rule: never two debits for one order.  Even if code were wrong, or two different
            // ChargeWallet messages were processed at the same moment, the second INSERT would be refused
            // (→ exception → rollback → retry → the retry finds the row and repeats the earlier answer).
            // The Cancelled tombstone uses the same slot, which is how it blocks a late ChargeWallet.
            e.HasIndex(l => new { l.OrderId, l.OperationType }).IsUnique();   // never two debits for one order
            // Enums stored as readable text ("Debit", "Completed") instead of numbers.
            e.Property(l => l.Direction).HasConversion<string>();
            e.Property(l => l.State).HasConversion<string>();
        });
        b.Entity<OutboxMessage>(e =>
        {
            // The dispatcher keeps asking for unpublished rows (PublishedAtUtc IS NULL).
            e.HasIndex(m => m.PublishedAtUtc);
            // Diagnostics filter the outbox by order (CorrelationId = OrderId).
            e.HasIndex(m => m.CorrelationId);
        });
        b.Entity<InboxMessage>(e =>
        {
            // MessageId as primary key: the same message can never be recorded as processed twice.
            e.HasKey(m => m.MessageId);
            // Diagnostics filter the inbox by order.
            e.HasIndex(m => m.CorrelationId);
        });
    }

    /// <summary>
    /// Returns the customer's wallet, creating an EMPTY one (balance 0) when the customer has none yet.
    /// The new wallet is only added to the change tracker; it is saved by the caller's SaveChanges
    /// (inside the consumer's transaction, or by the /demo/wallets endpoint).
    /// A charge against a brand-new wallet will then simply be refused for insufficient funds.
    /// </summary>
    /// <param name="customerId">The customer whose wallet is needed.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<Wallet> GetOrCreateWalletAsync(string customerId, CancellationToken ct)
    {
        // Try to load the existing (tracked) wallet.
        var wallet = await Wallets.FirstOrDefaultAsync(w => w.CustomerId == customerId, ct);
        if (wallet is null)
        {
            // Unknown customer: start with an empty wallet, Version 1 like every seeded row.
            wallet = new Wallet { CustomerId = customerId, Balance = 0, Version = 1 };
            // Mark it for insertion (written on the next SaveChanges).
            Wallets.Add(wallet);
        }
        return wallet;
    }
}
