using Microsoft.EntityFrameworkCore;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Data;

/// <summary>payments.db — owned exclusively by the Payments service.</summary>
public sealed class PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : DbContext(options), IOutboxDbContext, IInboxDbContext
{
    public DbSet<Wallet> Wallets => Set<Wallet>();
    public DbSet<LedgerEntry> Ledger => Set<LedgerEntry>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Wallet>(e =>
        {
            e.HasKey(w => w.CustomerId);
            e.Property(w => w.Version).IsConcurrencyToken();
        });
        b.Entity<LedgerEntry>(e =>
        {
            e.HasIndex(l => new { l.OrderId, l.OperationType }).IsUnique();   // never two debits for one order
            e.Property(l => l.Direction).HasConversion<string>();
            e.Property(l => l.State).HasConversion<string>();
        });
        b.Entity<OutboxMessage>(e =>
        {
            e.HasIndex(m => m.PublishedAtUtc);
            e.HasIndex(m => m.CorrelationId);
        });
        b.Entity<InboxMessage>(e =>
        {
            e.HasKey(m => m.MessageId);
            e.HasIndex(m => m.CorrelationId);
        });
    }

    public async Task<Wallet> GetOrCreateWalletAsync(string customerId, CancellationToken ct)
    {
        var wallet = await Wallets.FirstOrDefaultAsync(w => w.CustomerId == customerId, ct);
        if (wallet is null)
        {
            wallet = new Wallet { CustomerId = customerId, Balance = 0, Version = 1 };
            Wallets.Add(wallet);
        }
        return wallet;
    }
}
