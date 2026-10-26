using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Domain;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Data;

/// <summary>inventory.db — owned exclusively by the Inventory service.</summary>
public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options), IOutboxDbContext, IInboxDbContext
{
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Stock> Stock => Set<Stock>();
    public DbSet<Reservation> Reservations => Set<Reservation>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Product>(e => e.HasIndex(p => p.Sku).IsUnique());
        b.Entity<Stock>(e =>
        {
            e.HasKey(s => s.ProductId);
            e.Property(s => s.Version).IsConcurrencyToken();
            e.Ignore(s => s.Available);
        });
        b.Entity<Reservation>(e =>
        {
            e.HasIndex(r => r.OrderId).IsUnique();   // one reservation per order, enforced by the database
            e.Property(r => r.State).HasConversion<string>();
            e.HasMany(r => r.Lines).WithOne().HasForeignKey(l => l.ReservationId);
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

    /// <summary>
    /// Current levels of every product, embedded in events as evidence for the UI's Before/During/After view.
    /// Built from the TRACKED entities, so inside a handler it already reflects the pending (not yet committed)
    /// changes of the current transaction — i.e. the state the event announces.
    /// </summary>
    public async Task<List<StockLevel>> StockLevelsAsync(CancellationToken ct)
    {
        var products = await Products.OrderBy(p => p.Name).ToListAsync(ct);
        var stock = await Stock.ToListAsync(ct);
        return products
            .Select(p => (p, s: stock.First(s => s.ProductId == p.Id)))
            .Select(x => new StockLevel(x.p.Id, x.p.Name, x.s.OnHand, x.s.Reserved))
            .ToList();
    }
}
