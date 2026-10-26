using Microsoft.EntityFrameworkCore;
using MiniShop.Orders.Domain;
using MiniShop.Orders.Saga;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Data;

/// <summary>orders.db — owned exclusively by the Orders service. No other service opens this file.</summary>
public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options), IOutboxDbContext, IInboxDbContext
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderItem> OrderItems => Set<OrderItem>();
    public DbSet<OrderSagaState> SagaInstances => Set<OrderSagaState>();
    public DbSet<OrderAudit> OrderAudit => Set<OrderAudit>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Order>(e =>
        {
            e.HasKey(o => o.Id);
            e.HasIndex(o => o.IdempotencyKey).IsUnique();   // S14: one order per Idempotency-Key
            e.Property(o => o.State).HasConversion<string>();
            e.HasMany(o => o.Items).WithOne().HasForeignKey(i => i.OrderId);
        });
        b.Entity<OrderSagaState>(e =>
        {
            e.HasKey(s => s.CorrelationId);
            e.Property(s => s.Version).IsConcurrencyToken();
            e.HasIndex(s => s.DeadlineUtc);
        });
        b.Entity<OrderAudit>(e => e.HasIndex(a => a.OrderId));
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
}
