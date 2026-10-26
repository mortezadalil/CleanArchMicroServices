using Microsoft.EntityFrameworkCore;
using MiniShop.Shared.Inbox;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Data;

/// <summary>readmodel.db — owned by the Web. Projections, audit log and scenario results.</summary>
public sealed class ReadModelDbContext(DbContextOptions<ReadModelDbContext> options) : DbContext(options), IInboxDbContext
{
    public DbSet<ProductCard> ProductCards => Set<ProductCard>();
    public DbSet<WalletBalance> WalletBalances => Set<WalletBalance>();
    public DbSet<OrderSummary> Orders => Set<OrderSummary>();
    public DbSet<EventAudit> EventAudit => Set<EventAudit>();
    public DbSet<SagaSnapshot> SagaSnapshots => Set<SagaSnapshot>();
    public DbSet<ProjectionCheckpoint> Checkpoints => Set<ProjectionCheckpoint>();
    public DbSet<ScenarioRun> ScenarioRuns => Set<ScenarioRun>();
    public DbSet<ScenarioAssertion> ScenarioAssertions => Set<ScenarioAssertion>();
    /// <summary>ProjectionInbox: prevents applying the same event twice.</summary>
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<ProductCard>(e => { e.HasKey(p => p.ProductId); e.Ignore(p => p.Available); });
        b.Entity<WalletBalance>(e => e.HasKey(w => w.CustomerId));
        b.Entity<OrderSummary>(e =>
        {
            e.HasKey(o => o.OrderId);
            e.HasIndex(o => o.CustomerId);
            e.HasIndex(o => o.ScenarioId);
        });
        b.Entity<EventAudit>(e =>
        {
            e.HasKey(a => a.Seq);
            e.HasIndex(a => a.CorrelationId);
            e.HasIndex(a => a.ScenarioId);
            e.HasIndex(a => a.MessageId);
            e.HasIndex(a => new { a.Category, a.Kind });
        });
        b.Entity<SagaSnapshot>(e => e.HasKey(s => s.CorrelationId));
        b.Entity<ProjectionCheckpoint>(e => e.HasKey(c => c.Projection));
        b.Entity<ScenarioRun>(e => e.HasKey(r => r.Id));
        b.Entity<ScenarioAssertion>(e => e.HasIndex(a => a.RunId));
        b.Entity<InboxMessage>(e =>
        {
            e.ToTable("ProjectionInbox");
            e.HasKey(m => m.MessageId);
        });
    }
}
