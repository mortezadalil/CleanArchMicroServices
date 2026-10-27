// ==================================================================================================
// FILE: src/MiniShop.Web/Data/ReadModelDbContext.cs
//
// WHAT IT IS
//   The EF Core DbContext for readmodel.db, the ONE AND ONLY database owned by MiniShop.Web.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   CQRS read side.  The write side (Orders, Inventory, Payments) each own their own database; the Web
//   never opens those.  Instead it receives their events over RabbitMQ and stores a copy, shaped for
//   the screens, in the tables below.  Because every table here is derived from events, it can be
//   thrown away and rebuilt (see Projections/ReadModelRebuilder.cs).
//
// PATTERNS
//   - CQRS read model (ProductCards, WalletBalances, Orders, SagaSnapshots).
//   - Append-only event log (EventAudit), the source for replay/rebuild.
//   - Inbox for idempotent consumption (the "ProjectionInbox" table, required by IInboxDbContext).
//
// It is registered in Program.cs with AddDbContextFactory, which gives BOTH a scoped context (used by
// message consumers, one per message) and an IDbContextFactory (used by Blazor and background jobs).
//
// Read next: ReadModel/ReadModels.cs (the row classes these tables store).
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Shared.Inbox;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Data;

/// <summary>
/// readmodel.db — owned by the Web. Projections, audit log and scenario results.
/// Implements <see cref="IInboxDbContext"/> so the shared <c>IdempotentConsumer</c> base class can check
/// and record processed message ids in the same transaction as the projection update.
/// </summary>
public sealed class ReadModelDbContext(DbContextOptions<ReadModelDbContext> options) : DbContext(options), IInboxDbContext
{
    /// <summary>Storefront catalog: one card per product, projected from ProductCatalogChanged.</summary>
    public DbSet<ProductCard> ProductCards => Set<ProductCard>();
    /// <summary>Customer wallet balances, projected from WalletBalanceChanged (which carries the final balance).</summary>
    public DbSet<WalletBalance> WalletBalances => Set<WalletBalance>();
    /// <summary>One row per order (list + details), projected from the order/stock/payment events.</summary>
    public DbSet<OrderSummary> Orders => Set<OrderSummary>();
    /// <summary>Append-only log of everything received (business events + telemetry).  Replay/rebuild source.</summary>
    public DbSet<EventAudit> EventAudit => Set<EventAudit>();
    /// <summary>Latest known saga state per order, built from SagaTransitioned diagnostics.</summary>
    public DbSet<SagaSnapshot> SagaSnapshots => Set<SagaSnapshot>();
    /// <summary>Projection progress and lag ("how far behind is the read side?").</summary>
    public DbSet<ProjectionCheckpoint> Checkpoints => Set<ProjectionCheckpoint>();
    /// <summary>Scenario Lab runs (one row per execution of S01..S16).</summary>
    public DbSet<ScenarioRun> ScenarioRuns => Set<ScenarioRun>();
    /// <summary>Invariant checks of each scenario run, with their evidence.</summary>
    public DbSet<ScenarioAssertion> ScenarioAssertions => Set<ScenarioAssertion>();
    /// <summary>
    /// ProjectionInbox: prevents applying the same event twice.
    /// RabbitMQ delivers at-least-once: after a crash or a lost acknowledgement the SAME message
    /// (same MessageId) may arrive again.  The base consumer looks the MessageId up here first and
    /// skips duplicates; it inserts the id in the same transaction as the projection change, so
    /// "applied" and "remembered as applied" can never get out of sync.
    /// </summary>
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    /// <summary>Maps the classes to tables: primary keys, indexes and table names (EnsureCreated builds the schema from this).</summary>
    protected override void OnModelCreating(ModelBuilder b)
    {
        // ProductId is the business key, so an upsert can find the card directly.
        // Available is a computed C# property (OnHand - Reserved): not a column, so it must be ignored.
        b.Entity<ProductCard>(e => { e.HasKey(p => p.ProductId); e.Ignore(p => p.Available); });
        // One balance row per customer.
        b.Entity<WalletBalance>(e => e.HasKey(w => w.CustomerId));
        b.Entity<OrderSummary>(e =>
        {
            // The OrderId (= saga CorrelationId) is the key: every event of an order updates the same row.
            e.HasKey(o => o.OrderId);
            // "My orders" filters by customer.
            e.HasIndex(o => o.CustomerId);
            // The Scenario Lab looks up the orders of one run.
            e.HasIndex(o => o.ScenarioId);
        });
        b.Entity<EventAudit>(e =>
        {
            // Seq is an auto-increment long: it records the order in which the Web RECEIVED things,
            // which is the order a rebuild replays them in.
            e.HasKey(a => a.Seq);
            // Order timeline: everything with the same CorrelationId (= OrderId).
            e.HasIndex(a => a.CorrelationId);
            // Scenario Lab: everything stamped with one ScenarioId.
            e.HasIndex(a => a.ScenarioId);
            // Message Explorer: find a message by id.
            e.HasIndex(a => a.MessageId);
            // Counters such as "how many InboxDuplicateIgnored diagnostics?".
            e.HasIndex(a => new { a.Category, a.Kind });
        });
        // One snapshot per saga instance (CorrelationId = OrderId).
        b.Entity<SagaSnapshot>(e => e.HasKey(s => s.CorrelationId));
        // One checkpoint row per projection name (today only "ReadModel").
        b.Entity<ProjectionCheckpoint>(e => e.HasKey(c => c.Projection));
        // Run Id = the ScenarioId stamped on every message of that run.
        b.Entity<ScenarioRun>(e => e.HasKey(r => r.Id));
        // Assertions are always loaded per run.  (Key = Id by EF convention.)
        b.Entity<ScenarioAssertion>(e => e.HasIndex(a => a.RunId));
        b.Entity<InboxMessage>(e =>
        {
            // Give the shared InboxMessage class a name that says what it is for in THIS service.
            e.ToTable("ProjectionInbox");
            // MessageId as primary key: even if two deliveries raced, the second insert would fail and
            // its transaction would roll back, so a duplicate can never be applied twice.
            e.HasKey(m => m.MessageId);
        });
    }
}
