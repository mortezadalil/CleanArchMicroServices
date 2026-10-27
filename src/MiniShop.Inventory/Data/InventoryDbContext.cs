// ==================================================================================================
// FILE: src/MiniShop.Inventory/Data/InventoryDbContext.cs
//
// WHAT THIS FILE IS
//   The Entity Framework Core DbContext of the Inventory service, i.e. the gateway to inventory.db
//   (a SQLite file).  It declares the tables and the database-level rules (keys, unique indexes,
//   concurrency tokens) that protect the business invariants even when code has a bug.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   "Database per service": inventory.db belongs to Inventory ONLY.  Besides the business tables
//   (Products, Stock, Reservations) it contains the two messaging tables every MiniShop service has:
//     - Outbox: messages to publish, written in the SAME transaction as the business change
//               (Transactional Outbox pattern, see shared/Outbox/OutboxMessage.cs).
//     - Inbox : MessageIds already processed, so a redelivered message is ignored
//               (Inbox / idempotent consumer pattern, see shared/Messaging/IdempotentConsumer.cs).
//   Because all of them live in one database, a single local transaction can cover
//   "change the stock + remember the message + queue the answer" with no distributed transaction.
//
// Read next: Data/CatalogSeed.cs (the demo data that fills these tables on start).
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Domain;
using MiniShop.Shared.Inbox;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Data;

/// <summary>
/// inventory.db — owned exclusively by the Inventory service.
/// Implements <see cref="IOutboxDbContext"/> (so the shared OutboxDispatcher can read the Outbox table and
/// <c>PublishViaOutbox</c> can add rows) and <see cref="IInboxDbContext"/> (so the shared IdempotentConsumer
/// can check and fill the Inbox table).  The primary constructor receives the options configured in
/// Program.cs (SQLite connection string + the fault-injection interceptor).
/// </summary>
public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options), IOutboxDbContext, IInboxDbContext
{
    /// <summary>The catalog: one row per product (name, SKU, price, active flag).</summary>
    public DbSet<Product> Products => Set<Product>();

    /// <summary>Stock levels: one row per product (OnHand, Reserved, Version).</summary>
    public DbSet<Stock> Stock => Set<Stock>();

    /// <summary>One reservation per order (with its lines); also stores rejections and tombstones.</summary>
    public DbSet<Reservation> Reservations => Set<Reservation>();

    /// <summary>Transactional Outbox: messages waiting to be published to RabbitMQ by the OutboxDispatcher.</summary>
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();

    /// <summary>Inbox: MessageIds already processed by this service (de-duplication of redeliveries).</summary>
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();

    /// <summary>
    /// Fluent configuration of the model.  Called once by EF Core when the model is built; with
    /// <c>EnsureCreated</c> (see Program.cs) the indexes and keys declared here become the real SQLite schema.
    /// </summary>
    protected override void OnModelCreating(ModelBuilder b)
    {
        // Two products can never share a SKU: the database refuses the insert, even if code forgets to check.
        b.Entity<Product>(e => e.HasIndex(p => p.Sku).IsUnique());

        b.Entity<Stock>(e =>
        {
            // The key is ProductId itself (not a separate Id): this makes "one stock row per product" a hard rule.
            e.HasKey(s => s.ProductId);
            // Optimistic concurrency (S15): EF adds "AND Version = <value read>" to every UPDATE of this row.
            // If another transaction changed it in between, no row matches, SaveChanges throws
            // DbUpdateConcurrencyException and the message is retried with fresh numbers.
            e.Property(s => s.Version).IsConcurrencyToken();
            // Available is computed (OnHand - Reserved); there must be no column for it.
            e.Ignore(s => s.Available);
        });

        b.Entity<Reservation>(e =>
        {
            // The BUSINESS idempotency key: one reservation per order, enforced by the database.
            // Even if two different ReserveStock messages for the same order slipped past the code check at
            // the same moment, the second INSERT would fail (→ retry → it then finds the row and re-answers).
            e.HasIndex(r => r.OrderId).IsUnique();
            // Store "Reserved"/"Committed"... as text instead of 0/1/2: readable in a DB browser, and
            // reordering the enum later cannot silently change the meaning of old rows.
            e.Property(r => r.State).HasConversion<string>();
            // One reservation has many lines; each line points back with ReservationId.
            // WithOne() without a navigation: a line does not need a reference to its parent object.
            e.HasMany(r => r.Lines).WithOne().HasForeignKey(l => l.ReservationId);
        });

        b.Entity<OutboxMessage>(e =>
        {
            // The dispatcher constantly asks "which rows are not published yet?" (PublishedAtUtc IS NULL).
            e.HasIndex(m => m.PublishedAtUtc);
            // The diagnostics endpoints filter the outbox by order (CorrelationId = OrderId).
            e.HasIndex(m => m.CorrelationId);
        });

        b.Entity<InboxMessage>(e =>
        {
            // MessageId is the key: inserting the same MessageId twice is impossible, which is the
            // last line of defence against processing one message twice.
            e.HasKey(m => m.MessageId);
            // Lets the diagnostics endpoints list the inbox of one order quickly.
            e.HasIndex(m => m.CorrelationId);
        });
    }

    /// <summary>
    /// Current levels of every product, embedded in events as evidence for the UI's Before/During/After view.
    /// Built from the TRACKED entities, so inside a handler it already reflects the pending (not yet committed)
    /// changes of the current transaction — i.e. the state the event announces.
    /// </summary>
    /// <param name="ct">Cancellation token of the current message / request.</param>
    /// <returns>One <see cref="StockLevel"/> per product, sorted by product name.</returns>
    public async Task<List<StockLevel>> StockLevelsAsync(CancellationToken ct)
    {
        // Load all products (only three in the demo) in a stable order so the UI always lists them the same way.
        var products = await Products.OrderBy(p => p.Name).ToListAsync(ct);
        // Load all stock rows.  EF's identity map returns the SAME tracked objects the handler has just modified,
        // so the numbers include changes that are not saved yet.
        var stock = await Stock.ToListAsync(ct);
        // Pair every product with its stock row and turn the pair into the immutable StockLevel record
        // that travels inside StockReserved / StockReleased / StockReservationRejected.
        return products
            .Select(p => (p, s: stock.First(s => s.ProductId == p.Id)))
            .Select(x => new StockLevel(x.p.Id, x.p.Name, x.s.OnHand, x.s.Reserved))
            .ToList();
    }
}
