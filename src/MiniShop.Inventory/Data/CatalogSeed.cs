// ==================================================================================================
// FILE: src/MiniShop.Inventory/Data/CatalogSeed.cs
//
// WHAT THIS FILE IS
//   The demo data of the Inventory service (three products, 10 units each) plus the helpers that
//   ANNOUNCE the catalog to the rest of the system as ProductCatalogChanged events.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Inventory owns the catalog; the Web only has a read model (product cards) built from events.
//   So whenever the catalog or a stock level changes, Inventory publishes ProductCatalogChanged
//   through its Transactional Outbox, and the Web projects it.  This file is used by:
//     - Program.cs on start-up and by POST /demo/reset          (SeedAsync)
//     - SnapshotRequestedConsumer when a late subscriber asks     (AnnounceCatalogAsync)
//     - every consumer that changes stock, and POST /demo/stock   (CatalogChanged)
//
// PATTERNS: Transactional Outbox (events are rows first, messages later), idempotent seeding,
//           deterministic ids for reproducible scenarios.
//
// Read next: Consumers/ReserveStockConsumer.cs (the heart of the service).
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Inventory.Domain;
using MiniShop.Shared.Outbox;

namespace MiniShop.Inventory.Data;

/// <summary>
/// Deterministic demo catalog: three products with FIXED ids, so scenarios and tests can refer to them.
/// Prices are in Toman (IRT). The seed runs on first start and on POST /demo/reset.
/// </summary>
public static class CatalogSeed
{
    /// <summary>Fixed id of the headphones. Fixed (not Guid.NewGuid()) so it is the same on every machine and after every reset.</summary>
    public static readonly Guid Headphones = Guid.Parse("11111111-1111-1111-1111-111111111111");

    /// <summary>Fixed id of the keyboard.</summary>
    public static readonly Guid Keyboard = Guid.Parse("22222222-2222-2222-2222-222222222222");

    /// <summary>Fixed id of the mouse (the product used by the stock scenarios, e.g. S02 and S15).</summary>
    public static readonly Guid Mouse = Guid.Parse("33333333-3333-3333-3333-333333333333");

    /// <summary>Starting OnHand of every product after a seed or reset.</summary>
    public const int DefaultOnHand = 10;

    /// <summary>
    /// The seed definition: (id, SKU, name, price in Toman).  A property returning a fresh collection
    /// expression keeps the data read-only; the underscores in 500_000 are just digit separators.
    /// </summary>
    public static IReadOnlyList<(Guid Id, string Sku, string Name, long Price)> Products =>
    [
        (Headphones, "HP-001", "Headphones", 500_000),
        (Keyboard, "KB-001", "Keyboard", 800_000),
        (Mouse, "MS-001", "Mouse", 300_000),
    ];

    /// <summary>
    /// Insert products/stock if missing and announce the catalog so the Web can (re)build its product cards.
    /// Idempotent: running it again on a filled database adds nothing, it only re-announces.
    /// </summary>
    /// <param name="db">The Inventory database context (a fresh scope at start-up, or the request's context on reset).</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task SeedAsync(InventoryDbContext db, CancellationToken ct)
    {
        // Go through the three demo products one by one.
        foreach (var (id, sku, name, price) in Products)
        {
            // Insert only what is missing, so a restart never overwrites stock that changed during the demo.
            if (!await db.Products.AnyAsync(p => p.Id == id, ct))
            {
                // Product and its stock row are added together: a product without a stock row would make
                // StockLevelsAsync and the consumers fail.  Version starts at 1 for both.
                db.Products.Add(new Product { Id = id, Sku = sku, Name = name, Price = price, Version = 1 });
                db.Stock.Add(new Stock { ProductId = id, OnHand = DefaultOnHand, Reserved = 0, Version = 1 });
            }
        }
        // First save: the rows must exist in the database before AnnounceCatalogAsync queries them.
        await db.SaveChangesAsync(ct);
        // Queue one ProductCatalogChanged per product in the Outbox table...
        await AnnounceCatalogAsync(db, ct);
        // ...and save those outbox rows. The OutboxDispatcher publishes them shortly after
        // (immediately when the caller calls OutboxSignal.Notify(), otherwise on its next polling round).
        await db.SaveChangesAsync(ct);
    }

    /// <summary>
    /// Queue a ProductCatalogChanged event for every product (idempotent on the read side).
    /// Does NOT save: the caller decides when to commit, so the rows can join the caller's transaction
    /// (for example the IdempotentConsumer transaction in SnapshotRequestedConsumer).
    /// </summary>
    /// <param name="db">The Inventory database context; outbox rows are added to its change tracker.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task AnnounceCatalogAsync(InventoryDbContext db, CancellationToken ct)
    {
        // Read the whole catalog...
        var products = await db.Products.ToListAsync(ct);
        // ...and all stock rows as a dictionary, so each product's stock is found in O(1) below.
        var stock = await db.Stock.ToDictionaryAsync(s => s.ProductId, ct);
        // One event per product.  causation null = nobody's message caused this; it is a "here is the
        // full state" announcement.  Projecting the same event twice is harmless (the Web just overwrites the card).
        foreach (var p in products)
            db.PublishViaOutbox(CatalogChanged(p, stock[p.Id], causation: null));
    }

    /// <summary>
    /// Builds the ProductCatalogChanged event for one product with its current stock numbers.
    /// </summary>
    /// <param name="p">The product (name, SKU, price, version).</param>
    /// <param name="s">Its stock row (OnHand and Reserved; Available is computed by the receiver).</param>
    /// <param name="causation">
    /// The message that caused this change (e.g. the ReserveStock command), or null for a seed / snapshot / manual change.
    /// When given, the event joins that message's trace in the Developer Console.
    /// </param>
    public static ProductCatalogChanged CatalogChanged(Product p, Stock s, Message? causation) =>
        new(p.Id, p.Sku, p.Name, p.Price, s.OnHand, s.Reserved, p.Version)
        {
            // CorrelationId groups messages of one flow.  With a cause it is the order's id (so the stock change
            // shows up in the order's timeline); without one we fall back to the product id.
            CorrelationId = causation?.CorrelationId ?? p.Id,
            // CausationId = "which message made me": the parent link in the message chain.
            CausationId = causation?.MessageId,
            // Carry the Scenario Lab id along so the UI can show this event inside the running experiment.
            ScenarioId = causation?.ScenarioId,
            // Who produced the event (shown in logs, the outbox table and the UI).
            Producer = ServiceNames.Inventory,
        };
}
