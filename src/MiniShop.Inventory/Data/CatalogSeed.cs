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
    public static readonly Guid Headphones = Guid.Parse("11111111-1111-1111-1111-111111111111");
    public static readonly Guid Keyboard = Guid.Parse("22222222-2222-2222-2222-222222222222");
    public static readonly Guid Mouse = Guid.Parse("33333333-3333-3333-3333-333333333333");
    public const int DefaultOnHand = 10;

    public static IReadOnlyList<(Guid Id, string Sku, string Name, long Price)> Products =>
    [
        (Headphones, "HP-001", "Headphones", 500_000),
        (Keyboard, "KB-001", "Keyboard", 800_000),
        (Mouse, "MS-001", "Mouse", 300_000),
    ];

    /// <summary>Insert products/stock if missing and announce the catalog so the Web can (re)build its product cards.</summary>
    public static async Task SeedAsync(InventoryDbContext db, CancellationToken ct)
    {
        foreach (var (id, sku, name, price) in Products)
        {
            if (!await db.Products.AnyAsync(p => p.Id == id, ct))
            {
                db.Products.Add(new Product { Id = id, Sku = sku, Name = name, Price = price, Version = 1 });
                db.Stock.Add(new Stock { ProductId = id, OnHand = DefaultOnHand, Reserved = 0, Version = 1 });
            }
        }
        await db.SaveChangesAsync(ct);
        await AnnounceCatalogAsync(db, ct);
        await db.SaveChangesAsync(ct);
    }

    /// <summary>Queue a ProductCatalogChanged event for every product (idempotent on the read side).</summary>
    public static async Task AnnounceCatalogAsync(InventoryDbContext db, CancellationToken ct)
    {
        var products = await db.Products.ToListAsync(ct);
        var stock = await db.Stock.ToDictionaryAsync(s => s.ProductId, ct);
        foreach (var p in products)
            db.PublishViaOutbox(CatalogChanged(p, stock[p.Id], causation: null));
    }

    public static ProductCatalogChanged CatalogChanged(Product p, Stock s, Message? causation) =>
        new(p.Id, p.Sku, p.Name, p.Price, s.OnHand, s.Reserved, p.Version)
        {
            CorrelationId = causation?.CorrelationId ?? p.Id,
            CausationId = causation?.MessageId,
            ScenarioId = causation?.ScenarioId,
            Producer = ServiceNames.Inventory,
        };
}
