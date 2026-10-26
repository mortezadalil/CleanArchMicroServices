using MiniShop.Contracts;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Cart;

/// <summary>
/// The shopping cart lives in the Blazor circuit (scoped service): it is UI state, not domain state.
/// The Idempotency-Key is created once per cart, so a retried checkout (double click, flaky network)
/// can never create a second order (S14).
/// </summary>
public sealed class CartService
{
    private readonly Dictionary<Guid, OrderLine> _lines = new();

    public string CustomerId { get; set; } = "demo-customer";
    public string IdempotencyKey { get; private set; } = Guid.NewGuid().ToString("N");

    public IReadOnlyCollection<OrderLine> Lines => _lines.Values;
    public long Total => _lines.Values.Sum(l => l.LineTotal);
    public int Count => _lines.Values.Sum(l => l.Quantity);

    public void Add(ProductCard product, int quantity)
    {
        if (quantity <= 0) return;
        var existing = _lines.GetValueOrDefault(product.ProductId);
        var newQuantity = (existing?.Quantity ?? 0) + quantity;
        _lines[product.ProductId] = new OrderLine(product.ProductId, product.Name, product.Price, newQuantity);
    }

    public void SetQuantity(Guid productId, int quantity)
    {
        if (!_lines.TryGetValue(productId, out var line)) return;
        if (quantity <= 0) _lines.Remove(productId);
        else _lines[productId] = line with { Quantity = quantity };
    }

    public void Remove(Guid productId) => _lines.Remove(productId);

    /// <summary>After a successful checkout the cart is emptied and a NEW key is generated for the next order.</summary>
    public void Clear()
    {
        _lines.Clear();
        IdempotencyKey = Guid.NewGuid().ToString("N");
    }
}
