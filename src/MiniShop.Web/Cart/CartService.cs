// ==================================================================================================
// FILE: src/MiniShop.Web/Cart/CartService.cs
//
// WHAT IT IS
//   The shopping cart of the storefront: which products, how many, total price, and the Idempotency-Key
//   that will be sent with the checkout.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Pure UI state inside MiniShop.Web.  It is NOT a domain object and is not stored in any database:
//   nothing in the system cares about a cart until it is checked out.  At checkout the storefront sends
//   ONE command, POST /api/orders (ServiceClients.PlaceOrderAsync), with these lines and this key; from
//   then on, Orders owns the order.
//
// PATTERN: idempotency key (scenario S14)
//   Networks fail in the worst place: the order may be created but the HTTP answer lost.  The browser
//   (or an impatient double click) then sends the same checkout again.  Because both requests carry the
//   SAME Idempotency-Key, Orders recognises the second one and returns the first order (Duplicate = true)
//   instead of charging the customer twice.  A NEW key is generated only after a successful checkout.
//
// Registered in Program.cs as SCOPED = one cart per Blazor circuit (per browser tab).
//
// Read next: Services/ServiceClients.cs (PlaceOrderAsync).
// ==================================================================================================

using MiniShop.Contracts;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Cart;

/// <summary>
/// The shopping cart lives in the Blazor circuit (scoped service): it is UI state, not domain state.
/// The Idempotency-Key is created once per cart, so a retried checkout (double click, flaky network)
/// can never create a second order (S14).
/// Lost when the tab is closed or the circuit drops; acceptable for a demo cart.
/// </summary>
public sealed class CartService
{
    /// <summary>One line per product id; adding the same product again increases the quantity instead of adding a line.</summary>
    private readonly Dictionary<Guid, OrderLine> _lines = new();

    /// <summary>Who is shopping.  The demo has no login, so it defaults to the seeded "demo-customer".</summary>
    public string CustomerId { get; set; } = "demo-customer";
    /// <summary>
    /// Sent as the Idempotency-Key header of POST /api/orders.  Private setter: only <see cref="Clear"/>
    /// may replace it, after a successful checkout.  "N" format = 32 hex digits without dashes.
    /// </summary>
    public string IdempotencyKey { get; private set; } = Guid.NewGuid().ToString("N");

    /// <summary>The cart lines, read-only for callers.</summary>
    public IReadOnlyCollection<OrderLine> Lines => _lines.Values;
    /// <summary>Cart total (sum of the line totals), in the smallest currency unit.  Indicative only: Orders computes the real amount.</summary>
    public long Total => _lines.Values.Sum(l => l.LineTotal);
    /// <summary>Number of items (sum of quantities), for the cart badge.</summary>
    public int Count => _lines.Values.Sum(l => l.Quantity);

    /// <summary>
    /// Adds <paramref name="quantity"/> units of a product.  Name and price are copied from the read-model
    /// card shown on screen.  No stock check here: the card may be stale, so Inventory decides at checkout.
    /// </summary>
    public void Add(ProductCard product, int quantity)
    {
        // Ignore nonsense input (0 or negative) instead of creating an empty or negative line.
        if (quantity <= 0) return;
        // Existing line for this product, or null.
        var existing = _lines.GetValueOrDefault(product.ProductId);
        var newQuantity = (existing?.Quantity ?? 0) + quantity;
        // OrderLine is an immutable record, so we replace the line instead of mutating it.
        _lines[product.ProductId] = new OrderLine(product.ProductId, product.Name, product.Price, newQuantity);
    }

    /// <summary>Sets the quantity of an existing line; 0 or less removes the line.  Unknown products are ignored.</summary>
    public void SetQuantity(Guid productId, int quantity)
    {
        if (!_lines.TryGetValue(productId, out var line)) return;
        if (quantity <= 0) _lines.Remove(productId);
        // "with" makes a copy of the record with only Quantity changed.
        else _lines[productId] = line with { Quantity = quantity };
    }

    /// <summary>Removes a product from the cart (no-op if absent).</summary>
    public void Remove(Guid productId) => _lines.Remove(productId);

    /// <summary>
    /// After a successful checkout the cart is emptied and a NEW key is generated for the next order.
    /// Do NOT call this after a FAILED checkout: keeping the key is what makes the retry safe (S14).
    /// </summary>
    public void Clear()
    {
        _lines.Clear();
        // A new key = a new, different order next time.  Reusing the old key would make Orders return the
        // previous order as a "duplicate".
        IdempotencyKey = Guid.NewGuid().ToString("N");
    }
}
