// ==================================================================================================
// FILE: src/MiniShop.Inventory/Domain/Catalog.cs
//
// WHAT THIS FILE IS
//   The domain model (the "nouns") of the Inventory service: Product, Stock, Reservation and
//   ReservationLine, plus the ReservationState enum that describes a reservation's lifecycle.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Inventory is one of four services (Orders, Inventory, Payments, Web).  Each service owns its own
//   database ("database per service"); these classes are the tables of inventory.db.  No other service
//   ever reads or writes them directly: Orders asks Inventory to reserve stock by SENDING a command
//   (ReserveStock) through RabbitMQ, and Inventory answers by PUBLISHING an event (StockReserved or
//   StockReservationRejected).  The Web learns about products only through ProductCatalogChanged events.
//
// WHICH PATTERNS YOU WILL SEE HERE
//   - Reservation (a.k.a. "soft lock"): stock is put aside while the order's payment is still unknown,
//     and is either given back (Released) or really consumed (Committed) later.
//   - Business idempotency key: at most one Reservation per OrderId (unique index, see InventoryDbContext).
//   - Optimistic concurrency: Stock.Version is checked on every UPDATE so two writers cannot both
//     take the last unit (scenario S15).
//   - Tombstone: a Reservation row in state Cancelled that exists only to say "this order was cancelled,
//     refuse any late ReserveStock for it".
//
// These are deliberately plain classes with public setters (an "anemic" model): the rules live in the
// consumers (Consumers/*.cs) so a student can read each use case top to bottom in one place.
//
// Read next: Data/InventoryDbContext.cs (how these classes are mapped to tables and which indexes exist).
// ==================================================================================================

namespace MiniShop.Inventory.Domain;

/// <summary>
/// A product of the shop.  Inventory is the ONLY source of truth for products, demo prices and stock.
/// The Web shows product cards built from ProductCatalogChanged events, and Orders copies the price the
/// customer saw into the order; but when it matters (reserving stock), Inventory checks that copy
/// against THIS price, because a price that came from a browser can never be trusted.
/// </summary>
public sealed class Product
{
    /// <summary>
    /// Primary key.  The demo products have FIXED, well-known ids (see CatalogSeed) so that scenarios,
    /// tests and the Web can refer to "the mouse" without looking it up first.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>Stock-keeping unit, the human/business code of the product (e.g. "MS-001"). Unique in the database.</summary>
    public string Sku { get; set; } = "";

    /// <summary>Display name shown on the product card and in stock snapshots ("Mouse").</summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Current unit price in the smallest unit we use (whole Toman).  A <c>long</c>, never a
    /// <c>double</c>: money must not suffer from floating-point rounding errors.
    /// </summary>
    public long Price { get; set; }

    /// <summary>Currency code of <see cref="Price"/>. "IRT" = Iranian Toman, the only currency of the demo.</summary>
    public string Currency { get; set; } = "IRT";

    /// <summary>
    /// Meant to be bumped on every change of the product's catalog data (e.g. a price change); the demo seeds
    /// it with 1 and has no price-change endpoint yet.  It travels in ProductCatalogChanged and the Web copies
    /// it onto the product card.  Note that the price check does NOT rely on this number: Orders' price
    /// snapshot is compared with the CURRENT <see cref="Price"/> when stock is reserved.
    /// (Not to be confused with <see cref="Stock.Version"/>, which is the concurrency token of the stock row.)
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// False = the product is withdrawn from sale.  ReserveStockConsumer refuses to reserve an inactive
    /// product even if someone still has it in an old cart.
    /// </summary>
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// Stock numbers of one product (one row per product, keyed by ProductId).
///
///   OnHand    = units physically in the warehouse.
///   Reserved  = units promised to orders whose outcome is still unknown (payment pending).
///   Available = OnHand - Reserved = what a NEW order may still take.
///
/// How each operation changes the numbers:
///   Reserve  (ReserveStock)              : Reserved += qty            (only if Available >= qty)
///   Release  (ReleaseStock, compensation): Reserved -= qty            (the units become available again)
///   Commit   (OrderConfirmed)            : OnHand -= qty AND Reserved -= qty  (the goods are really gone)
///
/// Example: OnHand 10, Reserved 0 → reserve 2 → OnHand 10, Reserved 2, Available 8
///          → commit → OnHand 8, Reserved 0, Available 8.   (A release instead would give 10 / 0 / 10.)
///
/// Version is an optimistic-concurrency token so two reservations can never both take the last unit (S15).
/// </summary>
public sealed class Stock
{
    /// <summary>Primary key AND link to <see cref="Product.Id"/>: exactly one stock row per product.</summary>
    public Guid ProductId { get; set; }

    /// <summary>Units physically present in the warehouse. Only a Commit (order confirmed) lowers it.</summary>
    public int OnHand { get; set; }

    /// <summary>Units held for orders that are not finished yet. Raised by Reserve, lowered by Release or Commit.</summary>
    public int Reserved { get; set; }

    /// <summary>
    /// Optimistic-concurrency token.  Every code path that changes this row also does <c>Version++</c>.
    /// EF Core then writes <c>UPDATE Stock SET ..., Version = new WHERE ProductId = @id AND Version = old</c>.
    /// If somebody else changed the row in between, zero rows match, EF throws
    /// DbUpdateConcurrencyException, the whole transaction rolls back, and MassTransit retries the message;
    /// on the retry the consumer reads the fresh numbers and (for the last unit) correctly rejects.
    /// Without it, two writers could both read "Available = 1" and both reserve: stock would go negative.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// Computed, never stored (InventoryDbContext tells EF to ignore it): storing it would create a third
    /// number that could disagree with the other two.
    /// </summary>
    public int Available => OnHand - Reserved;
}

/// <summary>
/// The lifecycle of a <see cref="Reservation"/>.  Normal paths:
///   (none) → Reserved → Committed   (order confirmed: goods leave the warehouse)
///   (none) → Reserved → Released    (payment failed or timed out: compensation gives the units back)
///   (none) → Rejected               (could not reserve: no stock, price changed, unknown product)
///   (none) → Cancelled              (ReleaseStock arrived BEFORE ReserveStock: a tombstone)
/// Rejected, Released, Committed and Cancelled are final: a reservation never leaves them.
/// The enum is stored as a string (see InventoryDbContext) so the database is readable by a human.
/// </summary>
public enum ReservationState
{
    /// <summary>Stock is held for the order (counted in <see cref="Stock.Reserved"/>).</summary>
    Reserved,
    /// <summary>The reservation could not be made (insufficient stock, price mismatch...). Nothing is held.</summary>
    Rejected,
    /// <summary>The reservation was given back (compensation). Nothing is held anymore.</summary>
    Released,
    /// <summary>The order was confirmed: OnHand was reduced; nothing is held anymore.</summary>
    Committed,
    /// <summary>
    /// Tombstone: a release arrived BEFORE any reservation; a late ReserveStock for this order is refused.
    /// Messages can overtake each other (retries, delays), so "cancel" may really arrive before "reserve";
    /// without the tombstone the late reserve would lock stock for an order that no longer exists.
    /// </summary>
    Cancelled,
}

/// <summary>
/// The record of what Inventory decided for one order.  There is at most one reservation per order
/// (unique index on OrderId = the business idempotency key): even if two DIFFERENT ReserveStock messages
/// arrive for the same order (the Inbox only catches the SAME MessageId twice), the second one finds this
/// row and simply repeats the earlier answer.  Rejections and tombstones are stored as rows too, precisely
/// so that "what did we answer?" can always be repeated.
/// </summary>
public sealed class Reservation
{
    /// <summary>Technical primary key of the row.</summary>
    public Guid Id { get; set; }

    /// <summary>The order this reservation belongs to. UNIQUE in the database: one reservation per order.</summary>
    public Guid OrderId { get; set; }

    /// <summary>Where the reservation is in its lifecycle (see <see cref="ReservationState"/>).</summary>
    public ReservationState State { get; set; }

    /// <summary>
    /// Why it was rejected / released / cancelled (null for a successful reservation).  Re-used word for
    /// word when the earlier answer has to be repeated, so the saga always receives the same reason.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>When the row was first written (UTC). Used to sort the /diagnostics/reservations list.</summary>
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>When the state last changed (UTC), e.g. Reserved → Committed.</summary>
    public DateTime UpdatedAtUtc { get; set; }

    /// <summary>
    /// Which products and how many units are held.  Empty for Rejected and Cancelled reservations,
    /// because nothing was ever held for them.  Release and Commit walk these lines to undo / finalize.
    /// </summary>
    public List<ReservationLine> Lines { get; set; } = [];
}

/// <summary>One product + quantity held by a <see cref="Reservation"/> (child table, one row per order line).</summary>
public sealed class ReservationLine
{
    /// <summary>Auto-increment primary key (assigned by SQLite on insert).</summary>
    public long Id { get; set; }

    /// <summary>Foreign key to the owning <see cref="Reservation"/>.</summary>
    public Guid ReservationId { get; set; }

    /// <summary>Which product's stock was reserved.</summary>
    public Guid ProductId { get; set; }

    /// <summary>
    /// How many units were added to <see cref="Stock.Reserved"/>.  Remembered here because the later
    /// ReleaseStock / OrderConfirmed messages carry only the OrderId, not the quantities.
    /// </summary>
    public int Quantity { get; set; }
}
