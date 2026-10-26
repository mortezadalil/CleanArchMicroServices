using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Messaging;
using MiniShop.Web.Data;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Projections;

/// <summary>
/// Applies ONE business event to the read-model tables.  Rules that make projections safe under
/// at-least-once, out-of-order delivery:
///   - upserts only (never "insert or fail");
///   - an older event never overwrites a newer row (compare OccurredAtUtc);
///   - a terminal order state (Confirmed / Rejected / RequiresAttention) is never downgraded.
/// The same code is used live (from RabbitMQ) and during a rebuild (from EventAudit).
/// </summary>
public sealed class Projector(ReadModelDbContext db)
{
    public async Task<string> ApplyAsync(Message @event, CancellationToken ct)
    {
        switch (@event)
        {
            case ProductCatalogChanged e:
                return await UpsertProductAsync(e, ct);
            case WalletBalanceChanged e:
                return await UpsertWalletAsync(e, ct);
            case OrderPlaced e:
                return await UpsertOrderAsync(e.OrderId, e, "Pending", null, o =>
                {
                    o.CustomerId = e.CustomerId;
                    o.Amount = e.Amount;
                    o.LinesJson = Json.Serialize(e.Lines);
                    o.ScenarioId = e.ScenarioId;
                    o.CreatedAtUtc = e.OccurredAtUtc;
                }, ct);
            case StockReserved e:
                return await UpsertOrderAsync(e.OrderId, e, "AwaitingPayment", null, null, ct);
            case StockReservationRejected e:
                return await UpsertOrderAsync(e.OrderId, e, "Rejecting", e.Reason, null, ct);
            case PaymentFailed e:
                return await UpsertOrderAsync(e.OrderId, e, "Compensating", e.Reason, null, ct);
            case PaymentCancelled e:
                return await UpsertOrderAsync(e.OrderId, e, "Compensating", e.Reason, null, ct);
            case OrderConfirmed e:
                return await UpsertOrderAsync(e.OrderId, e, "Confirmed", null, null, ct);
            case OrderRejected e:
                return await UpsertOrderAsync(e.OrderId, e, "Rejected", e.Reason, null, ct);
            case OrderRequiresAttention e:
                return await UpsertOrderAsync(e.OrderId, e, "RequiresAttention", e.Reason, null, ct);
            default:
                return $"{@event.Type}: no projection"; // PaymentCompleted, StockReleased... only feed the timeline
        }
    }

    private static readonly HashSet<string> TerminalStates = ["Confirmed", "Rejected", "RequiresAttention"];

    private async Task<string> UpsertOrderAsync(Guid orderId, Message e, string newState, string? reason,
        Action<OrderSummary>? init, CancellationToken ct)
    {
        var order = await db.Orders.FirstOrDefaultAsync(o => o.OrderId == orderId, ct);
        if (order is null)
        {
            order = new OrderSummary { OrderId = orderId, CreatedAtUtc = e.OccurredAtUtc, ScenarioId = e.ScenarioId };
            db.Orders.Add(order);
        }
        init?.Invoke(order);

        // Ordering guards: never go back in time, never leave a terminal state.
        var isOlder = e.OccurredAtUtc < order.UpdatedAtUtc;
        var wouldDowngrade = TerminalStates.Contains(order.State) && !TerminalStates.Contains(newState);
        if (isOlder || wouldDowngrade)
            return $"{e.Type}: ignored for projection (order already {order.State}, newer)";

        order.State = newState;
        order.FailureReason = reason ?? order.FailureReason;
        order.UpdatedAtUtc = e.OccurredAtUtc;
        order.LastEvent = e.Type;
        return $"{e.Type}: order {orderId:N} → {newState}";
    }

    private async Task<string> UpsertProductAsync(ProductCatalogChanged e, CancellationToken ct)
    {
        var card = await db.ProductCards.FirstOrDefaultAsync(p => p.ProductId == e.ProductId, ct);
        if (card is null)
        {
            card = new ProductCard { ProductId = e.ProductId };
            db.ProductCards.Add(card);
        }
        else if (e.OccurredAtUtc < card.UpdatedAtUtc)
            return "ProductCatalogChanged: older than current card, ignored";

        card.Sku = e.Sku;
        card.Name = e.Name;
        card.Price = e.Price;
        card.OnHand = e.OnHand;
        card.Reserved = e.Reserved;
        card.Version = e.Version;
        card.UpdatedAtUtc = e.OccurredAtUtc;
        return $"ProductCard {e.Name}: onHand {e.OnHand}, reserved {e.Reserved}";
    }

    private async Task<string> UpsertWalletAsync(WalletBalanceChanged e, CancellationToken ct)
    {
        var wallet = await db.WalletBalances.FirstOrDefaultAsync(w => w.CustomerId == e.CustomerId, ct);
        if (wallet is null)
        {
            wallet = new WalletBalance { CustomerId = e.CustomerId };
            db.WalletBalances.Add(wallet);
        }
        else if (e.OccurredAtUtc < wallet.UpdatedAtUtc)
            return "WalletBalanceChanged: older than current row, ignored";

        wallet.Balance = e.Balance;
        wallet.UpdatedAtUtc = e.OccurredAtUtc;
        return $"WalletBalance {e.CustomerId}: {e.Balance:N0}";
    }
}
