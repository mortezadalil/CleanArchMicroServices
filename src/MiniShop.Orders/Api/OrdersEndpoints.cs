using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Orders.Data;
using MiniShop.Orders.Domain;
using MiniShop.Orders.Saga;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Api;

/// <summary>Request body of POST /api/orders. Prices are what the customer saw; Inventory re-validates them.</summary>
public sealed record PlaceOrderRequest(string CustomerId, IReadOnlyList<OrderLine> Lines, Guid? ScenarioId);

public static class OrdersEndpoints
{
    public const string IdempotencyHeader = "Idempotency-Key";

    public static void MapOrdersApi(this WebApplication app)
    {
        // ------------------------------------------------------------------------------------------
        // POST /api/orders — the ONE command the Web sends to a domain service.
        // It answers 202 Accepted immediately; the saga runs asynchronously through RabbitMQ.
        // ------------------------------------------------------------------------------------------
        app.MapPost("/api/orders", async (PlaceOrderRequest request, HttpRequest http, OrdersDbContext db,
            DiagnosticsPublisher diagnostics, OutboxSignal signal, CancellationToken ct) =>
        {
            var key = http.Headers[IdempotencyHeader].ToString();
            if (string.IsNullOrWhiteSpace(key))
                return Results.BadRequest($"{IdempotencyHeader} header is required.");
            if (request.Lines.Count == 0 || request.Lines.Any(l => l.Quantity <= 0 || l.UnitPrice < 0))
                return Results.BadRequest("Order must contain at least one line with a positive quantity.");

            // S14: the same Idempotency-Key always returns the same order, without creating a second one.
            var existing = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.IdempotencyKey == key, ct);
            if (existing is not null)
                return Results.Accepted($"/api/orders/{existing.Id}", new { orderId = existing.Id, duplicate = true });

            var now = DateTime.UtcNow;
            var order = new Order
            {
                Id = Guid.NewGuid(),
                CustomerId = request.CustomerId,
                Amount = request.Lines.Sum(l => l.LineTotal),
                State = OrderState.Pending,
                CreatedAtUtc = now,
                UpdatedAtUtc = now,
                IdempotencyKey = key,
                ScenarioId = request.ScenarioId,
                Items = request.Lines.Select(l => new OrderItem
                {
                    ProductId = l.ProductId,
                    ProductNameSnapshot = l.ProductName,
                    UnitPriceSnapshot = l.UnitPrice,
                    Quantity = l.Quantity,
                }).ToList(),
            };

            var placed = new OrderPlaced(order.Id, order.CustomerId, order.Amount, request.Lines)
            {
                CorrelationId = order.Id,
                ScenarioId = order.ScenarioId,
                Producer = ServiceNames.Orders,
            };

            // ONE transaction: the order, the saga instance and the OrderPlaced outbox row.
            // Either all of them exist or none of them does.
            using var _ = FaultContext.Enter(order.ScenarioId, order.Id);
            db.Orders.Add(order);
            db.SagaInstances.Add(new OrderSagaState { CorrelationId = order.Id, CreatedAtUtc = now, UpdatedAtUtc = now });
            var outboxRow = db.PublishViaOutbox(placed);
            db.OrderAudit.Add(new OrderAudit
            {
                OrderId = order.Id, Trigger = "Api", OrderFrom = null, OrderTo = nameof(OrderState.Pending),
                SagaFrom = null, SagaTo = SagaStates.Started, Detail = "order accepted (202); OrderPlaced stored in outbox", AtUtc = now,
            });

            try
            {
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException)
            {
                // Two identical requests raced; the unique index on IdempotencyKey let only one win.
                db.ChangeTracker.Clear();
                var winner = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.IdempotencyKey == key, ct);
                if (winner is null) throw;
                return Results.Accepted($"/api/orders/{winner.Id}", new { orderId = winner.Id, duplicate = true });
            }

            signal.Notify();
            diagnostics.Emit(DiagnosticKind.OutboxStored, placed, detail: "OrderPlaced stored in outbox (same transaction as the order)");
            diagnostics.Emit(DiagnosticKind.StateSnapshot, order.Id, order.ScenarioId, detail: "order created",
                snapshot: new { order = new { order.Id, order.State, order.Amount, order.CustomerId }, saga = new { CurrentState = SagaStates.Started } });

            return Results.Accepted($"/api/orders/{order.Id}", new { orderId = order.Id, duplicate = false, outboxMessageId = outboxRow.MessageId });
        });

        // Operator action for RequiresAttention orders (S12): re-send the pending compensation.
        app.MapPost("/api/orders/{orderId:guid}/retry-compensation", async (Guid orderId, OrdersDbContext db, SagaRunner runner,
            OutboxSignal signal, CancellationToken ct) =>
        {
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            var loaded = await runner.LoadAsync(orderId, ct);
            if (loaded is null) return Results.NotFound();
            var (saga, order) = loaded.Value;
            var decision = OrderSaga.RetryCompensation(saga, order, runner.Options);
            if (decision.Ignored) return Results.Conflict(new { error = decision.Detail, saga.CurrentState });
            var result = runner.Apply(saga, order, decision, trigger: "ManualRetry", causation: null);
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            result.AfterCommit?.Invoke();
            signal.Notify();
            return Results.Ok(new { orderId, saga.CurrentState, order.State });
        });
    }

    /// <summary>Read-only evidence endpoints used by the Scenario Lab to verify invariants in the OWNER database.</summary>
    public static void MapOrdersDiagnostics(this RouteGroupBuilder group)
    {
        group.MapGet("/orders/{orderId:guid}", async (Guid orderId, OrdersDbContext db) =>
        {
            var order = await db.Orders.AsNoTracking().Include(o => o.Items).FirstOrDefaultAsync(o => o.Id == orderId);
            if (order is null) return Results.NotFound();
            var saga = await db.SagaInstances.AsNoTracking().FirstOrDefaultAsync(s => s.CorrelationId == orderId);
            var audit = await db.OrderAudit.AsNoTracking().Where(a => a.OrderId == orderId).OrderBy(a => a.Id).ToListAsync();
            return Results.Ok(new { order, saga, audit });
        });

        group.MapGet("/orders", async (OrdersDbContext db, int take = 50) =>
            Results.Ok(await db.Orders.AsNoTracking().OrderByDescending(o => o.CreatedAtUtc).Take(take)
                .Select(o => new { o.Id, o.CustomerId, o.Amount, State = o.State.ToString(), o.CreatedAtUtc, o.ScenarioId, o.IdempotencyKey })
                .ToListAsync()));

        group.MapGet("/sagas", async (OrdersDbContext db, int take = 50) =>
            Results.Ok(await db.SagaInstances.AsNoTracking().OrderByDescending(s => s.UpdatedAtUtc).Take(take).ToListAsync()));

        group.MapGet("/orders/count", async (OrdersDbContext db, string? idempotencyKey) =>
            Results.Ok(new { count = await db.Orders.CountAsync(o => idempotencyKey == null || o.IdempotencyKey == idempotencyKey) }));
    }
}
