// ==================================================================================================
// MiniShop.Orders — Api/OrdersEndpoints.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   The HTTP API of the Orders service, written as ASP.NET Core "minimal API" endpoints.
//   - POST /api/orders                              : place an order (the ONE command Web sends here).
//   - POST /api/orders/{id}/retry-compensation      : operator action for RequiresAttention (S12).
//   - GET  /diagnostics/orders, /orders/{id}, ...   : read-only evidence for the Scenario Lab.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Browser → Web → (HTTP) → THIS FILE → orders.db (order + saga + outbox row in ONE transaction)
//   → OutboxDispatcher → RabbitMQ → saga consumers (Saga/SagaConsumers.cs) → ...
//   The HTTP call never waits for Inventory or Payments: it answers 202 Accepted immediately and the
//   saga continues asynchronously through RabbitMQ.
//
// PATTERNS
//   - Transactional Outbox: solves the "dual-write problem". If we saved the order to the database and
//     THEN published OrderPlaced to RabbitMQ, a crash (or a broker outage, S04) between the two steps
//     would leave an order that nobody ever processes. Instead we write the message as a ROW in the
//     same database transaction; a background dispatcher publishes it later (at-least-once).
//   - Idempotent API (Idempotency-Key header, S14): a client that retries (double click, network
//     timeout) gets the SAME order back instead of a second one.
//
// Read next: Saga/OrderSaga.cs (what happens after OrderPlaced is consumed).
// ==================================================================================================

// EF Core query operators (FirstOrDefaultAsync, AsNoTracking, Include, ...).
using Microsoft.EntityFrameworkCore;
// Message contracts (OrderPlaced, OrderLine) and ServiceNames.
using MiniShop.Contracts;
// DiagnosticKind: the kinds of telemetry records shown in the developer console.
using MiniShop.Contracts.Diagnostics;
// OrdersDbContext.
using MiniShop.Orders.Data;
// Order, OrderItem, OrderAudit, OrderState.
using MiniShop.Orders.Domain;
// OrderSagaState, SagaStates, SagaRunner, OrderSaga.
using MiniShop.Orders.Saga;
// DiagnosticsPublisher (fire-and-forget telemetry).
using MiniShop.Shared.Diagnostics;
// FaultContext (tells fault rules which scenario/order we are working on).
using MiniShop.Shared.Faults;
// PublishViaOutbox extension and OutboxSignal.
using MiniShop.Shared.Outbox;

namespace MiniShop.Orders.Api;

/// <summary>Request body of POST /api/orders. Prices are what the customer saw; Inventory re-validates them.</summary>
/// <param name="CustomerId">Who is buying; Payments charges this customer's wallet.</param>
/// <param name="Lines">Products, quantities and the unit prices shown at checkout.</param>
/// <param name="ScenarioId">
/// Set only when the order is started by the Scenario Lab. It travels in every message of this order so
/// fault rules can target ONE experiment without disturbing normal storefront orders.
/// </param>
public sealed record PlaceOrderRequest(string CustomerId, IReadOnlyList<OrderLine> Lines, Guid? ScenarioId);

/// <summary>
/// Maps the Orders HTTP endpoints. A static class of extension methods keeps Program.cs short:
/// it only calls <c>app.MapOrdersApi()</c> and <c>group.MapOrdersDiagnostics()</c>.
/// </summary>
public static class OrdersEndpoints
{
    /// <summary>
    /// Name of the HTTP header that carries the client's idempotency key. The Web generates one key per
    /// checkout and re-uses it when it retries, so a retry can never create a second order (S14).
    /// </summary>
    public const string IdempotencyHeader = "Idempotency-Key";

    /// <summary>Maps POST /api/orders and POST /api/orders/{orderId}/retry-compensation.</summary>
    /// <param name="app">The application being configured in Program.cs.</param>
    public static void MapOrdersApi(this WebApplication app)
    {
        // ------------------------------------------------------------------------------------------
        // POST /api/orders — the ONE command the Web sends to a domain service.
        // It answers 202 Accepted immediately; the saga runs asynchronously through RabbitMQ.
        //
        // Why 202 and not 201 Created / 200 OK? Because the order is NOT finished: it is "Pending".
        // Stock and payment are decided later by other services. 202 means "I accepted your request
        // and will process it"; the Location header (/api/orders/{id}) tells the client where to look.
        //
        // The lambda parameters are filled by ASP.NET Core: the JSON body (request), the raw HttpRequest
        // (to read the header), and scoped services from DI (one OrdersDbContext per request).
        // ------------------------------------------------------------------------------------------
        app.MapPost("/api/orders", async (PlaceOrderRequest request, HttpRequest http, OrdersDbContext db,
            DiagnosticsPublisher diagnostics, OutboxSignal signal, CancellationToken ct) =>
        {
            // The idempotency key is mandatory: without it we could not tell a retry from a new order.
            var key = http.Headers[IdempotencyHeader].ToString();
            if (string.IsNullOrWhiteSpace(key))
                return Results.BadRequest($"{IdempotencyHeader} header is required.");
            // Basic input validation: reject empty orders, zero/negative quantities and negative prices
            // here, before anything is stored or sent to other services.
            if (request.Lines.Count == 0 || request.Lines.Any(l => l.Quantity <= 0 || l.UnitPrice < 0))
                return Results.BadRequest("Order must contain at least one line with a positive quantity.");

            // S14: the same Idempotency-Key always returns the same order, without creating a second one.
            // Fast path: the common case of a retry AFTER the first request finished. AsNoTracking because
            // we only read it. (The race of two requests at the SAME moment is handled further below.)
            var existing = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.IdempotencyKey == key, ct);
            if (existing is not null)
                return Results.Accepted($"/api/orders/{existing.Id}", new { orderId = existing.Id, duplicate = true });

            // One timestamp for every row of this transaction, so they line up exactly in the audit.
            var now = DateTime.UtcNow;
            // Build the aggregate. The Id is generated HERE (not by the database) so we can put it in the
            // OrderPlaced message before anything is saved. The OrderId is also the saga's CorrelationId.
            var order = new Order
            {
                Id = Guid.NewGuid(),
                CustomerId = request.CustomerId,
                // The total is computed on the server from the lines, never trusted from the client.
                Amount = request.Lines.Sum(l => l.LineTotal),
                // Every order starts as Pending; only the saga moves it further.
                State = OrderState.Pending,
                CreatedAtUtc = now,
                UpdatedAtUtc = now,
                // Stored with a UNIQUE index (see OrdersDbContext): the database is the final judge of S14.
                IdempotencyKey = key,
                ScenarioId = request.ScenarioId,
                // Copy each line as a SNAPSHOT: later price changes in the catalog do not alter this order.
                Items = request.Lines.Select(l => new OrderItem
                {
                    ProductId = l.ProductId,
                    ProductNameSnapshot = l.ProductName,
                    UnitPriceSnapshot = l.UnitPrice,
                    Quantity = l.Quantity,
                }).ToList(),
            };

            // The integration event that starts the saga. Envelope fields:
            //   CorrelationId = order id  → every message of this order can be traced end-to-end,
            //   ScenarioId                → lets fault rules target this order only,
            //   Producer                  → shown in the developer console.
            var placed = new OrderPlaced(order.Id, order.CustomerId, order.Amount, request.Lines)
            {
                CorrelationId = order.Id,
                ScenarioId = order.ScenarioId,
                Producer = ServiceNames.Orders,
            };

            // ONE transaction: the order, the saga instance and the OrderPlaced outbox row.
            // Either all of them exist or none of them does.
            // (SaveChangesAsync wraps all added rows in ONE implicit transaction, so no explicit
            // BeginTransaction is needed here.) This is the cure for the dual-write problem: we never
            // "save to the DB and then publish to RabbitMQ" as two separate steps.
            //
            // FaultContext.Enter tells the SaveChanges fault interceptor which scenario/order is being
            // saved, so a Scenario Lab rule can make exactly this save fail. Disposed at end of the lambda.
            using var _ = FaultContext.Enter(order.ScenarioId, order.Id);
            // 1) the order itself (with its items, via the navigation property).
            db.Orders.Add(order);
            // 2) the saga instance, in state Started. CorrelationId = OrderId (one saga per order).
            db.SagaInstances.Add(new OrderSagaState { CorrelationId = order.Id, CreatedAtUtc = now, UpdatedAtUtc = now });
            // 3) the OrderPlaced message as an Outbox ROW (not sent yet!). The dispatcher publishes it later,
            //    even if RabbitMQ is down right now (S04) or this process crashes right after commit.
            var outboxRow = db.PublishViaOutbox(placed);
            // 4) the first line of the audit trail ("null → Pending", "null → Started").
            db.OrderAudit.Add(new OrderAudit
            {
                OrderId = order.Id, Trigger = "Api", OrderFrom = null, OrderTo = nameof(OrderState.Pending),
                SagaFrom = null, SagaTo = SagaStates.Started, Detail = "order accepted (202); OrderPlaced stored in outbox", AtUtc = now,
            });

            try
            {
                // COMMIT: all four rows become visible together, or none of them does.
                await db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException)
            {
                // Two identical requests raced; the unique index on IdempotencyKey let only one win.
                // Both passed the "existing" check above at the same time (neither row existed yet), so
                // both tried to INSERT. The database rejected the second INSERT. We do not treat that as an
                // error: we look up the winner and answer exactly as if this were a normal retry (S14).
                // Clear the tracker: our failed entities are still "Added" and must not be re-saved.
                db.ChangeTracker.Clear();
                var winner = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.IdempotencyKey == key, ct);
                // No winner means the failure was something else (e.g. an injected fault): let it surface as 500.
                if (winner is null) throw;
                return Results.Accepted($"/api/orders/{winner.Id}", new { orderId = winner.Id, duplicate = true });
            }

            // Everything below runs only AFTER a successful commit.
            // Wake the outbox dispatcher now instead of waiting for its next 1-second poll (latency only;
            // a missed signal is harmless because the dispatcher polls anyway).
            signal.Notify();
            // Telemetry for the developer console: "OrderPlaced is safely in the outbox".
            diagnostics.Emit(DiagnosticKind.OutboxStored, placed, detail: "OrderPlaced stored in outbox (same transaction as the order)");
            // A snapshot of the new order + saga state, so the console can draw the first box of the timeline.
            diagnostics.Emit(DiagnosticKind.StateSnapshot, order.Id, order.ScenarioId, detail: "order created",
                snapshot: new { order = new { order.Id, order.State, order.Amount, order.CustomerId }, saga = new { CurrentState = SagaStates.Started } });

            // 202 Accepted + Location header. duplicate = false tells the client this request created the order.
            return Results.Accepted($"/api/orders/{order.Id}", new { orderId = order.Id, duplicate = false, outboxMessageId = outboxRow.MessageId });
        });

        // Operator action for RequiresAttention orders (S12): re-send the pending compensation.
        // When a compensation (ReleaseStock) was never confirmed after MaxCompensationAttempts, the saga
        // stops and waits for a human instead of pretending the order was rejected. Once the human fixed
        // the cause (e.g. removed the failing fault rule), this endpoint tries again.
        app.MapPost("/api/orders/{orderId:guid}/retry-compensation", async (Guid orderId, OrdersDbContext db, SagaRunner runner,
            OutboxSignal signal, CancellationToken ct) =>
        {
            // Explicit transaction: we READ the saga, decide, and WRITE back; nobody may change the saga
            // in between (a consumer or the timeout worker could be touching the same order).
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            // Load saga + order as tracked entities (changes are saved below).
            var loaded = await runner.LoadAsync(orderId, ct);
            if (loaded is null) return Results.NotFound();
            var (saga, order) = loaded.Value;
            // Ask the PURE state machine what to do. It refuses unless the saga is in RequiresAttention.
            var decision = OrderSaga.RetryCompensation(saga, order, runner.Options);
            // 409 Conflict: the request is valid but does not fit the current state of the resource.
            if (decision.Ignored) return Results.Conflict(new { error = decision.Detail, saga.CurrentState });
            // Apply the decision: new states, new deadline, audit row, ReleaseStock outbox row.
            var result = runner.Apply(saga, order, decision, trigger: "ManualRetry", causation: null);
            // Save and commit together, so the state change and the outgoing command cannot be separated.
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
            // Telemetry only after commit: the console never shows a transition that was rolled back.
            result.AfterCommit?.Invoke();
            // Wake the dispatcher so ReleaseStock leaves immediately.
            signal.Notify();
            return Results.Ok(new { orderId, saga.CurrentState, order.State });
        });
    }

    /// <summary>Read-only evidence endpoints used by the Scenario Lab to verify invariants in the OWNER database.</summary>
    /// <param name="group">The protected "/diagnostics" route group (Development only, X-Demo-Token required).</param>
    /// <remarks>
    /// Why read the owner database instead of the Web's read model? The read model is eventually
    /// consistent and could hide a bug. Checking orders.db directly is the strongest evidence.
    /// All queries use AsNoTracking because nothing is modified.
    /// </remarks>
    public static void MapOrdersDiagnostics(this RouteGroupBuilder group)
    {
        // GET /diagnostics/orders/{id}: the order with its items, its saga row and its full audit trail.
        group.MapGet("/orders/{orderId:guid}", async (Guid orderId, OrdersDbContext db) =>
        {
            var order = await db.Orders.AsNoTracking().Include(o => o.Items).FirstOrDefaultAsync(o => o.Id == orderId);
            if (order is null) return Results.NotFound();
            var saga = await db.SagaInstances.AsNoTracking().FirstOrDefaultAsync(s => s.CorrelationId == orderId);
            // Ordered by the auto-increment Id = the order in which the transitions were committed.
            var audit = await db.OrderAudit.AsNoTracking().Where(a => a.OrderId == orderId).OrderBy(a => a.Id).ToListAsync();
            return Results.Ok(new { order, saga, audit });
        });

        // GET /diagnostics/orders?take=50: the most recent orders (projection to a small anonymous type).
        group.MapGet("/orders", async (OrdersDbContext db, int take = 50) =>
            Results.Ok(await db.Orders.AsNoTracking().OrderByDescending(o => o.CreatedAtUtc).Take(take)
                .Select(o => new { o.Id, o.CustomerId, o.Amount, State = o.State.ToString(), o.CreatedAtUtc, o.ScenarioId, o.IdempotencyKey })
                .ToListAsync()));

        // GET /diagnostics/sagas?take=50: the most recently changed saga instances (state, deadline, version).
        group.MapGet("/sagas", async (OrdersDbContext db, int take = 50) =>
            Results.Ok(await db.SagaInstances.AsNoTracking().OrderByDescending(s => s.UpdatedAtUtc).Take(take).ToListAsync()));

        // GET /diagnostics/orders/count?idempotencyKey=...: S14 checks that the count is exactly 1.
        // Without the query parameter it counts all orders.
        group.MapGet("/orders/count", async (OrdersDbContext db, string? idempotencyKey) =>
            Results.Ok(new { count = await db.Orders.CountAsync(o => idempotencyKey == null || o.IdempotencyKey == idempotencyKey) }));
    }
}
