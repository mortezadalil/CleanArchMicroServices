// ==================================================================================================
// FILE: src/MiniShop.Web/Queries/ReadModelQueries.cs
//
// WHAT IT IS
//   Every read the UI does, in one class: products, wallet, orders, timeline, saga, message explorer,
//   projection lag, scenario runs.  Pages never build their own EF queries; they call these methods.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   The QUERY side of CQRS.
//     Write side: a command (POST /api/orders) goes to the Orders service, which owns the rules.
//     Read side:  pages read here, from readmodel.db ONLY.  No method touches orders.db, inventory.db,
//                 payments.db or RabbitMQ.  The data may be a few milliseconds (or, during S08, many
//                 seconds) behind the owners: that is eventual consistency, and the UI shows it.
//
// DbContextFactory vs scoped DbContext (important for Blazor Server)
//   This class is scoped, and in Blazor Server a scope lives as long as the browser tab (the circuit).
//   A DbContext is not thread-safe and its change tracker keeps every loaded entity.  Holding one for
//   hours, used by several components at once, would throw "a second operation was started on this
//   context" and leak memory.  So each method creates a FRESH context from the factory, runs one query
//   and disposes it ("await using").  AsNoTracking: we only read, so EF need not track the entities.
//
// Read next: Cart/CartService.cs and Services/ServiceClients.cs (the write path of the storefront).
// ==================================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Web.Data;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Queries;

/// <summary>
/// Projection lag as shown on the CQRS Explorer.
/// AppliedCount: events applied so far.  LastEventOccurredAtUtc / LastAppliedAtUtc: when the last event
/// happened at its producer / was applied here.  LastLagMs: the difference.  QueueBacklog: messages
/// still waiting in minishop.web (from the RabbitMQ management API), i.e. lag that is not applied yet.
/// </summary>
public sealed record ProjectionLag(long AppliedCount, DateTime? LastEventOccurredAtUtc, DateTime? LastAppliedAtUtc, double LastLagMs, int QueueBacklog);

/// <summary>
/// The QUERY side of CQRS: every page reads through these methods and ONLY from readmodel.db.
/// No query here ever touches orders.db / inventory.db / payments.db or RabbitMQ.
/// Uses <see cref="IDbContextFactory{TContext}"/>: one short-lived context per call (safe in Blazor Server).
/// </summary>
public sealed class ReadModelQueries(IDbContextFactory<ReadModelDbContext> factory)
{
    /// <summary>Storefront catalog, sorted by name.</summary>
    public async Task<List<ProductCard>> GetProductsAsync()
    {
        // Fresh context per call, disposed at the end of the method.
        await using var db = await factory.CreateDbContextAsync();
        return await db.ProductCards.AsNoTracking().OrderBy(p => p.Name).ToListAsync();
    }

    /// <summary>Wallet balance of one customer, or null if not projected yet (e.g. right after a reset).</summary>
    public async Task<WalletBalance?> GetWalletAsync(string customerId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.WalletBalances.AsNoTracking().FirstOrDefaultAsync(w => w.CustomerId == customerId);
    }

    /// <summary>Most recent orders first, optionally only one customer's ("My orders").</summary>
    public async Task<List<OrderSummary>> GetOrdersAsync(string? customerId = null, int take = 50)
    {
        await using var db = await factory.CreateDbContextAsync();
        // Build the query step by step; nothing runs against SQLite until ToListAsync.
        var query = db.Orders.AsNoTracking().AsQueryable();
        if (customerId is not null) query = query.Where(o => o.CustomerId == customerId);
        // Take limits the page size so the list stays fast however many orders exist.
        return await query.OrderByDescending(o => o.CreatedAtUtc).Take(take).ToListAsync();
    }

    /// <summary>One order (list and details share the same row), or null if not projected yet.</summary>
    public async Task<OrderSummary?> GetOrderDetailsAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.OrderId == orderId);
    }

    /// <summary>
    /// Everything recorded for one order (business + diagnostics), oldest first.
    /// Sorted by the PRODUCER's time (OccurredAtUtc), then by Seq to keep a stable order for equal times,
    /// so the timeline tells the story as it happened, not as it was delivered.
    /// </summary>
    public async Task<List<EventAudit>> GetOrderTimelineAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.EventAudit.AsNoTracking().Where(a => a.CorrelationId == orderId).OrderBy(a => a.OccurredAtUtc).ThenBy(a => a.Seq).ToListAsync();
    }

    /// <summary>Latest known saga state of an order (from SagaTransitioned telemetry), or null.</summary>
    public async Task<SagaSnapshot?> GetSagaAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.SagaSnapshots.AsNoTracking().FirstOrDefaultAsync(s => s.CorrelationId == orderId);
    }

    /// <summary>Message Explorer: recent records with optional filters.  Every filter is optional and they combine with AND.</summary>
    public async Task<List<EventAudit>> GetMessageTraceAsync(Guid? orderId = null, Guid? scenarioId = null, string? service = null,
        string? category = null, string? search = null, int take = 200)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().AsQueryable();
        // Add a WHERE clause only for the filters the user actually set.
        if (orderId is not null) query = query.Where(a => a.CorrelationId == orderId);
        if (scenarioId is not null) query = query.Where(a => a.ScenarioId == scenarioId);
        if (service is not null) query = query.Where(a => a.Service == service);
        if (category is not null) query = query.Where(a => a.Category == category);
        // Free-text search over the kind, the detail text and the subject message type.
        if (!string.IsNullOrWhiteSpace(search))
            query = query.Where(a => a.Kind.Contains(search) || (a.Detail != null && a.Detail.Contains(search))
                                     || (a.SubjectMessageType != null && a.SubjectMessageType.Contains(search)));
        // Newest first (by receive order), capped so the page stays responsive.
        return await query.OrderByDescending(a => a.Seq).Take(take).ToListAsync();
    }

    /// <summary>The latest audit rows (live feed of the Developer Console), optionally only those recorded since a time.</summary>
    public async Task<List<EventAudit>> GetRecentAsync(int take = 20, DateTime? since = null)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().AsQueryable();
        if (since is not null) query = query.Where(a => a.RecordedAtUtc >= since);
        return await query.OrderByDescending(a => a.Seq).Take(take).ToListAsync();
    }

    /// <summary>
    /// Reads the "ReadModel" checkpoint and combines it with the queue backlog the caller got from the
    /// RabbitMQ management API (this class never talks to RabbitMQ itself).  Zeros when nothing was applied yet.
    /// </summary>
    public async Task<ProjectionLag> GetProjectionLagAsync(int queueBacklog)
    {
        await using var db = await factory.CreateDbContextAsync();
        var cp = await db.Checkpoints.AsNoTracking().FirstOrDefaultAsync(c => c.Projection == "ReadModel");
        return new ProjectionLag(cp?.AppliedCount ?? 0, cp?.LastEventOccurredAtUtc, cp?.LastAppliedAtUtc, cp?.LastLagMs ?? 0, queueBacklog);
    }

    /// <summary>
    /// Counters for the Outbox/Inbox inspector cards, derived from telemetry.
    /// Returns "diagnostic kind → count" (e.g. OutboxStored → 12, InboxDuplicateIgnored → 1), optionally for one scenario.
    /// We count telemetry instead of reading the owners' outbox/inbox tables, because the Web never opens their databases.
    /// </summary>
    public async Task<Dictionary<string, int>> GetDiagnosticCountsAsync(Guid? scenarioId = null)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().Where(a => a.Category == "Diagnostic");
        if (scenarioId is not null) query = query.Where(a => a.ScenarioId == scenarioId);
        // GROUP BY Kind runs in SQLite (helped by the (Category, Kind) index); only the totals come back.
        return await query.GroupBy(a => a.Kind).Select(g => new { g.Key, Count = g.Count() }).ToDictionaryAsync(x => x.Key, x => x.Count);
    }

    /// <summary>Scenario Lab history, newest first.</summary>
    public async Task<List<ScenarioRun>> GetScenarioRunsAsync(int take = 30)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.ScenarioRuns.AsNoTracking().OrderByDescending(r => r.StartedAtUtc).Take(take).ToListAsync();
    }

    /// <summary>One scenario run with its assertions (in the order they were checked).  Run is null if unknown.</summary>
    public async Task<(ScenarioRun? Run, List<ScenarioAssertion> Assertions)> GetScenarioRunAsync(Guid runId)
    {
        await using var db = await factory.CreateDbContextAsync();
        var run = await db.ScenarioRuns.AsNoTracking().FirstOrDefaultAsync(r => r.Id == runId);
        var assertions = await db.ScenarioAssertions.AsNoTracking().Where(a => a.RunId == runId).OrderBy(a => a.Id).ToListAsync();
        return (run, assertions);
    }

    /// <summary>
    /// The ACTUAL business trace of an order: the sequence of commands/events as they were PUBLISHED
    /// by their owners (MessagePublished diagnostics), de-duplicated by message id.  Catalog/wallet
    /// notifications are excluded because they are not part of the saga conversation.
    /// The Scenario Lab compares this list with the scenario's expected trace.
    /// </summary>
    public async Task<List<string>> GetActualTraceAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        // Database part: the MessagePublished telemetry of this order, in the order things happened.
        // Only the two needed columns are loaded.
        var rows = await db.EventAudit.AsNoTracking()
            .Where(a => a.CorrelationId == orderId && a.Category == "Diagnostic" && a.Kind == DiagnosticKind.MessagePublished)
            .OrderBy(a => a.OccurredAtUtc).ThenBy(a => a.Seq)
            .Select(a => new { a.SubjectMessageId, a.SubjectMessageType })
            .ToListAsync();
        // In-memory part (LINQ to Objects):
        //   - drop catalog/wallet notifications, which are side effects, not saga steps;
        //   - DistinctBy message id: an outbox may publish the same message twice (crash after send,
        //     before marking it sent), and that is a legal at-least-once duplicate, not an extra step;
        //   - keep just the type names.
        return rows.Where(r => r.SubjectMessageType is not null and not ("ProductCatalogChanged" or "WalletBalanceChanged"))
            .DistinctBy(r => r.SubjectMessageId)
            .Select(r => r.SubjectMessageType!)
            .ToList();
    }
}
