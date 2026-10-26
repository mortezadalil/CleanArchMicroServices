using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Web.Data;
using MiniShop.Web.ReadModel;

namespace MiniShop.Web.Queries;

/// <summary>Projection lag as shown on the CQRS Explorer.</summary>
public sealed record ProjectionLag(long AppliedCount, DateTime? LastEventOccurredAtUtc, DateTime? LastAppliedAtUtc, double LastLagMs, int QueueBacklog);

/// <summary>
/// The QUERY side of CQRS: every page reads through these methods and ONLY from readmodel.db.
/// No query here ever touches orders.db / inventory.db / payments.db or RabbitMQ.
/// </summary>
public sealed class ReadModelQueries(IDbContextFactory<ReadModelDbContext> factory)
{
    public async Task<List<ProductCard>> GetProductsAsync()
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.ProductCards.AsNoTracking().OrderBy(p => p.Name).ToListAsync();
    }

    public async Task<WalletBalance?> GetWalletAsync(string customerId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.WalletBalances.AsNoTracking().FirstOrDefaultAsync(w => w.CustomerId == customerId);
    }

    public async Task<List<OrderSummary>> GetOrdersAsync(string? customerId = null, int take = 50)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.Orders.AsNoTracking().AsQueryable();
        if (customerId is not null) query = query.Where(o => o.CustomerId == customerId);
        return await query.OrderByDescending(o => o.CreatedAtUtc).Take(take).ToListAsync();
    }

    public async Task<OrderSummary?> GetOrderDetailsAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.OrderId == orderId);
    }

    /// <summary>Everything recorded for one order (business + diagnostics), oldest first.</summary>
    public async Task<List<EventAudit>> GetOrderTimelineAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.EventAudit.AsNoTracking().Where(a => a.CorrelationId == orderId).OrderBy(a => a.OccurredAtUtc).ThenBy(a => a.Seq).ToListAsync();
    }

    public async Task<SagaSnapshot?> GetSagaAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.SagaSnapshots.AsNoTracking().FirstOrDefaultAsync(s => s.CorrelationId == orderId);
    }

    /// <summary>Message Explorer: recent records with optional filters.</summary>
    public async Task<List<EventAudit>> GetMessageTraceAsync(Guid? orderId = null, Guid? scenarioId = null, string? service = null,
        string? category = null, string? search = null, int take = 200)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().AsQueryable();
        if (orderId is not null) query = query.Where(a => a.CorrelationId == orderId);
        if (scenarioId is not null) query = query.Where(a => a.ScenarioId == scenarioId);
        if (service is not null) query = query.Where(a => a.Service == service);
        if (category is not null) query = query.Where(a => a.Category == category);
        if (!string.IsNullOrWhiteSpace(search))
            query = query.Where(a => a.Kind.Contains(search) || (a.Detail != null && a.Detail.Contains(search))
                                     || (a.SubjectMessageType != null && a.SubjectMessageType.Contains(search)));
        return await query.OrderByDescending(a => a.Seq).Take(take).ToListAsync();
    }

    public async Task<List<EventAudit>> GetRecentAsync(int take = 20, DateTime? since = null)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().AsQueryable();
        if (since is not null) query = query.Where(a => a.RecordedAtUtc >= since);
        return await query.OrderByDescending(a => a.Seq).Take(take).ToListAsync();
    }

    public async Task<ProjectionLag> GetProjectionLagAsync(int queueBacklog)
    {
        await using var db = await factory.CreateDbContextAsync();
        var cp = await db.Checkpoints.AsNoTracking().FirstOrDefaultAsync(c => c.Projection == "ReadModel");
        return new ProjectionLag(cp?.AppliedCount ?? 0, cp?.LastEventOccurredAtUtc, cp?.LastAppliedAtUtc, cp?.LastLagMs ?? 0, queueBacklog);
    }

    /// <summary>Counters for the Outbox/Inbox inspector cards, derived from telemetry.</summary>
    public async Task<Dictionary<string, int>> GetDiagnosticCountsAsync(Guid? scenarioId = null)
    {
        await using var db = await factory.CreateDbContextAsync();
        var query = db.EventAudit.AsNoTracking().Where(a => a.Category == "Diagnostic");
        if (scenarioId is not null) query = query.Where(a => a.ScenarioId == scenarioId);
        return await query.GroupBy(a => a.Kind).Select(g => new { g.Key, Count = g.Count() }).ToDictionaryAsync(x => x.Key, x => x.Count);
    }

    public async Task<List<ScenarioRun>> GetScenarioRunsAsync(int take = 30)
    {
        await using var db = await factory.CreateDbContextAsync();
        return await db.ScenarioRuns.AsNoTracking().OrderByDescending(r => r.StartedAtUtc).Take(take).ToListAsync();
    }

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
    /// </summary>
    public async Task<List<string>> GetActualTraceAsync(Guid orderId)
    {
        await using var db = await factory.CreateDbContextAsync();
        var rows = await db.EventAudit.AsNoTracking()
            .Where(a => a.CorrelationId == orderId && a.Category == "Diagnostic" && a.Kind == DiagnosticKind.MessagePublished)
            .OrderBy(a => a.OccurredAtUtc).ThenBy(a => a.Seq)
            .Select(a => new { a.SubjectMessageId, a.SubjectMessageType })
            .ToListAsync();
        return rows.Where(r => r.SubjectMessageType is not null and not ("ProductCatalogChanged" or "WalletBalanceChanged"))
            .DistinctBy(r => r.SubjectMessageId)
            .Select(r => r.SubjectMessageType!)
            .ToList();
    }
}
