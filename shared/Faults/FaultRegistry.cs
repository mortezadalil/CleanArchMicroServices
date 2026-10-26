using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Diagnostics;

namespace MiniShop.Shared.Faults;

/// <summary>Result of <see cref="FaultRegistry.ApplyAsync"/>: flags the caller must honour itself.</summary>
public readonly record struct FaultOutcome(bool Reject, bool Duplicate, string? Reason)
{
    public static readonly FaultOutcome None = new(false, false, null);
}

/// <summary>
/// In-memory store of fault rules for ONE service, plus the single method every activation point calls.
///
/// <see cref="ApplyAsync"/> looks for an active rule matching (point, scenario, order), consumes one hit,
/// records a FaultInjected diagnostic, and then:
///   Fail      → throws InjectedFaultException
///   Delay     → awaits DelayMs
///   Pause     → waits until the rule is disabled / expired / deleted
///   Reject    → returns Reject = true (the caller produces a business "no")
///   Duplicate → returns Duplicate = true (the caller publishes twice)
/// </summary>
public sealed class FaultRegistry(string serviceName, DiagnosticsPublisher diagnostics, ILogger<FaultRegistry> logger)
{
    private readonly Dictionary<Guid, FaultRule> _rules = new();
    private readonly Lock _gate = new();

    public string ServiceName => serviceName;

    public IReadOnlyList<FaultRule> List()
    {
        lock (_gate) return _rules.Values.OrderBy(r => r.CreatedAtUtc).ToList();
    }

    public FaultRule Upsert(FaultRule rule)
    {
        rule.TargetService = serviceName;
        lock (_gate) _rules[rule.Id] = rule;
        logger.LogInformation("Fault rule {Kind} at {Point} registered (scenario {Scenario}, hits {Hits})",
            rule.Kind, rule.ActivationPoint, rule.ScenarioId, rule.RemainingHits);
        return rule;
    }

    public bool Remove(Guid id)
    {
        lock (_gate) return _rules.Remove(id);
    }

    /// <summary>Teardown of a scenario: every rule it created disappears, so experiments stay isolated.</summary>
    public int RemoveByScenario(Guid scenarioId)
    {
        lock (_gate)
        {
            var ids = _rules.Values.Where(r => r.ScenarioId == scenarioId).Select(r => r.Id).ToList();
            foreach (var id in ids) _rules.Remove(id);
            return ids.Count;
        }
    }

    public int Clear()
    {
        lock (_gate)
        {
            var count = _rules.Count;
            _rules.Clear();
            return count;
        }
    }

    /// <summary>Pause rules are not "consumed" per hit: they hold while active.</summary>
    public bool IsPaused(string point, Guid? scenarioId, Guid? correlationId = null)
    {
        lock (_gate)
            return _rules.Values.Any(r => r.Kind == FaultKind.Pause && r.IsActive(DateTime.UtcNow) && r.Matches(point, scenarioId, correlationId));
    }

    public Task<FaultOutcome> ApplyAsync(string point, Message message, CancellationToken ct) =>
        ApplyAsync(point, message.ScenarioId, message.CorrelationId, message.MessageId, message.Type, ct);

    public async Task<FaultOutcome> ApplyAsync(string point, Guid? scenarioId, Guid? correlationId,
        Guid? subjectMessageId, string? subjectMessageType, CancellationToken ct)
    {
        var rule = TakeHit(point, scenarioId, correlationId);
        if (rule is null)
            return FaultOutcome.None;

        diagnostics.Emit(DiagnosticKind.FaultInjected, correlationId: correlationId ?? Guid.Empty, scenarioId: scenarioId,
            subjectMessageId: subjectMessageId, subjectMessageType: subjectMessageType,
            detail: $"{rule.Kind} at {point}" + (rule.Description is null ? "" : $" — {rule.Description}"));
        logger.LogWarning("Injecting fault {Kind} at {Point} for order {Order}", rule.Kind, point, correlationId);

        switch (rule.Kind)
        {
            case FaultKind.Fail:
                throw new InjectedFaultException($"Injected failure at {point}");

            case FaultKind.Delay:
                await Task.Delay(TimeSpan.FromMilliseconds(rule.DelayMs), ct);
                return FaultOutcome.None;

            case FaultKind.Pause:
                // Hold the message here; the queue builds a visible backlog behind it.
                while (IsRuleStillActive(rule.Id))
                    await Task.Delay(250, ct);
                return FaultOutcome.None;

            case FaultKind.Reject:
                return new FaultOutcome(Reject: true, Duplicate: false, Reason: rule.Description ?? "rejected by injected fault");

            case FaultKind.Duplicate:
                return new FaultOutcome(Reject: false, Duplicate: true, Reason: null);

            default:
                return FaultOutcome.None;
        }
    }

    private FaultRule? TakeHit(string point, Guid? scenarioId, Guid? correlationId)
    {
        lock (_gate)
        {
            var now = DateTime.UtcNow;
            var rule = _rules.Values
                .Where(r => r.IsActive(now) && r.Matches(point, scenarioId, correlationId))
                .OrderByDescending(r => r.ScenarioId is not null) // scoped rules win over global ones
                .FirstOrDefault();
            if (rule is null)
                return null;

            rule.Hits++;
            if (rule.Kind != FaultKind.Pause && rule.RemainingHits is not null)
                rule.RemainingHits--;
            return rule;
        }
    }

    private bool IsRuleStillActive(Guid id)
    {
        lock (_gate) return _rules.TryGetValue(id, out var r) && r.IsActive(DateTime.UtcNow);
    }
}
