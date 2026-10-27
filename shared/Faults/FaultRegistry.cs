// ==========================================================================================
// FILE: shared/Faults/FaultRegistry.cs
//
// WHAT THIS FILE IS
//   The in-memory store of fault rules of ONE service, and the single method (ApplyAsync) that every
//   fault point in the code calls to ask "should something go wrong here, now, for this message?".
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  One singleton per service (registered in ServiceHostingExtensions.cs).
//   Written by the control plane (/demo/faults in shared/Demo/DemoEndpoints.cs), which the Web's
//   Scenario Lab calls; read by IdempotentConsumer (consumer fault points), OutboxDispatcher
//   (Outbox.Dispatch / Outbox.AfterPublish) and FaultingSaveChangesInterceptor (Db.SaveChanges).
//
// WHICH PATTERN IT IMPLEMENTS
//   Fault injection scoped by ScenarioId: every rule a scenario creates carries the scenario's id, and
//   only messages with the same ScenarioId match it.  So S10 can make "every ChargeWallet fail" while a
//   student places a normal order in another tab, and that order still succeeds.
//
// Read next: shared/Faults/FaultRule.cs (the rule data) and src/MiniShop.Web/Scenarios/ScenarioCatalog.cs
//            (which scenario creates which rule).
// ==========================================================================================

using Microsoft.Extensions.Logging;
using MiniShop.Contracts;
using MiniShop.Contracts.Diagnostics;
using MiniShop.Shared.Diagnostics;

namespace MiniShop.Shared.Faults;

/// <summary>Result of <see cref="FaultRegistry.ApplyAsync"/>: flags the caller must honour itself.</summary>
/// <remarks>
/// C# NOTE — <c>readonly record struct</c>: a small immutable VALUE type (copied, not referenced, no heap
/// allocation) that still gets record features (value equality, ToString, <c>with</c>).  Ideal for a tiny
/// result returned on every message.
/// </remarks>
/// <param name="Reject">True: the handler should answer a business "no" (e.g. PaymentFailed) instead of doing the work.</param>
/// <param name="Duplicate">True: the outbox dispatcher should publish the message twice (S06).</param>
/// <param name="Reason">Text for the rejection, shown to the user/console.</param>
public readonly record struct FaultOutcome(bool Reject, bool Duplicate, string? Reason)
{
    /// <summary>"No fault": the common case, shared as one static value.</summary>
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
/// <remarks>
/// Thread safety: HTTP requests (adding/removing rules), consumers and the outbox dispatcher all use the
/// registry at the same time from different threads.  A plain Dictionary is NOT safe for concurrent use,
/// so every access goes through <c>lock (_gate)</c>.
/// </remarks>
/// <param name="serviceName">Name of this service, stamped on registered rules.</param>
/// <param name="diagnostics">Used to emit a FaultInjected record each time a rule fires, so the console shows the failure was deliberate.</param>
/// <param name="logger">Service logger.</param>
public sealed class FaultRegistry(string serviceName, DiagnosticsPublisher diagnostics, ILogger<FaultRegistry> logger)
{
    /// <summary>Rules by id.  Only touched while holding <see cref="_gate"/>.</summary>
    private readonly Dictionary<Guid, FaultRule> _rules = new();

    /// <summary>The lock object protecting <see cref="_rules"/>.</summary>
    /// <remarks>
    /// C# NOTE — <c>System.Threading.Lock</c> (.NET 9 / C# 13): a dedicated lock type.  <c>lock (_gate) { ... }</c>
    /// on a Lock uses its faster, purpose-built API instead of the older Monitor on a plain <c>object</c>.
    /// Locks are held only for short in-memory work — never across an <c>await</c> (which is not allowed in a lock).
    /// </remarks>
    private readonly Lock _gate = new();

    /// <summary>This service's name (as passed at registration).</summary>
    public string ServiceName => serviceName;

    /// <summary>A snapshot copy of all rules, oldest first (for GET /demo/faults).</summary>
    /// <remarks>ToList() copies inside the lock, so the caller can enumerate the result without racing other threads.</remarks>
    public IReadOnlyList<FaultRule> List()
    {
        lock (_gate) return _rules.Values.OrderBy(r => r.CreatedAtUtc).ToList();
    }

    /// <summary>Adds a rule, or replaces the rule with the same Id.</summary>
    /// <param name="rule">The rule received from POST /demo/faults.</param>
    /// <returns>The stored rule.</returns>
    public FaultRule Upsert(FaultRule rule)
    {
        // The rule lives in THIS service, whatever the caller wrote.
        rule.TargetService = serviceName;
        // Indexer assignment = insert or overwrite ("upsert").
        lock (_gate) _rules[rule.Id] = rule;
        logger.LogInformation("Fault rule {Kind} at {Point} registered (scenario {Scenario}, hits {Hits})",
            rule.Kind, rule.ActivationPoint, rule.ScenarioId, rule.RemainingHits);
        return rule;
    }

    /// <summary>Deletes one rule.  Deleting a Pause rule releases the messages it was holding.</summary>
    /// <returns>True if the rule existed.</returns>
    public bool Remove(Guid id)
    {
        lock (_gate) return _rules.Remove(id);
    }

    /// <summary>Teardown of a scenario: every rule it created disappears, so experiments stay isolated.</summary>
    /// <param name="scenarioId">The finished scenario.</param>
    /// <returns>How many rules were removed.</returns>
    public int RemoveByScenario(Guid scenarioId)
    {
        lock (_gate)
        {
            // Collect ids first: removing from a Dictionary while enumerating it would throw.
            var ids = _rules.Values.Where(r => r.ScenarioId == scenarioId).Select(r => r.Id).ToList();
            foreach (var id in ids) _rules.Remove(id);
            return ids.Count;
        }
    }

    /// <summary>Removes every rule (global reset).</summary>
    /// <returns>How many rules were removed.</returns>
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
    /// <remarks>Used by the outbox dispatcher to skip (hold) rows of a paused scenario without consuming a hit.</remarks>
    /// <param name="point">The activation point.</param>
    /// <param name="scenarioId">ScenarioId of the message/row (null = normal order).</param>
    /// <param name="correlationId">OrderId, optional.</param>
    public bool IsPaused(string point, Guid? scenarioId, Guid? correlationId = null)
    {
        lock (_gate)
            return _rules.Values.Any(r => r.Kind == FaultKind.Pause && r.IsActive(DateTime.UtcNow) && r.Matches(point, scenarioId, correlationId));
    }

    /// <summary>Convenience overload: takes the ids from a message envelope.</summary>
    /// <param name="point">The activation point (one of <see cref="FaultPoints"/>).</param>
    /// <param name="message">The message being processed; its ScenarioId decides which rules may match.</param>
    /// <param name="ct">Cancellation (shutdown) token, honoured by Delay and Pause.</param>
    public Task<FaultOutcome> ApplyAsync(string point, Message message, CancellationToken ct) =>
        ApplyAsync(point, message.ScenarioId, message.CorrelationId, message.MessageId, message.Type, ct);

    /// <summary>
    /// Finds a matching active rule, consumes one hit, records telemetry and performs the fault (see class summary).
    /// </summary>
    /// <param name="point">The activation point.</param>
    /// <param name="scenarioId">Scenario of the current work (null = normal order: only global rules can match).</param>
    /// <param name="correlationId">Order of the current work.</param>
    /// <param name="subjectMessageId">Message being processed, for the diagnostic record (null for Db.SaveChanges).</param>
    /// <param name="subjectMessageType">Its type name, for the diagnostic record.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Flags for the caller; <see cref="FaultOutcome.None"/> when no rule fired.</returns>
    public async Task<FaultOutcome> ApplyAsync(string point, Guid? scenarioId, Guid? correlationId,
        Guid? subjectMessageId, string? subjectMessageType, CancellationToken ct)
    {
        // Pick the rule and decrement its hits atomically (inside the lock), so two concurrent messages
        // cannot both use the last remaining hit of a "fail twice" rule.
        var rule = TakeHit(point, scenarioId, correlationId);
        // The normal, fast path: nothing to inject.
        if (rule is null)
            return FaultOutcome.None;

        // Make the injected fault visible in the Developer Console BEFORE acting, so even a Fail that throws
        // is explained ("Fail at Db.SaveChanges — SaveChanges throws before commit").
        diagnostics.Emit(DiagnosticKind.FaultInjected, correlationId: correlationId ?? Guid.Empty, scenarioId: scenarioId,
            subjectMessageId: subjectMessageId, subjectMessageType: subjectMessageType,
            detail: $"{rule.Kind} at {point}" + (rule.Description is null ? "" : $" — {rule.Description}"));
        logger.LogWarning("Injecting fault {Kind} at {Point} for order {Order}", rule.Kind, point, correlationId);

        switch (rule.Kind)
        {
            // Throw: the caller's normal error handling takes over (rollback + MassTransit retry, or outbox backoff).
            case FaultKind.Fail:
                throw new InjectedFaultException($"Injected failure at {point}");

            // Slow down, then continue normally (S08 slow projection, S09, S13 late payment).
            case FaultKind.Delay:
                await Task.Delay(TimeSpan.FromMilliseconds(rule.DelayMs), ct);
                return FaultOutcome.None;

            case FaultKind.Pause:
                // Hold the message here; the queue builds a visible backlog behind it.
                // Poll every 250 ms (outside the lock) until the rule is disabled, expires or is deleted (S05).
                while (IsRuleStillActive(rule.Id))
                    await Task.Delay(250, ct);
                return FaultOutcome.None;

            // No exception: the caller turns this into a business refusal.
            case FaultKind.Reject:
                return new FaultOutcome(Reject: true, Duplicate: false, Reason: rule.Description ?? "rejected by injected fault");

            // No exception: the outbox dispatcher publishes the message a second time (S06).
            case FaultKind.Duplicate:
                return new FaultOutcome(Reject: false, Duplicate: true, Reason: null);

            // Defensive: an unknown kind does nothing rather than crash.
            default:
                return FaultOutcome.None;
        }
    }

    /// <summary>Selects the best matching active rule and records one hit on it, all under the lock.</summary>
    /// <returns>The rule that fires, or null.</returns>
    private FaultRule? TakeHit(string point, Guid? scenarioId, Guid? correlationId)
    {
        lock (_gate)
        {
            // One clock reading for the whole decision.
            var now = DateTime.UtcNow;
            var rule = _rules.Values
                .Where(r => r.IsActive(now) && r.Matches(point, scenarioId, correlationId))
                .OrderByDescending(r => r.ScenarioId is not null) // scoped rules win over global ones
                .FirstOrDefault();
            if (rule is null)
                return null;

            // Count the hit for the UI.
            rule.Hits++;
            // Consume one of the allowed hits.  Pause is exempt: it holds for as long as it is active, it is not
            // "used up" by holding a message.  When RemainingHits reaches 0, IsActive becomes false.
            if (rule.Kind != FaultKind.Pause && rule.RemainingHits is not null)
                rule.RemainingHits--;
            return rule;
        }
    }

    /// <summary>True while the rule still exists and is active (used by the Pause loop).</summary>
    private bool IsRuleStillActive(Guid id)
    {
        // `out var r` declares the variable inline; the && short-circuits if the rule was deleted.
        lock (_gate) return _rules.TryGetValue(id, out var r) && r.IsActive(DateTime.UtcNow);
    }
}
