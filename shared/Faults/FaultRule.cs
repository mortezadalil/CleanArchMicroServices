// ==========================================================================================
// FILE: shared/Faults/FaultRule.cs
//
// WHAT THIS FILE IS
//   The data types of fault injection: the kinds of fault (FaultKind), one rule (FaultRule), the list
//   of named places where a fault can fire (FaultPoints), and the exception thrown by a Fail rule.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  The Web's Scenario Lab (src/MiniShop.Web/Scenarios) creates rules by
//   POSTing JSON to /demo/faults on the target service (shared/Demo/DemoEndpoints.cs); the rules are
//   stored in that service's FaultRegistry (FaultRegistry.cs) and evaluated at the FaultPoints by the
//   consumers, the outbox dispatcher and the SaveChanges interceptor.
//
// WHICH PATTERN IT IMPLEMENTS
//   Fault injection / chaos testing, SCOPED BY ScenarioId so an experiment never disturbs other orders.
//
// Read next: shared/Faults/FaultRegistry.cs.
// ==========================================================================================

namespace MiniShop.Shared.Faults;

/// <summary>What the fault does when it fires.</summary>
/// <remarks>Serialized as its name ("Fail", "Delay"...) thanks to JsonStringEnumConverter, so the Scenario Lab can send readable JSON.</remarks>
public enum FaultKind
{
    /// <summary>Throw an exception at the activation point (simulates a crash / transient error → retry).</summary>
    Fail,
    /// <summary>Wait DelayMs before continuing (simulates a slow consumer / network).</summary>
    Delay,
    /// <summary>Publish the message twice (simulates broker redelivery).</summary>
    Duplicate,
    /// <summary>Block at the activation point until the rule is disabled or expires (simulates a stopped consumer).</summary>
    Pause,
    /// <summary>Do not crash, but make the business operation answer "no" (e.g. decline a payment).</summary>
    Reject,
}

/// <summary>
/// One fault-injection rule.  Rules live in memory inside the targeted service and are managed
/// through the Development-only control plane (POST/DELETE /demo/faults).
///
/// Scoping: a rule with a ScenarioId only affects messages carrying that ScenarioId, so an
/// experiment never disturbs other orders.  A rule without ScenarioId is global (manual toggles).
/// RemainingHits limits how many times the rule fires (null = unlimited while enabled/not expired).
/// </summary>
/// <remarks>
/// A mutable class (get; set;) because it is bound from the JSON body of POST /demo/faults and because the
/// registry updates <see cref="Hits"/> / <see cref="RemainingHits"/> as the rule fires.
/// In memory only: restarting the service (S09) clears all rules, which is a safe default for a demo.
/// </remarks>
public sealed class FaultRule
{
    /// <summary>Rule id (generated if the caller does not send one); used to update or delete it.</summary>
    public Guid Id { get; set; } = Guid.NewGuid();
    /// <summary>Only messages of this scenario are affected; null = global rule (affects every message at the point).</summary>
    public Guid? ScenarioId { get; set; }
    /// <summary>Optionally narrow the rule further to ONE order; null = any order.</summary>
    public Guid? CorrelationId { get; set; }
    /// <summary>Which service owns the rule; overwritten by the registry with its own name (informational).</summary>
    public string TargetService { get; set; } = "";
    /// <summary>One of <see cref="FaultPoints"/>.</summary>
    public string ActivationPoint { get; set; } = "";
    /// <summary>What happens when the rule fires.</summary>
    public FaultKind Kind { get; set; }
    /// <summary>Delay duration in milliseconds (only for <see cref="FaultKind.Delay"/>).</summary>
    public int DelayMs { get; set; }
    /// <summary>How many more times the rule may fire (e.g. 2 = "fail twice, then succeed", S07); null = unlimited.</summary>
    public int? RemainingHits { get; set; }
    /// <summary>After this time the rule is inactive.  The control plane forces a 10-minute maximum if none is given.</summary>
    public DateTime? ExpiresAtUtc { get; set; }
    /// <summary>Manual on/off switch (turning a Pause rule off releases the held messages).</summary>
    public bool Enabled { get; set; } = true;
    /// <summary>Human description shown in the UI ("decline the next payment").</summary>
    public string? Description { get; set; }
    /// <summary>When the rule was registered; the list is ordered by it.</summary>
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    /// <summary>How many times the rule has fired so far (shown in the UI as evidence).</summary>
    public int Hits { get; set; }

    /// <summary>True while the rule can still fire: enabled, not expired, hits not used up.</summary>
    /// <param name="now">Current UTC time, passed in so one decision uses one consistent clock reading.</param>
    public bool IsActive(DateTime now) =>
        Enabled
        && (ExpiresAtUtc is null || ExpiresAtUtc > now)
        && (RemainingHits is null || RemainingHits > 0);

    /// <summary>
    /// True when the rule applies to this place and this message.  A null ScenarioId/CorrelationId on the RULE
    /// means "any"; a value means "only this one".  This is where scenario scoping happens.
    /// </summary>
    /// <param name="point">The activation point being evaluated.</param>
    /// <param name="scenarioId">The message's ScenarioId (null for a normal order).</param>
    /// <param name="correlationId">The message's OrderId.</param>
    public bool Matches(string point, Guid? scenarioId, Guid? correlationId) =>
        ActivationPoint == point
        && (ScenarioId is null || ScenarioId == scenarioId)
        && (CorrelationId is null || CorrelationId == correlationId);
}

/// <summary>Named places in the code where a fault can be injected.</summary>
/// <remarks>Strings (not an enum) so they read well in JSON and logs; <see cref="All"/> is used to validate incoming rules.</remarks>
public static class FaultPoints
{
    /// <summary>Outbox dispatcher, before publishing a row (Duplicate in S06; Pause / Fail / Delay for manual experiments).</summary>
    public const string OutboxDispatch = "Outbox.Dispatch";
    /// <summary>Outbox dispatcher, after publishing and before marking the row as published (S11 crash in the gap).</summary>
    public const string OutboxAfterPublish = "Outbox.AfterPublish";
    /// <summary>Any DbContext SaveChanges, through the interceptor (S07 transient database failure).</summary>
    public const string DbSaveChanges = "Db.SaveChanges";
    /// <summary>Inventory's ReserveStock consumer (e.g. slowed down in S09).</summary>
    public const string InventoryReserve = "Inventory.ReserveStock";
    /// <summary>Inventory's ReleaseStock consumer (compensation that keeps failing, S12).</summary>
    public const string InventoryRelease = "Inventory.ReleaseStock";
    /// <summary>Payments' ChargeWallet consumer (paused S05, failing S10, slowed S13).</summary>
    public const string PaymentsCharge = "Payments.ChargeWallet";
    /// <summary>Payments' CancelPayment consumer.</summary>
    public const string PaymentsCancel = "Payments.CancelPayment";
    /// <summary>The Web's read-model projection consumers (slow S08, crashing S16).</summary>
    public const string WebProjection = "Web.Projection";
    /// <summary>The Orders saga consumers.</summary>
    public const string OrdersSaga = "Orders.Saga";

    /// <summary>Every valid point; POST /demo/faults rejects a rule whose ActivationPoint is not in this list (catches typos).</summary>
    /// <remarks>A collection expression (C# 12) creating a string[].</remarks>
    public static readonly string[] All =
    [
        OutboxDispatch, OutboxAfterPublish, DbSaveChanges, InventoryReserve, InventoryRelease,
        PaymentsCharge, PaymentsCancel, WebProjection, OrdersSaga,
    ];
}

/// <summary>Thrown by an injected Fail fault. Consumers treat it like any other exception (retry → error queue).</summary>
/// <remarks>A dedicated type makes injected failures easy to recognise in logs, versus real bugs.</remarks>
/// <param name="message">Text of the exception ("Injected failure at Db.SaveChanges").</param>
public sealed class InjectedFaultException(string message) : Exception(message);
