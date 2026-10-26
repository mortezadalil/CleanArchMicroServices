namespace MiniShop.Shared.Faults;

/// <summary>What the fault does when it fires.</summary>
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
public sealed class FaultRule
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid? ScenarioId { get; set; }
    public Guid? CorrelationId { get; set; }
    public string TargetService { get; set; } = "";
    /// <summary>One of <see cref="FaultPoints"/>.</summary>
    public string ActivationPoint { get; set; } = "";
    public FaultKind Kind { get; set; }
    public int DelayMs { get; set; }
    public int? RemainingHits { get; set; }
    public DateTime? ExpiresAtUtc { get; set; }
    public bool Enabled { get; set; } = true;
    /// <summary>Human description shown in the UI ("decline the next payment").</summary>
    public string? Description { get; set; }
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public int Hits { get; set; }

    public bool IsActive(DateTime now) =>
        Enabled
        && (ExpiresAtUtc is null || ExpiresAtUtc > now)
        && (RemainingHits is null || RemainingHits > 0);

    public bool Matches(string point, Guid? scenarioId, Guid? correlationId) =>
        ActivationPoint == point
        && (ScenarioId is null || ScenarioId == scenarioId)
        && (CorrelationId is null || CorrelationId == correlationId);
}

/// <summary>Named places in the code where a fault can be injected.</summary>
public static class FaultPoints
{
    public const string OutboxDispatch = "Outbox.Dispatch";
    public const string OutboxAfterPublish = "Outbox.AfterPublish";
    public const string DbSaveChanges = "Db.SaveChanges";
    public const string InventoryReserve = "Inventory.ReserveStock";
    public const string InventoryRelease = "Inventory.ReleaseStock";
    public const string PaymentsCharge = "Payments.ChargeWallet";
    public const string PaymentsCancel = "Payments.CancelPayment";
    public const string WebProjection = "Web.Projection";
    public const string OrdersSaga = "Orders.Saga";

    public static readonly string[] All =
    [
        OutboxDispatch, OutboxAfterPublish, DbSaveChanges, InventoryReserve, InventoryRelease,
        PaymentsCharge, PaymentsCancel, WebProjection, OrdersSaga,
    ];
}

/// <summary>Thrown by an injected Fail fault. Consumers treat it like any other exception (retry → error queue).</summary>
public sealed class InjectedFaultException(string message) : Exception(message);
