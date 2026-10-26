namespace MiniShop.Orders.Saga;

/// <summary>Saga states (see the state chart in README.md).</summary>
public static class SagaStates
{
    public const string Started = "Started";
    public const string WaitingForStock = "WaitingForStock";
    public const string WaitingForPayment = "WaitingForPayment";
    public const string WaitingForPaymentCancellation = "WaitingForPaymentCancellation";
    public const string WaitingForRelease = "WaitingForRelease";
    public const string Completed = "Completed";
    public const string Rejected = "Rejected";
    public const string RequiresAttention = "RequiresAttention";

    public static bool IsTerminal(string state) => state is Completed or Rejected;
}

/// <summary>
/// Persisted saga instance (one per order, CorrelationId = OrderId).
/// The saga survives restarts because EVERY decision is stored here and in the Outbox in the same
/// transaction as the triggering message's Inbox row.  Version is an optimistic-concurrency token:
/// two messages for the same order processed at the same time cannot both win.
/// </summary>
public sealed class OrderSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = SagaStates.Started;
    /// <summary>Why the order is being compensated (carried into OrderRejected).</summary>
    public string? FailureReason { get; set; }
    /// <summary>How many times the current compensation command was (re)sent.</summary>
    public int CompensationAttempts { get; set; }
    /// <summary>When the timeout worker should act if no answer arrived. Null = not waiting.</summary>
    public DateTime? DeadlineUtc { get; set; }
    public int Version { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}
