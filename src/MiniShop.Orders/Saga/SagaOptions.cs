namespace MiniShop.Orders.Saga;

/// <summary>Timeouts of the order saga (configuration section "Saga"). Short on purpose: this is a classroom demo.</summary>
public sealed class SagaOptions
{
    public const string Section = "Saga";
    public int StockTimeoutSeconds { get; set; } = 20;
    public int PaymentTimeoutSeconds { get; set; } = 20;
    /// <summary>How long to wait for StockReleased / PaymentCancelled before re-sending the compensation command.</summary>
    public int CompensationTimeoutSeconds { get; set; } = 8;
    /// <summary>After this many unanswered compensation commands the saga gives up: RequiresAttention.</summary>
    public int MaxCompensationAttempts { get; set; } = 3;
}
