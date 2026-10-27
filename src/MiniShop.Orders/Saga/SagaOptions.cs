// ==================================================================================================
// MiniShop.Orders — Saga/SagaOptions.cs
// ==================================================================================================
//
// WHAT THIS FILE IS
//   Strongly-typed settings for the order saga, bound from the "Saga" section of appsettings.json in
//   Program.cs (builder.Services.Configure<SagaOptions>(...)). This is the standard .NET "options
//   pattern": classes ask for IOptions<SagaOptions> instead of reading raw configuration strings.
//
// WHERE IT IS USED
//   OrderSaga.cs turns these numbers into deadlines (SagaDecision.WaitFor); SagaTimeoutWorker.cs fires
//   when a deadline passes. Scenarios S10 (payment timeout), S12 (compensation keeps failing) and S13
//   (late payment) depend directly on these values.
//
// Read next: Saga/OrderSaga.cs.
// ==================================================================================================

namespace MiniShop.Orders.Saga;

/// <summary>Timeouts of the order saga (configuration section "Saga"). Short on purpose: this is a classroom demo.</summary>
/// <remarks>
/// The property initialisers are the defaults used when appsettings.json does not set a value.
/// Real systems would use minutes or hours; seconds let students watch a timeout happen live.
/// </remarks>
public sealed class SagaOptions
{
    /// <summary>Name of the configuration section ("Saga") these options are read from.</summary>
    public const string Section = "Saga";
    /// <summary>How long to wait for Inventory to answer ReserveStock before compensating with ReleaseStock.</summary>
    public int StockTimeoutSeconds { get; set; } = 20;
    /// <summary>How long to wait for Payments to answer ChargeWallet before asking it to cancel (S10, S13).</summary>
    public int PaymentTimeoutSeconds { get; set; } = 20;
    /// <summary>How long to wait for StockReleased / PaymentCancelled before re-sending the compensation command.</summary>
    public int CompensationTimeoutSeconds { get; set; } = 8;
    /// <summary>After this many unanswered compensation commands the saga gives up: RequiresAttention.</summary>
    public int MaxCompensationAttempts { get; set; } = 3;
}
