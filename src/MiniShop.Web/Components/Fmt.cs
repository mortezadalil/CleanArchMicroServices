namespace MiniShop.Web.Components;

/// <summary>Small formatting helpers shared by the pages.</summary>
public static class Fmt
{
    public static string Money(long amount) => $"{amount:N0} T";
    public static string ShortId(System.Text.Json.Nodes.JsonNode? node) => node is null ? "—" : node.GetValue<string>()[..8];
    public static string Short(Guid? id) => id is null ? "—" : id.Value.ToString("N")[..8];
    public static string Time(DateTime utc) => utc.ToLocalTime().ToString("HH:mm:ss.fff");
    public static string Ago(DateTime utc)
    {
        var span = DateTime.UtcNow - utc;
        return span.TotalSeconds < 60 ? $"{span.TotalSeconds:0}s ago" : span.TotalMinutes < 60 ? $"{span.TotalMinutes:0}m ago" : utc.ToLocalTime().ToString("HH:mm");
    }

    /// <summary>CSS class of a state badge: green success, amber pending, red failure, purple compensation.</summary>
    public static string StateClass(string? state) => state switch
    {
        "Confirmed" or "Completed" or "Committed" or "Healthy" or "PASSED" or "Success" => "badge ok",
        "Rejected" or "Failed" or "FAILED" or "Down" or "Unhealthy" or "Cancelled" => "badge err",
        "Compensating" or "WaitingForRelease" or "WaitingForPaymentCancellation" or "Released" or "Rejecting" => "badge comp",
        "RequiresAttention" or "INCONCLUSIVE" or "Degraded" => "badge warn",
        _ => "badge pending",
    };

    /// <summary>CSS class of a timeline row by message/diagnostic kind.</summary>
    public static string KindClass(string kind) => kind switch
    {
        "ReserveStock" or "ReleaseStock" or "ChargeWallet" or "CancelPayment" => "kind command",
        "ConsumerFailed" or "MessageDeadLettered" or "RetryScheduled" => "kind failure",
        "FaultInjected" => "kind fault",
        "InboxDuplicateIgnored" => "kind dedup",
        "SagaTransitioned" => "kind saga",
        "StockReleased" or "PaymentFailed" or "PaymentCancelled" or "OrderRejected" or "OrderRequiresAttention" or "StockReservationRejected" => "kind comp",
        "OutboxStored" or "OutboxDispatched" or "MessagePublished" or "MessageConsumed" or "ProjectionUpdated" or "StateSnapshot" => "kind diag",
        _ => "kind event",
    };
}
