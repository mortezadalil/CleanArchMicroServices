// =====================================================================================================
//  Fmt.cs  —  tiny formatting helpers used by the Razor pages
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    Pure static functions that turn values (amounts, ids, timestamps, states) into short strings or
//    CSS class names.  Keeping them here keeps the markup short and makes every page format things
//    the same way.
//
//  What the student sees on screen:
//    Every "1,300,000 T" price, every 8-character id, every "12s ago", and the colour of every state
//    badge and timeline row comes from here.
//
//  Data source: none; it only formats values that the pages already loaded from the read model.
//
//  Read next: LiveComponentBase.cs (how pages refresh themselves), then wwwroot/app.css (the classes
//  returned by StateClass / KindClass).
// =====================================================================================================

namespace MiniShop.Web.Components;

/// <summary>Small formatting helpers shared by the pages.</summary>
public static class Fmt
{
    /// <summary>Formats an amount with thousands separators and the currency sign "T" (e.g. "800,000 T").
    /// Amounts are whole numbers (long): money is never stored as floating point.</summary>
    public static string Money(long amount) => $"{amount:N0} T";
    /// <summary>First 8 characters of an id stored in a JSON node (diagnostics answers), or "—" when missing.</summary>
    public static string ShortId(System.Text.Json.Nodes.JsonNode? node) => node is null ? "—" : node.GetValue<string>()[..8];
    /// <summary>First 8 hex characters of a Guid ("N" format = no dashes): enough to recognise an id on screen.</summary>
    public static string Short(Guid? id) => id is null ? "—" : id.Value.ToString("N")[..8];
    /// <summary>A UTC timestamp shown in the server's local time with milliseconds (message ordering matters).</summary>
    public static string Time(DateTime utc) => utc.ToLocalTime().ToString("HH:mm:ss.fff");
    /// <summary>A relative time: "12s ago", "5m ago", or the clock time when older than an hour.</summary>
    public static string Ago(DateTime utc)
    {
        var span = DateTime.UtcNow - utc;
        // Nested conditional: under a minute → seconds; under an hour → minutes; otherwise → HH:mm.
        return span.TotalSeconds < 60 ? $"{span.TotalSeconds:0}s ago" : span.TotalMinutes < 60 ? $"{span.TotalMinutes:0}m ago" : utc.ToLocalTime().ToString("HH:mm");
    }

    /// <summary>CSS class of a state badge: green success, amber pending, red failure, purple compensation.</summary>
    /// <remarks>
    /// One mapping for ALL kinds of states (order, saga, reservation, payment, health, scenario verdict),
    /// so the same colour always means the same thing everywhere.  The classes are defined in app.css
    /// (.badge.ok, .badge.err, .badge.comp, .badge.warn, .badge.pending).
    /// </remarks>
    public static string StateClass(string? state) => state switch
    {
        // Finished well.
        "Confirmed" or "Completed" or "Committed" or "Healthy" or "PASSED" or "Success" => "badge ok",
        // Finished badly / unavailable.
        "Rejected" or "Failed" or "FAILED" or "Down" or "Unhealthy" or "Cancelled" => "badge err",
        // Compensation in progress or done (undoing earlier steps).
        "Compensating" or "WaitingForRelease" or "WaitingForPaymentCancellation" or "Released" or "Rejecting" => "badge comp",
        // Needs a human to look at it.
        "RequiresAttention" or "INCONCLUSIVE" or "Degraded" => "badge warn",
        // Everything else is "still in progress".
        _ => "badge pending",
    };

    /// <summary>CSS class of a timeline row by message/diagnostic kind.</summary>
    /// <remarks>
    /// Lets the student tell at a glance commands (asking a service to DO something) from events
    /// (announcing that something HAPPENED), failures, injected faults, inbox de-duplication, saga
    /// transitions, compensation messages, and purely technical diagnostics.
    /// </remarks>
    public static string KindClass(string kind) => kind switch
    {
        // Commands sent by the saga to one specific service.
        "ReserveStock" or "ReleaseStock" or "ChargeWallet" or "CancelPayment" => "kind command",
        // Something went wrong (and may be retried).
        "ConsumerFailed" or "MessageDeadLettered" or "RetryScheduled" => "kind failure",
        // A fault rule fired (an experiment, not a bug).
        "FaultInjected" => "kind fault",
        // The Inbox recognised a message it had already processed.
        "InboxDuplicateIgnored" => "kind dedup",
        // The saga moved from one state to another.
        "SagaTransitioned" => "kind saga",
        // Events of the "unhappy" / compensation path.
        "StockReleased" or "PaymentFailed" or "PaymentCancelled" or "OrderRejected" or "OrderRequiresAttention" or "StockReservationRejected" => "kind comp",
        // Technical plumbing records (outbox, publish/consume, projection, snapshots).
        "OutboxStored" or "OutboxDispatched" or "MessagePublished" or "MessageConsumed" or "ProjectionUpdated" or "StateSnapshot" => "kind diag",
        // Any other business event (OrderPlaced, StockReserved, PaymentCompleted, OrderConfirmed...).
        _ => "kind event",
    };
}
