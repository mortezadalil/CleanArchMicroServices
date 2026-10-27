// ==========================================================================================
// FILE: shared/Outbox/OutboxMessage.cs
//
// WHAT THIS FILE IS
//   The Outbox table entity, the interface for a DbContext that owns one, and the two extension
//   methods business code uses to "publish" or "send" a message: PublishViaOutbox / SendViaOutbox.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Every service that produces messages (Orders, Inventory, Payments)
//   has an Outbox table in its own SQLite database.  Business code never talks to RabbitMQ directly:
//   it adds an OutboxMessage to the DbContext, and shared/Outbox/OutboxDispatcher.cs later moves the
//   row to the broker.
//
// WHICH PATTERN IT IMPLEMENTS
//   Transactional Outbox — the answer to the DUAL-WRITE PROBLEM.  "Save the order in the database"
//   and "publish OrderPlaced to RabbitMQ" are two different systems; there is no transaction that
//   covers both.  If we saved and then crashed before publishing, the order would exist but nobody
//   would ever hear about it.  If we published first and the save failed, other services would react
//   to an order that does not exist.  Writing the message as a ROW in the same database transaction
//   as the business change makes both happen or neither; publishing is then a separate, retryable step.
//
// Read next: shared/Outbox/OutboxDispatcher.cs.
// ==========================================================================================

using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Shared.Messaging;

namespace MiniShop.Shared.Outbox;

/// <summary>
/// One row per message that must be delivered to RabbitMQ.
///
/// The row is written in the SAME database transaction as the business change it announces.
/// A separate background dispatcher (<see cref="OutboxDispatcher"/>) reads unpublished rows and
/// publishes them, so a broker outage or a crash between "commit" and "publish" never loses a message.
/// It can, however, publish a message twice (crash after publish, before PublishedAt is set), which is
/// exactly why every consumer has an Inbox.
/// </summary>
/// <remarks>
/// Scenario Lab: S04 (broker outage — rows pile up with Attempts/LastError and drain when RabbitMQ is back),
/// S06 (dispatcher forced to publish twice), S11 (crash in the gap between publish and mark-as-published).
/// </remarks>
public sealed class OutboxMessage
{
    /// <summary>Auto-increment primary key.  Insertion order = dispatch order, so events of one order leave in the order they were written.</summary>
    public long Id { get; set; }                      // insertion order = dispatch order
    /// <summary>The message's own MessageId.  Re-publishing the row re-uses it, so consumers' Inboxes recognise the duplicate.</summary>
    public Guid MessageId { get; set; }               // copied from the message envelope
    /// <summary>The order (flow) the message belongs to; used for filtering in /diagnostics/outbox.</summary>
    public Guid CorrelationId { get; set; }
    /// <summary>Scenario of the message, copied so the dispatcher can apply scenario-scoped Pause rules without deserializing the payload.</summary>
    public Guid? ScenarioId { get; set; }
    /// <summary>Short type name; mapped back to the CLR type by <see cref="ContractTypes.Resolve"/>.</summary>
    public string Type { get; set; } = "";            // short CLR name, e.g. "StockReserved"
    /// <summary>The full message as JSON (written with <see cref="Json.Options"/>).</summary>
    public string Payload { get; set; } = "";         // JSON of the whole message (envelope included)
    /// <summary>Null for an event (publish to every subscriber); a queue name for a command (send to exactly one service).</summary>
    public string? DestinationQueue { get; set; }     // null = publish (event); "minishop.inventory" = send (command)
    /// <summary>When the row was written (inside the business transaction).</summary>
    public DateTime CreatedAtUtc { get; set; }
    /// <summary>When the dispatcher successfully published it; null while it is still pending.</summary>
    public DateTime? PublishedAtUtc { get; set; }     // null = still pending
    /// <summary>How many times the dispatcher tried to publish this row (used for exponential backoff).</summary>
    public int Attempts { get; set; }
    /// <summary>Earliest time of the next try after a failure; null = try as soon as possible.</summary>
    public DateTime? NextAttemptUtc { get; set; }     // backoff: dispatcher skips the row until this time
    /// <summary>Error of the last failed attempt (e.g. "broker unreachable"), visible in the console during S04.</summary>
    public string? LastError { get; set; }

    /// <summary>Builds a pending outbox row from a message.</summary>
    /// <remarks>
    /// <c>=&gt; new() { ... }</c>: an expression-bodied method that returns a target-typed <c>new</c> with an
    /// object initializer.  <c>message.GetType()</c> is passed to the serializer so the JSON contains the
    /// properties of the REAL type (OrderPlaced's Lines etc.), not only those of the base Message.
    /// </remarks>
    /// <param name="message">The event or command to deliver.</param>
    /// <param name="destinationQueue">Null to publish an event, or a queue name to send a command.</param>
    public static OutboxMessage From(Message message, string? destinationQueue) => new()
    {
        MessageId = message.MessageId,
        CorrelationId = message.CorrelationId,
        ScenarioId = message.ScenarioId,
        Type = message.Type,
        Payload = Json.Serialize(message, message.GetType()),
        DestinationQueue = destinationQueue,
        CreatedAtUtc = DateTime.UtcNow,
    };
}

/// <summary>A DbContext that owns an Outbox table.</summary>
/// <remarks>Lets shared code (dispatcher, extension methods, endpoints) work with any service's DbContext.</remarks>
public interface IOutboxDbContext
{
    /// <summary>The outbox table of this service's database.</summary>
    DbSet<OutboxMessage> Outbox { get; }
}

/// <summary>
/// The two verbs business code uses instead of calling RabbitMQ.  They only ADD a row to the DbContext;
/// nothing is written until the surrounding SaveChanges/commit — which is the whole point.
/// </summary>
/// <remarks>
/// Extension methods on <see cref="IOutboxDbContext"/>: inside a handler you write
/// <c>Db.PublishViaOutbox(new StockReserved(...))</c> as if it were a method of the DbContext.
/// </remarks>
public static class OutboxExtensions
{
    /// <summary>Queue an EVENT for publication (fan-out to all subscribers).</summary>
    /// <remarks><c>@event</c>: the <c>@</c> prefix lets us use the C# keyword "event" as a parameter name.</remarks>
    /// <returns>The added row (useful in tests).</returns>
    public static OutboxMessage PublishViaOutbox(this IOutboxDbContext db, Message @event)
    {
        // No destination queue = "publish": RabbitMQ will copy it to every queue subscribed to this type.
        var row = OutboxMessage.From(@event, destinationQueue: null);
        // Only staged in EF's change tracker; saved together with the business change.
        db.Outbox.Add(row);
        return row;
    }

    /// <summary>Queue a COMMAND for delivery to one specific service queue.</summary>
    /// <param name="db">The service's DbContext.</param>
    /// <param name="command">The command (ReserveStock, ChargeWallet...).</param>
    /// <param name="destinationQueue">One of the <c>Topology</c> queue names.</param>
    /// <returns>The added row (useful in tests).</returns>
    public static OutboxMessage SendViaOutbox(this IOutboxDbContext db, Message command, string destinationQueue)
    {
        // With a destination queue = "send": delivered to exactly that service.
        var row = OutboxMessage.From(command, destinationQueue);
        // Staged only; committed with the business change (no dual write).
        db.Outbox.Add(row);
        return row;
    }
}
