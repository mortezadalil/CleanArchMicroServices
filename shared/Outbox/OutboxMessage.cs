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
public sealed class OutboxMessage
{
    public long Id { get; set; }                      // insertion order = dispatch order
    public Guid MessageId { get; set; }               // copied from the message envelope
    public Guid CorrelationId { get; set; }
    public Guid? ScenarioId { get; set; }
    public string Type { get; set; } = "";            // short CLR name, e.g. "StockReserved"
    public string Payload { get; set; } = "";         // JSON of the whole message (envelope included)
    public string? DestinationQueue { get; set; }     // null = publish (event); "minishop.inventory" = send (command)
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? PublishedAtUtc { get; set; }     // null = still pending
    public int Attempts { get; set; }
    public DateTime? NextAttemptUtc { get; set; }     // backoff: dispatcher skips the row until this time
    public string? LastError { get; set; }

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
public interface IOutboxDbContext
{
    DbSet<OutboxMessage> Outbox { get; }
}

public static class OutboxExtensions
{
    /// <summary>Queue an EVENT for publication (fan-out to all subscribers).</summary>
    public static OutboxMessage PublishViaOutbox(this IOutboxDbContext db, Message @event)
    {
        var row = OutboxMessage.From(@event, destinationQueue: null);
        db.Outbox.Add(row);
        return row;
    }

    /// <summary>Queue a COMMAND for delivery to one specific service queue.</summary>
    public static OutboxMessage SendViaOutbox(this IOutboxDbContext db, Message command, string destinationQueue)
    {
        var row = OutboxMessage.From(command, destinationQueue);
        db.Outbox.Add(row);
        return row;
    }
}
