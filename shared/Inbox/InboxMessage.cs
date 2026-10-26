using Microsoft.EntityFrameworkCore;

namespace MiniShop.Shared.Inbox;

/// <summary>
/// One row per message that a service has already processed.
/// The primary key is the MessageId, so inserting the same message twice fails and the second
/// delivery is detected inside the SAME transaction as the business write.
/// </summary>
public sealed class InboxMessage
{
    public Guid MessageId { get; set; }
    public string MessageType { get; set; } = "";
    public Guid CorrelationId { get; set; }
    public DateTime ReceivedAtUtc { get; set; }
}

/// <summary>A DbContext that owns an Inbox table.</summary>
public interface IInboxDbContext
{
    DbSet<InboxMessage> Inbox { get; }
}
