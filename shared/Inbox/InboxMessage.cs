// ==========================================================================================
// FILE: shared/Inbox/InboxMessage.cs
//
// WHAT THIS FILE IS
//   The Inbox table entity (one row per processed message) and the interface a DbContext
//   implements to say "I have an Inbox table".
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Every service's DbContext (OrdersDbContext, InventoryDbContext,
//   PaymentsDbContext, ReadModelDbContext) implements IInboxDbContext, so each service keeps its OWN
//   inbox in its OWN database — services never share tables.
//   The rows are written by shared/Messaging/IdempotentConsumer.cs and shown read-only by
//   /diagnostics/inbox (shared/Demo/DemoEndpoints.cs).
//
// WHICH PATTERN IT IMPLEMENTS
//   Inbox / idempotent receiver: under at-least-once delivery the same message can arrive twice
//   (S06 duplicate delivery, S11 crash after publish).  Storing its MessageId in the SAME transaction
//   as the business change lets the consumer recognise the second copy and ignore it.
//
// Read next: shared/Messaging/IdempotentConsumer.cs (where the inbox is checked and filled).
// ==========================================================================================

using Microsoft.EntityFrameworkCore;

namespace MiniShop.Shared.Inbox;

/// <summary>
/// One row per message that a service has already processed.
/// The primary key is the MessageId, so inserting the same message twice fails and the second
/// delivery is detected inside the SAME transaction as the business write.
/// </summary>
/// <remarks>
/// This is a mutable class with <c>{ get; set; }</c> (not a record) because EF Core materialises and tracks
/// entities by setting their properties.  Each DbContext's OnModelCreating maps it to a table and declares
/// MessageId as the key.  The unique key is the safety net: even if two transactions both passed the
/// "already in inbox?" check, the second insert would fail and roll back its business change.
/// </remarks>
public sealed class InboxMessage
{
    /// <summary>The <c>Message.MessageId</c> of the processed message — the primary key and the de-duplication key.</summary>
    public Guid MessageId { get; set; }
    /// <summary>Short type name ("ReserveStock"), for humans browsing the table.</summary>
    public string MessageType { get; set; } = "";
    /// <summary>The order the message belonged to, so /diagnostics/inbox?orderId=... can filter.</summary>
    public Guid CorrelationId { get; set; }
    /// <summary>When the message was processed (commit time, UTC).</summary>
    public DateTime ReceivedAtUtc { get; set; }
}

/// <summary>A DbContext that owns an Inbox table.</summary>
/// <remarks>
/// The interface lets generic shared code (IdempotentConsumer, DemoEndpoints) use <c>db.Inbox</c> without
/// knowing which concrete service DbContext it is talking to (used as a generic constraint there).
/// </remarks>
public interface IInboxDbContext
{
    /// <summary>The inbox table of this service's database.</summary>
    DbSet<InboxMessage> Inbox { get; }
}
