// ==========================================================================================
// FILE: shared/Messaging/ContractTypes.cs
//
// WHAT THIS FILE IS
//   A small lookup table: short message type name ("OrderPlaced") → CLR type (typeof(OrderPlaced)).
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ is linked as source into every service (like contracts/).  This helper is used by the
//   Outbox dispatcher (shared/Outbox/OutboxDispatcher.cs): an Outbox row only stores the message as
//   JSON text plus its short type name, and before publishing we must turn that text back into a
//   real, strongly typed object so MassTransit routes it to the right exchange.
//
// WHICH PATTERN IT IMPLEMENTS
//   Part of the Transactional Outbox: (de)serialization of stored messages.
//
// Read next: shared/Outbox/OutboxMessage.cs (where the name is stored) and
//            shared/Outbox/OutboxDispatcher.cs (where Resolve is called).
// ==========================================================================================

using MiniShop.Contracts;

namespace MiniShop.Shared.Messaging;

/// <summary>
/// Maps the short message type name stored in the outbox ("OrderPlaced") back to the CLR type,
/// so the dispatcher can deserialize the payload and publish it with the right MassTransit type.
/// </summary>
public static class ContractTypes
{
    /// <summary>
    /// Built ONCE (static readonly field, initialised the first time the class is used) by scanning the
    /// assembly that contains <see cref="Message"/> — i.e. the service's own assembly, because contracts/
    /// is compiled into it — for every concrete type deriving from Message.
    /// Reflection is slow, so doing it once and caching the result in a Dictionary keeps Resolve cheap.
    /// </summary>
    /// <remarks>
    /// The LINQ chain reads top to bottom: take all types → keep the non-abstract subclasses of Message
    /// (the abstract base itself cannot be instantiated) → index them by short Name.
    /// ToDictionary would throw at startup if two message types had the same short name, which is a
    /// useful early warning: short names must be unique for the outbox to work.
    /// </remarks>
    private static readonly Dictionary<string, Type> ByName = typeof(Message).Assembly
        .GetTypes()
        .Where(t => !t.IsAbstract && t.IsSubclassOf(typeof(Message)))
        .ToDictionary(t => t.Name, t => t);

    /// <summary>Returns the CLR type for a short message name, or throws a clear error if it is unknown.</summary>
    /// <remarks>
    /// The <c>cond ? a : throw ...</c> form is a "throw expression" (C# 7): it lets us throw inside an
    /// expression.  Failing loudly matters: an unknown name means a row in the outbox can never be
    /// published, and the dispatcher records the error on that row so it is visible instead of silently lost.
    /// </remarks>
    /// <param name="name">Short type name as stored in OutboxMessage.Type, e.g. "StockReserved".</param>
    public static Type Resolve(string name) =>
        ByName.TryGetValue(name, out var type)
            ? type
            : throw new InvalidOperationException($"Unknown message type '{name}'. Is it defined in contracts/?");
}
