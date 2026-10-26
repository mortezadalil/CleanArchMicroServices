using MiniShop.Contracts;

namespace MiniShop.Shared.Messaging;

/// <summary>
/// Maps the short message type name stored in the outbox ("OrderPlaced") back to the CLR type,
/// so the dispatcher can deserialize the payload and publish it with the right MassTransit type.
/// </summary>
public static class ContractTypes
{
    private static readonly Dictionary<string, Type> ByName = typeof(Message).Assembly
        .GetTypes()
        .Where(t => !t.IsAbstract && t.IsSubclassOf(typeof(Message)))
        .ToDictionary(t => t.Name, t => t);

    public static Type Resolve(string name) =>
        ByName.TryGetValue(name, out var type)
            ? type
            : throw new InvalidOperationException($"Unknown message type '{name}'. Is it defined in contracts/?");
}
