// ==========================================================================================
// FILE: shared/Messaging/Json.cs
//
// WHAT THIS FILE IS
//   The single JSON configuration used whenever MiniShop itself turns objects into text:
//   Outbox payloads, diagnostic snapshots (DiagnosticEvent.PayloadJson / SnapshotJson) and the audit.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Used by shared/Outbox/OutboxMessage.cs (serialize when storing),
//   shared/Outbox/OutboxDispatcher.cs (deserialize before publishing) and
//   shared/Diagnostics/DiagnosticsPublisher.cs.  (MassTransit has its own serializer for the wire
//   format; this class is only for what WE store.)
//
// WHY ONE PLACE
//   If the writer and the reader of an outbox row used different settings (for example different
//   property-name casing), a stored message could fail to deserialize later — a poison row.
//   Keeping one shared options object makes that impossible.
//
// Read next: shared/Outbox/OutboxMessage.cs.
// ==========================================================================================

using System.Text.Json;
using System.Text.Json.Serialization;

namespace MiniShop.Shared.Messaging;

/// <summary>One JSON configuration for everything we store (outbox payloads, snapshots, audit).</summary>
public static class Json
{
    /// <summary>
    /// The shared serializer settings.  <c>JsonSerializerDefaults.Web</c> = camelCase property names and
    /// case-insensitive reading (the same conventions ASP.NET Core uses), so stored JSON looks like API JSON.
    /// </summary>
    /// <remarks>
    /// <c>Converters = { ... }</c> is a collection initializer on an existing, get-only list: it calls
    /// <c>Converters.Add(...)</c> for each element.
    /// A single static instance also matters for performance: System.Text.Json caches type metadata per
    /// options instance, so creating new options for every call would be slow.
    /// </remarks>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        // Compact output: rows in the outbox/audit tables stay small.
        WriteIndented = false,
        // Leave out properties whose value is null: smaller rows, easier to read in the console.
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // Enums are written as their names ("Confirmed") instead of numbers (4): readable and safe if
        // someone reorders the enum members later.
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>Serializes a value using its compile-time type <typeparamref name="T"/>.</summary>
    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    /// <summary>
    /// Serializes a value using an explicit RUNTIME type.  Important for messages: a variable typed as the
    /// base <c>Message</c> would otherwise only write the base-class properties and lose OrderId, Lines, etc.
    /// </summary>
    /// <param name="value">The object to serialize.</param>
    /// <param name="type">Its real type, usually <c>value.GetType()</c>.</param>
    public static string Serialize(object value, Type type) => JsonSerializer.Serialize(value, type, Options);

    /// <summary>Deserializes JSON into a known compile-time type (null if the JSON is the literal null).</summary>
    public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options);

    /// <summary>Deserializes JSON into a type only known at runtime (the outbox uses this with <see cref="ContractTypes.Resolve"/>).</summary>
    public static object? Deserialize(string json, Type type) => JsonSerializer.Deserialize(json, type, Options);
}
