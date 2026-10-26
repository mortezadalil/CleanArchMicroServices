using System.Text.Json;
using System.Text.Json.Serialization;

namespace MiniShop.Shared.Messaging;

/// <summary>One JSON configuration for everything we store (outbox payloads, snapshots, audit).</summary>
public static class Json
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);
    public static string Serialize(object value, Type type) => JsonSerializer.Serialize(value, type, Options);
    public static T? Deserialize<T>(string json) => JsonSerializer.Deserialize<T>(json, Options);
    public static object? Deserialize(string json, Type type) => JsonSerializer.Deserialize(json, type, Options);
}
