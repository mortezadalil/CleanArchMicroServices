namespace MiniShop.Shared.Demo;

/// <summary>
/// Settings of the Development-only control plane (fault injection, reset, read-only diagnostics).
/// Enabled defaults to false; appsettings.Development.json and compose.yaml turn it on explicitly.
/// </summary>
public sealed class DemoOptions
{
    public const string Section = "Demo";
    public bool Enabled { get; set; }
    /// <summary>Shared secret sent by the Web in the X-Demo-Token header. Local demo only.</summary>
    public string Token { get; set; } = "minishop-dev";
}
