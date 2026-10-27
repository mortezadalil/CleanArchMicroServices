// ==========================================================================================
// FILE: shared/Demo/DemoOptions.cs
//
// WHAT THIS FILE IS
//   The settings of the Development-only "control plane": the endpoints that let the Web's
//   Scenario Lab inject faults, reset data and read diagnostic tables in the other services.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Bound from the "Demo" configuration section in
//   ServiceHostingExtensions.cs; read by shared/Demo/DemoEndpoints.cs (MapDemoGroup).
//
// WHY IT EXISTS
//   Fault injection and "delete all data" are dangerous.  They are OFF by default and must be
//   switched on explicitly (appsettings.Development.json, or Demo__Enabled=true in compose.yaml),
//   and every call must carry a shared token.  A real production system would not ship them at all.
//
// Read next: shared/Demo/DemoEndpoints.cs.
// ==========================================================================================

namespace MiniShop.Shared.Demo;

/// <summary>
/// Settings of the Development-only control plane (fault injection, reset, read-only diagnostics).
/// Enabled defaults to false; appsettings.Development.json and compose.yaml turn it on explicitly.
/// </summary>
public sealed class DemoOptions
{
    /// <summary>Name of the configuration section ("Demo"), kept next to the class so binding code cannot misspell it.</summary>
    public const string Section = "Demo";
    /// <summary>Master switch.  False (the default for a <c>bool</c>) = the /demo and /diagnostics endpoints are not even mapped.</summary>
    public bool Enabled { get; set; }
    /// <summary>Shared secret sent by the Web in the X-Demo-Token header. Local demo only.</summary>
    public string Token { get; set; } = "minishop-dev";
}
