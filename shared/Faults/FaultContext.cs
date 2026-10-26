namespace MiniShop.Shared.Faults;

/// <summary>
/// Ambient (async-local) scope that tells deep code — e.g. the EF SaveChanges interceptor — which
/// scenario / order is currently being processed, so that scoped fault rules can match without
/// passing ids through every method signature.
/// </summary>
public static class FaultContext
{
    private static readonly AsyncLocal<(Guid? ScenarioId, Guid? CorrelationId)?> Current = new();

    public static Guid? ScenarioId => Current.Value?.ScenarioId;
    public static Guid? CorrelationId => Current.Value?.CorrelationId;

    public static IDisposable Enter(Guid? scenarioId, Guid? correlationId)
    {
        var previous = Current.Value;
        Current.Value = (scenarioId, correlationId);
        return new Scope(previous);
    }

    private sealed class Scope((Guid?, Guid?)? previous) : IDisposable
    {
        public void Dispose() => Current.Value = previous;
    }
}
