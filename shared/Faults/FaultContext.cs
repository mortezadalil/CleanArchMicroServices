// ==========================================================================================
// FILE: shared/Faults/FaultContext.cs
//
// WHAT THIS FILE IS
//   An "ambient" holder for the ScenarioId and CorrelationId (OrderId) of the message that is
//   currently being processed on this async flow.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source), part of the fault-injection building block used by the Scenario Lab.
//   IdempotentConsumer.Consume calls FaultContext.Enter(...) at the start of each message; the EF Core
//   interceptor (FaultingSaveChangesInterceptor.cs) reads FaultContext.ScenarioId deep inside
//   SaveChanges, where no message object is available.
//
// WHICH PATTERN IT IMPLEMENTS
//   Ambient context (like HttpContext or Activity.Current): data that flows implicitly with the call,
//   used here so fault injection can be SCOPED BY ScenarioId — a rule created by one experiment only
//   hits that experiment's messages (S07 transient DB failure).
//
// Read next: shared/Faults/FaultingSaveChangesInterceptor.cs.
// ==========================================================================================

namespace MiniShop.Shared.Faults;

/// <summary>
/// Ambient (async-local) scope that tells deep code — e.g. the EF SaveChanges interceptor — which
/// scenario / order is currently being processed, so that scoped fault rules can match without
/// passing ids through every method signature.
/// </summary>
public static class FaultContext
{
    /// <summary>
    /// The current (ScenarioId, CorrelationId) pair, or null outside any message.
    /// </summary>
    /// <remarks>
    /// C# NOTE — <c>AsyncLocal&lt;T&gt;</c>: a static variable whose value is private to the current async
    /// flow.  Two messages processed at the same time (in different consumers) each see their own value,
    /// even though the field is static, and the value follows the code across <c>await</c> points even when
    /// the continuation runs on another thread.  A plain static field would be shared by everyone and mix up
    /// scenarios; [ThreadStatic] would be lost after the first await.
    ///
    /// C# NOTE — tuples: <c>(Guid? ScenarioId, Guid? CorrelationId)</c> is a value tuple with named elements; the
    /// trailing <c>?</c> makes the whole tuple nullable ("no scope entered").
    /// </remarks>
    private static readonly AsyncLocal<(Guid? ScenarioId, Guid? CorrelationId)?> Current = new();

    /// <summary>ScenarioId of the message being processed, or null (normal order / no message in scope).</summary>
    /// <remarks><c>?.</c> returns null instead of throwing when no scope is active.</remarks>
    public static Guid? ScenarioId => Current.Value?.ScenarioId;
    /// <summary>CorrelationId (OrderId) of the message being processed, or null.</summary>
    public static Guid? CorrelationId => Current.Value?.CorrelationId;

    /// <summary>
    /// Sets the ambient ids until the returned object is disposed.  Use with <c>using var _ = FaultContext.Enter(...)</c>.
    /// </summary>
    /// <param name="scenarioId">Scenario of the current message (null for normal orders).</param>
    /// <param name="correlationId">Order of the current message.</param>
    /// <returns>A scope; disposing it restores whatever value was there before (so scopes can nest safely).</returns>
    public static IDisposable Enter(Guid? scenarioId, Guid? correlationId)
    {
        // Remember the outer value, so nested scopes unwind correctly.
        var previous = Current.Value;
        // Set the new ambient value for this async flow (and the flows it starts).
        Current.Value = (scenarioId, correlationId);
        return new Scope(previous);
    }

    /// <summary>Restores the previous ambient value on Dispose (end of the <c>using</c> block).</summary>
    /// <remarks>A private nested class with a primary constructor: <c>previous</c> is captured and used in Dispose.</remarks>
    private sealed class Scope((Guid?, Guid?)? previous) : IDisposable
    {
        /// <summary>Puts the outer value back.</summary>
        public void Dispose() => Current.Value = previous;
    }
}
