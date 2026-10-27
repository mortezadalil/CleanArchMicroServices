// ==================================================================================================
// FILE: src/MiniShop.Web/Projections/UiNotifier.cs
//
// WHAT IT IS
//   A tiny in-process publish/subscribe signal: "data on topic X changed, re-render if you care".
//
// WHERE IT SITS IN THE ARCHITECTURE
//   The bridge between the message side and the UI side of MiniShop.Web:
//     RabbitMQ → projection consumer → commit → UiNotifier.Notify("readmodel")
//            → every Blazor component subscribed to Changed → re-query ReadModelQueries → re-render
//            → Blazor Server sends the DOM diff to the browser over its SignalR connection.
//   So the browser updates "live" although it never talks to RabbitMQ and we wrote no SignalR hub.
//
// Registered as a SINGLETON in Program.cs: consumers (one DI scope per message) and Blazor circuits
// (one DI scope per browser tab) must share the SAME instance, or the signal would reach no one.
//
// Read next: Queries/ReadModelQueries.cs (what a page does after it is notified).
// ==================================================================================================

namespace MiniShop.Web.Projections;

/// <summary>
/// In-process "something changed" signal.  Projections raise it after a commit; Blazor circuits
/// subscribe and re-render.  Blazor Server already pushes the new render over its SignalR connection,
/// so no extra hub is needed and the browser never talks to RabbitMQ.
/// Only works inside ONE Web process; with several Web instances each would notify only its own users
/// (fine here, because each instance also has its own read model).
/// </summary>
public sealed class UiNotifier
{
    /// <summary>
    /// Raised with the topic name.  Subscribers run on the caller's thread (a message consumer or a
    /// background job), so a Blazor component must marshal back with InvokeAsync(StateHasChanged), and
    /// must unsubscribe in Dispose or the singleton keeps the closed circuit alive (memory leak).
    /// </summary>
    public event Action<string>? Changed;

    /// <summary>
    /// Topics: "readmodel", "audit", "scenario", "health".
    /// Call it only AFTER the data is committed, otherwise a page could re-query and not see it yet.
    /// The ?. makes it a no-op when no page is open.
    /// </summary>
    public void Notify(string topic) => Changed?.Invoke(topic);
}
