namespace MiniShop.Web.Projections;

/// <summary>
/// In-process "something changed" signal.  Projections raise it after a commit; Blazor circuits
/// subscribe and re-render.  Blazor Server already pushes the new render over its SignalR connection,
/// so no extra hub is needed and the browser never talks to RabbitMQ.
/// </summary>
public sealed class UiNotifier
{
    public event Action<string>? Changed;

    /// <summary>Topics: "readmodel", "audit", "scenario", "health".</summary>
    public void Notify(string topic) => Changed?.Invoke(topic);
}
