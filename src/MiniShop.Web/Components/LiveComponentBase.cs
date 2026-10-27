// =====================================================================================================
//  LiveComponentBase.cs  —  base class that makes a page "live" (it re-renders by itself)
// -----------------------------------------------------------------------------------------------------
//  What it is:
//    Most pages inherit from this class (in Razor:  @inherits LiveComponentBase).  A derived page only
//    has to implement RefreshAsync() = "load my data from the read model".  This base class calls it
//    once at start-up and again every time the server says something changed.
//
//  What the student sees on screen:
//    Orders changing from Pending to Confirmed, timeline rows appearing, health dots turning red —
//    all without pressing F5.
//
//  How the update reaches the browser:
//    1. A projection (or the health monitor, or the scenario runner) calls UiNotifier.Notify(topic).
//    2. OnChanged below runs on THAT background thread, so it uses InvokeAsync to hop onto the
//       component's own render context (Blazor components are not thread-safe).
//    3. RefreshAsync reloads data; StateHasChanged tells Blazor "render me again".
//    4. Blazor computes the HTML difference and pushes it over the SignalR circuit to the browser.
//    The browser never polls.
//
//  Data source: whatever the derived page reads in RefreshAsync — always the Web's read model or the
//  read-only diagnostics endpoints, never another service's database.
//
//  Read next: Pages/OrdersPage.razor (the simplest page that inherits from this class).
// =====================================================================================================

using Microsoft.AspNetCore.Components;
using MiniShop.Web.Projections;

namespace MiniShop.Web.Components;

/// <summary>
/// Base class for pages/components that re-render when the read model changes.
/// It subscribes to <see cref="UiNotifier"/>, debounces bursts of notifications, reloads its data
/// (<see cref="RefreshAsync"/>) and calls StateHasChanged.  Blazor Server then pushes the new HTML
/// over its SignalR connection — the browser never polls and never talks to RabbitMQ.
/// </summary>
public abstract class LiveComponentBase : ComponentBase, IDisposable
{
    /// <summary>
    /// The in-process "something changed" event (a singleton shared by all circuits).
    /// [Inject] is Blazor's property injection: components cannot use constructor injection, so the
    /// framework sets this property right after creating the component.
    /// </summary>
    [Inject] protected UiNotifier Notifier { get; set; } = default!;

    // 0 = no refresh scheduled, 1 = a refresh is already waiting.  An int (not a bool) so it can be
    // changed atomically with Interlocked, because notifications arrive on many threads at once.
    private int _refreshPending;
    // Set when the page is closed / navigated away from; afterwards we must not render any more.
    private bool _disposed;

    /// <summary>Which notification topics trigger a refresh.</summary>
    /// <remarks>
    /// Topics: "readmodel" (a projection updated a table), "audit" (new telemetry record),
    /// "health" (a service went up/down), "scenario" (a run started/finished).
    /// A page that only shows orders can override this to ignore, e.g., health changes.
    /// </remarks>
    protected virtual string[] Topics => ["readmodel", "audit", "health", "scenario"];

    /// <summary>Blazor lifecycle method: runs once when the component is created.</summary>
    protected override async Task OnInitializedAsync()
    {
        // Subscribe first, so no change between "load" and "subscribe" can be missed.
        Notifier.Changed += OnChanged;
        // Initial load of the page's data.
        await RefreshAsync();
    }

    /// <summary>Reload the data this component shows.  Implemented by every derived page.</summary>
    protected abstract Task RefreshAsync();

    /// <summary>
    /// Called (on a background thread) for every notification.  It DEBOUNCES: many notifications in a
    /// short burst lead to ONE refresh 300 ms later instead of dozens of database queries and renders.
    /// </summary>
    private void OnChanged(string topic)
    {
        // Ignore topics this page does not care about, and anything after disposal.
        if (_disposed || !Topics.Contains(topic)) return;
        // Atomically set the flag to 1 and get the old value: if it was already 1, a refresh is
        // already scheduled and will pick up this change too.
        if (Interlocked.Exchange(ref _refreshPending, 1) == 1) return;   // a refresh is already scheduled

        // InvokeAsync runs the lambda on the component's synchronization context (the circuit's
        // "UI thread"), which is required before touching component state or calling StateHasChanged.
        // The "_ =" discards the Task on purpose: OnChanged is a plain event handler and must not block.
        _ = InvokeAsync(async () =>
        {
            try
            {
                await Task.Delay(300);                        // coalesce bursts (a saga step emits several records)
                // Clear the flag BEFORE loading, so a change that arrives during the load schedules a new refresh.
                Interlocked.Exchange(ref _refreshPending, 0);
                if (_disposed) return;
                await RefreshAsync();
                // Ask Blazor to re-render; the diff then travels over SignalR to the browser.
                StateHasChanged();
            }
            // The circuit was closed while we waited (tab closed): nothing to do.
            catch (ObjectDisposedException) { }
            // Any other error (e.g. a database briefly locked): reset the flag so the next change retries.
            catch (Exception) { Interlocked.Exchange(ref _refreshPending, 0); }
        });
    }

    /// <summary>
    /// Called by Blazor when the component is removed (navigation or tab closed).  Unsubscribing is
    /// essential: the notifier is a singleton, so a forgotten handler would keep this component alive
    /// in memory forever (a memory leak) and keep refreshing a page nobody sees.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
        Notifier.Changed -= OnChanged;
    }
}
