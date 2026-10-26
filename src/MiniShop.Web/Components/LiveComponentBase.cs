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
    [Inject] protected UiNotifier Notifier { get; set; } = default!;

    private int _refreshPending;
    private bool _disposed;

    /// <summary>Which notification topics trigger a refresh.</summary>
    protected virtual string[] Topics => ["readmodel", "audit", "health", "scenario"];

    protected override async Task OnInitializedAsync()
    {
        Notifier.Changed += OnChanged;
        await RefreshAsync();
    }

    protected abstract Task RefreshAsync();

    private void OnChanged(string topic)
    {
        if (_disposed || !Topics.Contains(topic)) return;
        if (Interlocked.Exchange(ref _refreshPending, 1) == 1) return;   // a refresh is already scheduled

        _ = InvokeAsync(async () =>
        {
            try
            {
                await Task.Delay(300);                        // coalesce bursts (a saga step emits several records)
                Interlocked.Exchange(ref _refreshPending, 0);
                if (_disposed) return;
                await RefreshAsync();
                StateHasChanged();
            }
            catch (ObjectDisposedException) { }
            catch (Exception) { Interlocked.Exchange(ref _refreshPending, 0); }
        });
    }

    public void Dispose()
    {
        _disposed = true;
        Notifier.Changed -= OnChanged;
    }
}
