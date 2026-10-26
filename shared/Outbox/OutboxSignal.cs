namespace MiniShop.Shared.Outbox;

/// <summary>
/// Lets a writer wake the dispatcher immediately after a commit instead of waiting for the next poll.
/// Purely a latency optimisation: the dispatcher also polls, so a missed signal is harmless.
/// </summary>
public sealed class OutboxSignal
{
    private readonly SemaphoreSlim _semaphore = new(0, 1);

    public void Notify()
    {
        if (_semaphore.CurrentCount == 0)
            _semaphore.Release();
    }

    public Task WaitAsync(TimeSpan timeout, CancellationToken ct) => _semaphore.WaitAsync(timeout, ct);
}
