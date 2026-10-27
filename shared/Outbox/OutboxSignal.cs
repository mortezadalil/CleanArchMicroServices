// ==========================================================================================
// FILE: shared/Outbox/OutboxSignal.cs
//
// WHAT THIS FILE IS
//   A tiny in-process "doorbell" between code that just committed Outbox rows and the
//   OutboxDispatcher background service.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source).  Registered as a singleton in ServiceHostingExtensions.cs.
//   Rung by IdempotentConsumer (after commit) and by API endpoints that write to the outbox;
//   waited on by OutboxDispatcher between batches.
//
// WHICH PATTERN IT IMPLEMENTS
//   Part of the Transactional Outbox: a latency optimisation on top of polling.  Correctness never
//   depends on it — the dispatcher polls every second anyway.
//
// Read next: shared/Outbox/OutboxDispatcher.cs (ExecuteAsync is the waiting side).
// ==========================================================================================

namespace MiniShop.Shared.Outbox;

/// <summary>
/// Lets a writer wake the dispatcher immediately after a commit instead of waiting for the next poll.
/// Purely a latency optimisation: the dispatcher also polls, so a missed signal is harmless.
/// </summary>
/// <remarks>
/// C# NOTE — <c>SemaphoreSlim</c>: a lightweight counter of "permits".  <c>WaitAsync</c> takes a permit
/// (or waits asynchronously, without blocking a thread, until one is available or the timeout passes);
/// <c>Release</c> adds a permit.  Here it is used as an async auto-reset event: at most one pending wake-up.
/// </remarks>
public sealed class OutboxSignal
{
    /// <summary>Starts with 0 permits (nothing to do yet) and allows at most 1 (many notifications collapse into one wake-up).</summary>
    private readonly SemaphoreSlim _semaphore = new(0, 1);

    /// <summary>Called after a commit that added outbox rows: "there is work, wake up".</summary>
    public void Notify()
    {
        // Only release when no permit is pending.  Releasing past the maximum (1) would throw
        // SemaphoreFullException; ten commits in a row only need ONE wake-up, because the dispatcher
        // reads ALL pending rows in its batch anyway.  (The check-then-release is not atomic, but a lost or
        // extra wake-up is harmless here — see the class summary.)
        if (_semaphore.CurrentCount == 0)
            _semaphore.Release();
    }

    /// <summary>
    /// Waits until <see cref="Notify"/> is called or <paramref name="timeout"/> passes, whichever comes first.
    /// The timeout is the fallback poll that guarantees progress even if a signal was missed (e.g. rows left
    /// over from before a restart, S09, or rows waiting for their backoff to expire, S04).
    /// </summary>
    /// <param name="timeout">Maximum wait (the poll interval).</param>
    /// <param name="ct">Cancelled on shutdown.</param>
    public Task WaitAsync(TimeSpan timeout, CancellationToken ct) => _semaphore.WaitAsync(timeout, ct);
}
