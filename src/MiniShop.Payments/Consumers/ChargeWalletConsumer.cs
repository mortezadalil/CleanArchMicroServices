// ==================================================================================================
// FILE: src/MiniShop.Payments/Consumers/ChargeWalletConsumer.cs
//
// WHAT THIS FILE IS
//   The handler for the ChargeWallet COMMAND: "take <Amount> from <CustomerId>'s wallet for order X".
//   It runs after Inventory reserved the stock.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Orders (saga) --ChargeWallet (sent to queue minishop.payments)--> THIS CONSUMER
//   THIS CONSUMER --PaymentCompleted or PaymentFailed (published)--> Orders saga (+ Web read model)
//   After a successful debit it also publishes WalletBalanceChanged for the Web's wallet summary.
//
// PATTERNS
//   - Idempotent consumer (Inbox), inherited from shared/Messaging/IdempotentConsumer.cs: ONE transaction
//     covers inbox check → this handler → inbox row → SaveChanges → commit.  HandleAsync never saves itself.
//   - Business idempotency on (OrderId, "Debit"): the Inbox only stops the SAME MessageId.  If two DIFFERENT
//     messages ask to charge the same order (S06 duplicate dispatch, a saga retry), the ledger row found
//     below makes the second one repeat the first answer.  The unique index is the safety net if both
//     arrive at the very same moment.  Result: never two debits for one order.
//   - Refusal is a business answer, not an exception: insufficient funds produces a Failed ledger row and a
//     PaymentFailed event, and the transaction COMMITS.  Throwing would only cause pointless retries
//     (the balance will not grow by retrying) and finally park the message in the error queue.
//   - Tombstone: if the saga already cancelled this payment (CancelPayment wrote a Cancelled row), a late
//     ChargeWallet is refused and never debits (S10).
//   - Transactional Outbox: the answer is committed together with the debit, so "money taken" and
//     "PaymentCompleted will be published" can never get out of sync.
//
// Read next: Consumers/CancelPaymentConsumer.cs (what happens when the saga stops waiting).
// ==================================================================================================

using MassTransit;
using Microsoft.EntityFrameworkCore;
using MiniShop.Contracts;
using MiniShop.Payments.Data;
using MiniShop.Payments.Domain;
using MiniShop.Shared.Diagnostics;
using MiniShop.Shared.Faults;
using MiniShop.Shared.Messaging;
using MiniShop.Shared.Outbox;

namespace MiniShop.Payments.Consumers;

/// <summary>
/// Debits the wallet exactly once per order.
///   - A ledger row for (OrderId, "Debit") already exists → repeat the earlier answer, move no money.
///   - Insufficient funds or an injected "decline"            → Failed row + PaymentFailed.
///   - Otherwise                                              → Balance -= Amount, Completed row, PaymentCompleted.
/// The wallet update, the ledger row, the Inbox row and the Outbox rows are one SQLite transaction.
/// </summary>
/// <param name="db">The Payments database (wallets, ledger, Inbox, Outbox), a fresh instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit so the answer leaves immediately.</param>
/// <param name="faults">Fault-injection rules (Development only), used for the FaultPoint below.</param>
/// <param name="logger">Standard logger.</param>
public sealed class ChargeWalletConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ChargeWalletConsumer> logger)
    : IdempotentConsumer<ChargeWallet, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>
    /// Fault point "Payments.ChargeWallet": Fail (crash → retry; permanent in S10 → error queue),
    /// Delay (slow / late payment, S13), Pause (stopped consumer, S05), Reject (decline).
    /// Applied by the base class BEFORE the transaction, so a long delay never holds the database lock.
    /// </summary>
    protected override string FaultPoint => FaultPoints.PaymentsCharge;

    /// <summary>The business logic of "charge the wallet for an order". Runs inside the base class's transaction.</summary>
    /// <param name="context">The delivered ChargeWallet command (OrderId, CustomerId, Amount) and its cancellation token.</param>
    /// <returns>A human-readable detail and a wallet/ledger snapshot for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ChargeWallet> context)
    {
        // The command: which order, whose wallet, how much.
        var command = context.Message;
        // Cancellation token (service shutdown).
        var ct = context.CancellationToken;

        // Load the customer's wallet (an empty one is created for an unknown customer, so the charge is
        // then refused for insufficient funds rather than crashing).
        var wallet = await Db.GetOrCreateWalletAsync(command.CustomerId, ct);
        // BUSINESS IDEMPOTENCY: has a decision already been recorded for this order's debit?
        var existing = await Db.Ledger.FirstOrDefaultAsync(l => l.OrderId == command.OrderId && l.OperationType == LedgerEntry.DebitOperation, ct);
        // Yes (Completed, Failed or a Cancelled tombstone) → move NO money; just repeat the decision, so the
        // saga that sent this (possibly duplicated or retried) command still gets an answer.
        if (existing is not null)
            return ReAnswer(existing, wallet, command);

        // Decide whether the payment is refused.  null = may proceed.
        //   1. an injected "Reject" fault (Scenario Lab: "decline the next payment") wins, with its own reason;
        //   2. otherwise the real rule: the balance must cover the amount (S03 insufficient wallet).
        // Note: a refusal is NOT an exception; it is an answer we record and publish.
        string? refusal = Fault.Reject ? Fault.Reason
            : wallet.Balance < command.Amount ? $"insufficient funds: balance {wallet.Balance:N0} < amount {command.Amount:N0}"
            : null;

        // Create the ledger row for (OrderId, "Debit") now, for BOTH outcomes: from this moment the order's single
        // debit slot is taken, whether the answer is "paid" or "refused".  Its State is set below.
        var entry = new LedgerEntry
        {
            OrderId = command.OrderId,
            CustomerId = command.CustomerId,
            Amount = command.Amount,
            Direction = LedgerDirection.Debit,
            OperationType = LedgerEntry.DebitOperation,
            CreatedAtUtc = DateTime.UtcNow,
        };
        // Mark for insertion.  If a concurrent message already inserted this (OrderId, "Debit") row, the unique
        // index makes SaveChanges fail → rollback → retry → the retry sees the row and takes the ReAnswer path.
        Db.Ledger.Add(entry);

        if (refusal is not null)
        {
            // REFUSED: record it, so a repeated ChargeWallet gets the same "no" with the same reason.
            entry.State = LedgerState.Failed;
            entry.Reason = refusal;
            // Tell the saga.  It will compensate (send ReleaseStock to Inventory) and reject the order.
            // The wallet is untouched, so BalanceAfter is simply the current balance.
            Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, refusal, wallet.Balance), command));
            // Normal return: the Failed row, the Inbox row and the event are committed together.
            return new HandleResult($"payment refused: {refusal}", Snapshot(wallet, entry));
        }

        // ACCEPTED: take the money.
        wallet.Balance -= command.Amount;
        // Concurrency token: if another transaction changed this wallet since we read it, SaveChanges throws and
        // the message is retried with the fresh balance; the same money can never be spent twice.
        wallet.Version++;
        // Record that money moved.
        entry.State = LedgerState.Completed;
        // The answer the saga waits for (it will then confirm the order), with the new balance as evidence.
        Db.PublishViaOutbox(Stamp(new PaymentCompleted(command.OrderId, command.CustomerId, command.Amount, wallet.Balance), command));
        // A second event for the Web's read model: the wallet summary must show the new balance.
        Db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, command));
        // The base class adds the Inbox row and commits debit + ledger + inbox + outbox atomically.
        return new HandleResult($"debited {command.Amount:N0}; balance now {wallet.Balance:N0}", Snapshot(wallet, entry));
    }

    /// <summary>
    /// A decision for this order's debit already exists: repeat it instead of changing anything.
    /// Always publishes an answer, so a saga that is (re)asking is never left waiting, and never moves money.
    /// </summary>
    /// <param name="existing">The ledger row found for (OrderId, "Debit").</param>
    /// <param name="wallet">The customer's wallet (for the BalanceAfter value of the answer).</param>
    /// <param name="command">The repeated command; used to stamp the answer.</param>
    private HandleResult ReAnswer(LedgerEntry existing, Wallet wallet, ChargeWallet command)
    {
        switch (existing.State)
        {
            // Already paid: re-publish PaymentCompleted with the ORIGINAL amount; do NOT debit a second time (S06).
            case LedgerState.Completed:
                Db.PublishViaOutbox(Stamp(new PaymentCompleted(command.OrderId, command.CustomerId, existing.Amount, wallet.Balance), command));
                return new HandleResult("order already debited; PaymentCompleted re-published, no second debit", Snapshot(wallet, existing));
            // TOMBSTONE: the saga cancelled the payment (CancelPayment) before this ChargeWallet was processed,
            // e.g. it was stuck in a retry or delayed (S10).  The order is already being rejected and its stock
            // released, so debiting now would take money for goods the customer will never get.  Refuse.
            case LedgerState.Cancelled:
                Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, "payment window was cancelled (tombstone)", wallet.Balance), command));
                return new HandleResult("late ChargeWallet refused: payment was cancelled (tombstone)", Snapshot(wallet, existing));
            // Failed earlier: repeat the same "no" with the stored reason.  We deliberately do NOT re-evaluate the
            // balance: an order gets ONE decision, otherwise a retry could flip "refused" into "paid" after the saga
            // already compensated.
            default:
                var reason = existing.Reason ?? "payment previously failed";
                Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, reason, wallet.Balance), command));
                return new HandleResult($"earlier answer repeated: failed ({reason})", Snapshot(wallet, existing));
        }
    }

    /// <summary>
    /// Small anonymous object describing the wallet and the ledger row, shown as "evidence" in the Developer
    /// Console (it is serialized into the telemetry record, never sent to other services).
    /// Also used by CancelPaymentConsumer (hence <c>internal</c>).
    /// </summary>
    /// <param name="wallet">The wallet to show (customer + balance).</param>
    /// <param name="entry">The related ledger row, or null.</param>
    internal static object Snapshot(Wallet wallet, LedgerEntry? entry) => new
    {
        wallet = new { wallet.CustomerId, wallet.Balance },
        ledger = entry is null ? null : new { entry.OrderId, entry.Amount, State = entry.State.ToString(), entry.OperationType },
    };

    /// <summary>
    /// Copies the tracing envelope of the causing command onto an answer (records are immutable, so
    /// <c>with { ... }</c> makes a modified copy).  Also used by CancelPaymentConsumer.
    /// </summary>
    /// <typeparam name="T">The concrete event type.</typeparam>
    /// <param name="event">The freshly built answer.</param>
    /// <param name="cause">The command that triggered it.</param>
    internal static T Stamp<T>(T @event, Message cause) where T : Message => @event with
    {
        // Same flow (= OrderId), so the saga can match the answer to its order.
        CorrelationId = cause.CorrelationId,
        // Parent link: "caused by that command" (the arrows in the timeline).
        CausationId = cause.MessageId,
        // Keep the Scenario Lab experiment id along the whole chain.
        ScenarioId = cause.ScenarioId,
        // Who produced the event.
        Producer = ServiceNames.Payments,
    };
}
