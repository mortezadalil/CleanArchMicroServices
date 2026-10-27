// ==================================================================================================
// FILE: src/MiniShop.Payments/Consumers/CancelPaymentConsumer.cs
//
// WHAT THIS FILE IS
//   The handler for the CancelPayment COMMAND: the saga waited too long for an answer to ChargeWallet
//   and now asks "did you take the money or not?  If not, make sure you never will."
//
// WHY IT EXISTS: "NO ANSWER" DOES NOT MEAN "NOT DONE"
//   When the saga times out, it does not know what happened.  The ChargeWallet may still be in the queue,
//   may be failing and retrying (S10), or may already have debited the wallet while the PaymentCompleted
//   answer is still on its way (S13).  If the saga just rejected the order and released the stock, in the
//   S13 case the customer would pay for an order that was cancelled.  So the saga RECONCILES first: it
//   asks Payments for a definitive answer, and only then decides.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   Orders (saga) --CancelPayment (sent to minishop.payments)--> THIS CONSUMER
//   THIS CONSUMER --PaymentCancelled        (published)--> saga releases stock and rejects the order
//   THIS CONSUMER --PaymentAlreadyCompleted (published)--> saga confirms the order (the money was taken)
//
// PATTERNS: reconciliation, tombstone (a Cancelled row in the single (OrderId, "Debit") slot),
//           idempotent consumer (Inbox) + business idempotency (re-sending CancelPayment gives the same answer).
//
// Read next: Consumers/SnapshotRequestedConsumer.cs
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
/// The saga timed out waiting for a payment and needs a DEFINITIVE answer (S10 / S13):
///   - Completed debit exists → PaymentAlreadyCompleted (the saga must confirm the order; money was taken).
///   - Failed/Cancelled exists → PaymentCancelled (nothing happened, nothing can happen).
///   - nothing exists          → write a Cancelled TOMBSTONE in the (OrderId,"Debit") slot, then PaymentCancelled.
/// Because the tombstone occupies the unique key, a ChargeWallet that arrives later can never debit.
/// </summary>
/// <param name="db">The Payments database (wallets, ledger, Inbox, Outbox), a fresh instance per message.</param>
/// <param name="diagnostics">Telemetry publisher for the Developer Console.</param>
/// <param name="signal">Wakes the OutboxDispatcher after commit.</param>
/// <param name="faults">Fault-injection rules (Development only), used for the FaultPoint below.</param>
/// <param name="logger">Standard logger.</param>
public sealed class CancelPaymentConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<CancelPaymentConsumer> logger)
    : IdempotentConsumer<CancelPayment, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>Fault point "Payments.CancelPayment" (applied by the base class before the transaction).</summary>
    protected override string FaultPoint => FaultPoints.PaymentsCancel;

    /// <summary>
    /// Reconciles the payment of one order and gives the saga a final answer.  Runs inside the base class's
    /// transaction.  Because Payments handles one message at a time and every decision lives in the same
    /// (OrderId, "Debit") row, ChargeWallet and CancelPayment can never both "win": whichever is processed
    /// first fills the slot, and the other one reads it.
    /// </summary>
    /// <param name="context">The delivered CancelPayment command (OrderId + Reason) and its cancellation token.</param>
    /// <returns>A human-readable detail and a ledger snapshot for the Developer Console.</returns>
    protected override async Task<HandleResult> HandleAsync(ConsumeContext<CancelPayment> context)
    {
        // The command: which order, and why the saga cancels (e.g. "payment timed out").
        var command = context.Message;
        // Cancellation token (service shutdown).
        var ct = context.CancellationToken;

        // Look up the order's single debit slot: has any decision been recorded yet?
        var existing = await Db.Ledger.FirstOrDefaultAsync(l => l.OrderId == command.OrderId && l.OperationType == LedgerEntry.DebitOperation, ct);
        // If so, also load the customer's wallet so the answer can report the balance (CancelPayment itself does
        // not carry a CustomerId; the ledger row knows it).  No row → no wallet needed.
        var wallet = existing is null ? null : await Db.Wallets.FirstOrDefaultAsync(w => w.CustomerId == existing.CustomerId, ct);

        if (existing?.State == LedgerState.Completed)
        {
            // CASE 1 (S13): the money WAS taken; only the answer was late.  We cannot "cancel" a payment that
            // happened (that would need a refund), so we refuse the cancellation and tell the saga the truth:
            // PaymentAlreadyCompleted.  The saga then confirms the order instead of rejecting it, so the
            // customer gets the goods they paid for.
            Db.PublishViaOutbox(ChargeWalletConsumer.Stamp(new PaymentAlreadyCompleted(command.OrderId, existing.Amount, wallet?.Balance ?? 0), command));
            // wallet! : a Completed row always has a real customer whose wallet exists (it was just debited).
            return new HandleResult("cancel refused: the debit already happened → PaymentAlreadyCompleted", ChargeWalletConsumer.Snapshot(wallet!, existing));
        }

        if (existing is null)
        {
            // CASE 2 (S10): no decision yet; the ChargeWallet has not been processed (it may still be queued or
            // failing).  Write a TOMBSTONE into the order's single debit slot.  Because of the unique
            // (OrderId, OperationType) index, the late ChargeWallet will find this Cancelled row and refuse,
            // so it can never take money for an order the saga has already given up on.
            existing = new LedgerEntry
            {
                OrderId = command.OrderId,
                // Unknown here (CancelPayment carries no customer) and irrelevant: no money moves.
                CustomerId = "",
                // Nothing will ever be charged through this row.
                Amount = 0,
                Direction = LedgerDirection.Debit,
                // Same operation type as a real debit: that is exactly what makes it occupy the slot.
                OperationType = LedgerEntry.DebitOperation,
                State = LedgerState.Cancelled,
                // Remember why, for the diagnostics view.
                Reason = command.Reason,
                CreatedAtUtc = DateTime.UtcNow,
            };
            // Mark for insertion (saved and committed by the base class together with the Inbox row and the answer).
            Db.Ledger.Add(existing);
        }

        // CASE 2 continued, and CASE 3 (a Failed or Cancelled row already existed): no money moved and none can
        // move anymore, so the saga may safely release the stock and reject the order.  Answering again for a
        // repeated CancelPayment is harmless: the answer is the same every time.
        Db.PublishViaOutbox(ChargeWalletConsumer.Stamp(new PaymentCancelled(command.OrderId, command.Reason), command));
        return new HandleResult($"payment cancelled ({existing.State} row occupies the debit slot) → PaymentCancelled",
            new { ledger = new { existing.OrderId, State = existing.State.ToString(), existing.Reason } });
    }
}
