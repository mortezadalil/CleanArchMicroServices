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
public sealed class CancelPaymentConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<CancelPaymentConsumer> logger)
    : IdempotentConsumer<CancelPayment, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    protected override string FaultPoint => FaultPoints.PaymentsCancel;

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<CancelPayment> context)
    {
        var command = context.Message;
        var ct = context.CancellationToken;

        var existing = await Db.Ledger.FirstOrDefaultAsync(l => l.OrderId == command.OrderId && l.OperationType == LedgerEntry.DebitOperation, ct);
        var wallet = existing is null ? null : await Db.Wallets.FirstOrDefaultAsync(w => w.CustomerId == existing.CustomerId, ct);

        if (existing?.State == LedgerState.Completed)
        {
            Db.PublishViaOutbox(ChargeWalletConsumer.Stamp(new PaymentAlreadyCompleted(command.OrderId, existing.Amount, wallet?.Balance ?? 0), command));
            return new HandleResult("cancel refused: the debit already happened → PaymentAlreadyCompleted", ChargeWalletConsumer.Snapshot(wallet!, existing));
        }

        if (existing is null)
        {
            existing = new LedgerEntry
            {
                OrderId = command.OrderId,
                CustomerId = "",
                Amount = 0,
                Direction = LedgerDirection.Debit,
                OperationType = LedgerEntry.DebitOperation,
                State = LedgerState.Cancelled,
                Reason = command.Reason,
                CreatedAtUtc = DateTime.UtcNow,
            };
            Db.Ledger.Add(existing);
        }

        Db.PublishViaOutbox(ChargeWalletConsumer.Stamp(new PaymentCancelled(command.OrderId, command.Reason), command));
        return new HandleResult($"payment cancelled ({existing.State} row occupies the debit slot) → PaymentCancelled",
            new { ledger = new { existing.OrderId, State = existing.State.ToString(), existing.Reason } });
    }
}
