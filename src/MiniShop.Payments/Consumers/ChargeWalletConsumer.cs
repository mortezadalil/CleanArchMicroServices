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
public sealed class ChargeWalletConsumer(PaymentsDbContext db, DiagnosticsPublisher diagnostics, OutboxSignal signal,
    FaultRegistry faults, ILogger<ChargeWalletConsumer> logger)
    : IdempotentConsumer<ChargeWallet, PaymentsDbContext>(db, diagnostics, signal, faults, logger)
{
    /// <summary>Fault point: Fail (crash → retry), Delay (slow / late payment), Pause (stopped consumer), Reject (decline).</summary>
    protected override string FaultPoint => FaultPoints.PaymentsCharge;

    protected override async Task<HandleResult> HandleAsync(ConsumeContext<ChargeWallet> context)
    {
        var command = context.Message;
        var ct = context.CancellationToken;

        var wallet = await Db.GetOrCreateWalletAsync(command.CustomerId, ct);
        var existing = await Db.Ledger.FirstOrDefaultAsync(l => l.OrderId == command.OrderId && l.OperationType == LedgerEntry.DebitOperation, ct);
        if (existing is not null)
            return ReAnswer(existing, wallet, command);

        string? refusal = Fault.Reject ? Fault.Reason
            : wallet.Balance < command.Amount ? $"insufficient funds: balance {wallet.Balance:N0} < amount {command.Amount:N0}"
            : null;

        var entry = new LedgerEntry
        {
            OrderId = command.OrderId,
            CustomerId = command.CustomerId,
            Amount = command.Amount,
            Direction = LedgerDirection.Debit,
            OperationType = LedgerEntry.DebitOperation,
            CreatedAtUtc = DateTime.UtcNow,
        };
        Db.Ledger.Add(entry);

        if (refusal is not null)
        {
            entry.State = LedgerState.Failed;
            entry.Reason = refusal;
            Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, refusal, wallet.Balance), command));
            return new HandleResult($"payment refused: {refusal}", Snapshot(wallet, entry));
        }

        wallet.Balance -= command.Amount;
        wallet.Version++;
        entry.State = LedgerState.Completed;
        Db.PublishViaOutbox(Stamp(new PaymentCompleted(command.OrderId, command.CustomerId, command.Amount, wallet.Balance), command));
        Db.PublishViaOutbox(WalletSeed.BalanceChanged(wallet, command));
        return new HandleResult($"debited {command.Amount:N0}; balance now {wallet.Balance:N0}", Snapshot(wallet, entry));
    }

    private HandleResult ReAnswer(LedgerEntry existing, Wallet wallet, ChargeWallet command)
    {
        switch (existing.State)
        {
            case LedgerState.Completed:
                Db.PublishViaOutbox(Stamp(new PaymentCompleted(command.OrderId, command.CustomerId, existing.Amount, wallet.Balance), command));
                return new HandleResult("order already debited; PaymentCompleted re-published, no second debit", Snapshot(wallet, existing));
            case LedgerState.Cancelled:
                Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, "payment window was cancelled (tombstone)", wallet.Balance), command));
                return new HandleResult("late ChargeWallet refused: payment was cancelled (tombstone)", Snapshot(wallet, existing));
            default:
                var reason = existing.Reason ?? "payment previously failed";
                Db.PublishViaOutbox(Stamp(new PaymentFailed(command.OrderId, command.CustomerId, command.Amount, reason, wallet.Balance), command));
                return new HandleResult($"earlier answer repeated: failed ({reason})", Snapshot(wallet, existing));
        }
    }

    internal static object Snapshot(Wallet wallet, LedgerEntry? entry) => new
    {
        wallet = new { wallet.CustomerId, wallet.Balance },
        ledger = entry is null ? null : new { entry.OrderId, entry.Amount, State = entry.State.ToString(), entry.OperationType },
    };

    internal static T Stamp<T>(T @event, Message cause) where T : Message => @event with
    {
        CorrelationId = cause.CorrelationId,
        CausationId = cause.MessageId,
        ScenarioId = cause.ScenarioId,
        Producer = ServiceNames.Payments,
    };
}
