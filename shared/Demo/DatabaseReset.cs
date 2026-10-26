using Microsoft.EntityFrameworkCore;

namespace MiniShop.Shared.Demo;

/// <summary>
/// Empties every table of a service database (Development "reset demo data").
///
/// We deliberately do NOT delete the SQLite file: the service is running, its connection pool and
/// background workers (outbox dispatcher, saga timeouts) still hold connections, and a deleted file
/// would keep receiving writes through those open handles.  Deleting the rows inside one transaction
/// is safe while the service runs: other writers simply wait for the SQLite write lock.
/// </summary>
public static class DatabaseReset
{
    public static async Task ClearAllTablesAsync(DbContext db, CancellationToken ct = default)
    {
        var tables = db.Model.GetEntityTypes()
            .Select(t => t.GetTableName())
            .Where(name => name is not null)
            .Distinct()
            .ToList();

        await db.Database.OpenConnectionAsync(ct);
        try
        {
            // Foreign keys are switched off for the duration so the delete order does not matter
            // (the pragma only works outside a transaction).
            await db.Database.ExecuteSqlRawAsync("PRAGMA foreign_keys = OFF;", ct);
            await using (var transaction = await db.Database.BeginTransactionAsync(ct))
            {
                foreach (var table in tables)
                    await db.Database.ExecuteSqlRawAsync($"DELETE FROM \"{table}\";", ct);
                await transaction.CommitAsync(ct);
            }
            await db.Database.ExecuteSqlRawAsync("PRAGMA foreign_keys = ON;", ct);
        }
        finally
        {
            await db.Database.CloseConnectionAsync();
        }

        db.ChangeTracker.Clear();
    }
}
