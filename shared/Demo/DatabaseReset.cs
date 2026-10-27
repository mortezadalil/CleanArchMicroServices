// ==========================================================================================
// FILE: shared/Demo/DatabaseReset.cs
//
// WHAT THIS FILE IS
//   A helper that deletes every row of every table of a service's SQLite database, while the
//   service keeps running.  Used by the "reset demo data" button.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   shared/ (linked source), Development-only tooling.  Each service exposes a reset endpoint on its
//   protected demo group that calls ClearAllTablesAsync and then re-seeds its data; the Web
//   orchestrates the whole reset (src/MiniShop.Web/Services/DemoResetService.cs) and finally asks for
//   fresh snapshots with ReadModelSnapshotRequested.
//
// WHY IT LOOKS LIKE THIS
//   Each microservice owns its own database ("database per service"), so a reset has to happen inside
//   each service — the Web is not allowed to touch other services' tables directly.
//
// Read next: src/MiniShop.Web/Services/DemoResetService.cs.
// ==========================================================================================

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
    /// <summary>Deletes all rows of all tables mapped by <paramref name="db"/>, in one transaction.</summary>
    /// <param name="db">The service's DbContext (any type: the table list is read from its EF model).</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task ClearAllTablesAsync(DbContext db, CancellationToken ct = default)
    {
        // Ask EF's model which tables this context maps (business tables, Outbox, Inbox, saga state...),
        // so the helper never needs a hand-maintained list.  Distinct because several entity types can share
        // one table; the null filter skips entity types not mapped to a table (e.g. views/keyless types).
        var tables = db.Model.GetEntityTypes()
            .Select(t => t.GetTableName())
            .Where(name => name is not null)
            .Distinct()
            .ToList();

        // Open the connection explicitly and keep it open: a PRAGMA only applies to the connection it runs on,
        // so the PRAGMA, the DELETEs and the second PRAGMA must all use the SAME connection.
        await db.Database.OpenConnectionAsync(ct);
        try
        {
            // Foreign keys are switched off for the duration so the delete order does not matter
            // (the pragma only works outside a transaction).
            await db.Database.ExecuteSqlRawAsync("PRAGMA foreign_keys = OFF;", ct);
            // One transaction: either every table is emptied, or (on error) none is — no half-reset database.
            await using (var transaction = await db.Database.BeginTransactionAsync(ct))
            {
                // Table names come from our own EF model (not from user input), so building raw SQL here is safe.
                // Double quotes quote the identifier in SQLite.
                foreach (var table in tables)
                    await db.Database.ExecuteSqlRawAsync($"DELETE FROM \"{table}\";", ct);
                await transaction.CommitAsync(ct);
            }
            // Restore foreign-key enforcement for the rest of this connection's life (it returns to the pool).
            await db.Database.ExecuteSqlRawAsync("PRAGMA foreign_keys = ON;", ct);
        }
        finally
        {
            // Always hand the connection back, even if a DELETE failed.
            await db.Database.CloseConnectionAsync();
        }

        // Forget any entities this DbContext was still tracking: they no longer exist in the database, and a later
        // SaveChanges on this context must not try to update them.
        db.ChangeTracker.Clear();
    }
}
