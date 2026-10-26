# Seeds

The demo data is deterministic and lives in code so that it is versioned with the services:

| Service   | File                                   | Values                                                        |
|-----------|----------------------------------------|---------------------------------------------------------------|
| Inventory | `src/MiniShop.Inventory/Data/CatalogSeed.cs` | Headphones 500,000 · Keyboard 800,000 · Mouse 300,000 (Toman), 10 units each, fixed product ids `1111…`, `2222…`, `3333…` |
| Payments  | `src/MiniShop.Payments/Data/WalletSeed.cs`   | customer `demo-customer` with 2,000,000                        |

Reset at any time (Development only): the **⟲ Reset data** button in the header (also in the Developer
Console), or `POST /demo/reset-all` on the Web (header `X-Demo-Token: minishop-dev`). A full reset:

1. removes every fault rule,
2. purges every MiniShop queue in RabbitMQ (including `*_error`),
3. empties `readmodel.db` (orders, timeline, scenario runs),
4. empties the tables of `orders.db`, `inventory.db`, `payments.db` and restores the seed above,
5. waits until the store shows the seed again.

The SQLite files are not deleted (the services keep running and hold open connections); their rows are
removed in one transaction instead. `POST /demo/reset` on a single service resets only that service.
Scenarios create their own customers (`scn-s03-…`) so they never touch the storefront wallet.
