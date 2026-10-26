# Seeds

The demo data is deterministic and lives in code so that it is versioned with the services:

| Service   | File                                   | Values                                                        |
|-----------|----------------------------------------|---------------------------------------------------------------|
| Inventory | `src/MiniShop.Inventory/Data/CatalogSeed.cs` | Headphones 500,000 · Keyboard 800,000 · Mouse 300,000 (Toman), 10 units each, fixed product ids `1111…`, `2222…`, `3333…` |
| Payments  | `src/MiniShop.Payments/Data/WalletSeed.cs`   | customer `demo-customer` with 2,000,000                        |

Reset at any time (Development only): **Developer Console → Reset demo data**, or
`POST /demo/reset` on each service, or `POST /demo/reset-all` on the Web (header `X-Demo-Token: minishop-dev`).
Scenarios create their own customers (`scn-s03-…`) so they never touch the storefront wallet.
