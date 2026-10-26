# shared/ — linked source building blocks

These files are **not** a fifth project. Each of the four services links them as source
(see the `<Compile Include="../../shared/**/*.cs" />` line in every `.csproj`), exactly like `contracts/`.
In a production system they would be published as a versioned NuGet package.

Why linked source instead of three copies: the Outbox, Inbox, fault-injection and telemetry code is
identical in every service, and a student should read one well-commented implementation, not three.

| Folder        | What it teaches                                                                   |
|---------------|-----------------------------------------------------------------------------------|
| `Outbox/`     | Transactional Outbox table + background dispatcher with retry/backoff             |
| `Inbox/`      | Inbox table used for message de-duplication                                        |
| `Messaging/`  | `IdempotentConsumer` base class: inbox check + business write + outbox in ONE transaction |
| `Faults/`     | Scoped fault injection (fail / delay / duplicate / pause / reject) + control-plane API |
| `Diagnostics/`| Telemetry events published on a separate channel for the Developer Console        |
| `Demo/`       | Development-only control plane helpers (token check, read-only diagnostic endpoints) |
