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

## Suggested reading order

Every file starts with a header (what it is, where it sits, which pattern, "Read next").
If you read them in this order, each one only uses ideas from the files before it:

1. `../contracts/Message.cs` → `Commands.cs` → `Events.cs` → `Topology.cs` → `Diagnostics.cs` (the shared language)
2. `Outbox/OutboxMessage.cs` (the dual-write problem and its fix) → `Outbox/OutboxSignal.cs` → `Outbox/OutboxDispatcher.cs` (at-least-once delivery, the "outbox gap")
3. `Inbox/InboxMessage.cs` → `Messaging/IdempotentConsumer.cs` (inbox check + business change + outbox in ONE transaction)
4. `Messaging/MessagingOptions.cs` (retries and the error queue) → `Messaging/ServiceHostingExtensions.cs` (how it is all wired)
5. `Faults/FaultRule.cs` → `Faults/FaultRegistry.cs` → `Faults/FaultContext.cs` → `Faults/FaultingSaveChangesInterceptor.cs` (Scenario Lab fault injection, scoped by ScenarioId)
6. `Diagnostics/DiagnosticsPublisher.cs` (best-effort telemetry, and why it never goes through the outbox)
7. `Demo/*` (control plane, health probes, demo reset)
