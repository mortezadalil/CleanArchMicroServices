// ==========================================================================================
// FILE: contracts/Message.cs
//
// WHAT THIS FILE IS
//   The base type of every message that travels between MiniShop services, plus the small
//   OrderLine value that several messages carry.
//
// WHERE IT SITS IN THE ARCHITECTURE
//   contracts/ is the "shared language" of the system.  It is NOT a project of its own: each of
//   the four services (Orders, Inventory, Payments, Web) compiles these .cs files as LINKED SOURCE
//   (see `<Compile Include="../../contracts/**/*.cs" />` in every .csproj).  That way all services
//   agree on exactly the same message shapes without a fifth assembly.  In a real company this
//   folder would be published as a versioned NuGet package.
//
// WHICH PATTERN IT IMPLEMENTS
//   "Message envelope": every message carries the same metadata (ids, timestamps, producer) so
//   that generic infrastructure (Inbox de-duplication, Outbox, tracing, fault injection, the
//   Developer Console) can work on ANY message without knowing its business meaning.
//
// Read next: contracts/Commands.cs and contracts/Events.cs (the concrete messages), then
//            contracts/Topology.cs (where they are sent).
// ==========================================================================================

// A file-scoped namespace (C# 10): everything in this file belongs to MiniShop.Contracts
// without an extra level of braces/indentation.
namespace MiniShop.Contracts;

/// <summary>
/// The common envelope carried by EVERY message in MiniShop (commands, events and diagnostics).
///
/// Why these fields exist:
///  - MessageId      : unique per message; consumers use it for de-duplication (Inbox).
///  - CorrelationId  : identifies the business flow. In MiniShop it is always the OrderId,
///                     so every message of one order can be traced end-to-end.
///  - CausationId    : the MessageId of the message that caused this one (parent in the chain).
///  - ScenarioId     : set only when the order was started from the Scenario Lab, so that
///                     fault injection can be scoped to a single experiment.
///  - OccurredAtUtc  : when the producer created the message (not when it was delivered).
///  - SchemaVersion  : lets consumers evolve contracts safely.
///  - Producer       : the service that created the message (Orders / Inventory / Payments / Web).
///
/// Messages are immutable C# records: once created they are never mutated.
///
/// C# NOTE — records:
///   A <c>record</c> is a class whose equality is based on its VALUES (two records with the same
///   property values are Equal), and which gets a compiler-generated ToString and a <c>with</c>
///   expression.  <c>var copy = msg with { ScenarioId = id };</c> creates a NEW record that is a copy
///   of <c>msg</c> except for the listed properties: the original is never modified.  That is exactly
///   what we want for messages, which are facts/instructions that must not change after being sent.
///
/// C# NOTE — abstract:
///   The base record is <c>abstract</c> so nobody can send a "bare" Message; you always send a
///   concrete type such as OrderPlaced, which is what RabbitMQ routes on.
/// </summary>
public abstract record Message
{
    /// <summary>
    /// Unique id of THIS message instance.  Generated once by the producer and never changed,
    /// even when the Outbox dispatcher re-publishes the same row after a crash (S11) or when a
    /// duplicate is injected (S06).  Consumers store it in their Inbox table: seeing the same
    /// MessageId twice means "already processed, ignore" (idempotency under at-least-once delivery).
    /// </summary>
    /// <remarks>
    /// C# NOTE — <c>init</c>: an init-only setter can be assigned in an object initializer
    /// (<c>new X { MessageId = ... }</c>) or a <c>with</c> expression, but never afterwards.
    /// The <c>= Guid.NewGuid()</c> part is a default value used when the creator does not set it.
    /// </remarks>
    public Guid MessageId { get; init; } = Guid.NewGuid();

    /// <summary>
    /// The business flow this message belongs to.  In MiniShop it is always the OrderId, so the
    /// Developer Console can filter "everything about order X" across all four services, and the
    /// Saga can find its instance for an incoming event.
    /// </summary>
    public Guid CorrelationId { get; init; }

    /// <summary>
    /// MessageId of the message that caused this one (for example the OrderPlaced that caused a
    /// ReserveStock).  Null for the first message of a flow.  Lets the UI draw the cause/effect chain.
    /// </summary>
    public Guid? CausationId { get; init; }

    /// <summary>
    /// Set only for orders started from the Scenario Lab.  Fault rules created by a scenario carry the
    /// same ScenarioId, so an injected failure ("fail the next payment") hits ONLY the experiment's
    /// own messages and never a normal shopper's order running at the same time.  Null = normal order.
    /// </summary>
    public Guid? ScenarioId { get; init; }

    /// <summary>
    /// When the producer created the message (UTC), not when it was delivered.  Delivery can be much
    /// later (broker outage S04, paused consumer S05), and comparing the two shows that delay.
    /// </summary>
    public DateTime OccurredAtUtc { get; init; } = DateTime.UtcNow;

    /// <summary>
    /// Version of the contract shape.  If a message ever gains/changes fields, a consumer can check this
    /// number to handle old and new producers side by side during a rolling deployment.
    /// </summary>
    public int SchemaVersion { get; init; } = 1;

    /// <summary>Which service created the message (one of <see cref="ServiceNames"/>). Used in logs and the UI graph.</summary>
    public string Producer { get; init; } = "";

    /// <summary>Short type name, e.g. "OrderPlaced". Used in logs, the outbox table and the UI.</summary>
    /// <remarks>
    /// This is a computed (get-only, expression-bodied) property: it has no backing field and is
    /// evaluated every time.  <c>GetType()</c> returns the RUNTIME type, so on an OrderPlaced instance it
    /// returns "OrderPlaced" even though the property is declared here in the base record.
    /// The Outbox stores this name, and <c>ContractTypes.Resolve</c> maps it back to the CLR type.
    /// </remarks>
    public string Type => GetType().Name;
}

/// <summary>One line of an order: product + quantity + the price the customer saw at checkout.</summary>
/// <remarks>
/// C# NOTE — positional record: <c>record OrderLine(Guid ProductId, ...)</c> declares a constructor
/// AND an init-only public property for each parameter in one line.
/// <c>sealed</c> means nobody can inherit from it (value objects should not be extended).
/// The UnitPrice is a SNAPSHOT of the price at checkout: Inventory re-validates it, so a price change
/// between "add to cart" and "reserve" is detected instead of silently charging a different amount.
/// Money is a <c>long</c> (smallest currency unit, e.g. cents) to avoid floating-point rounding errors.
/// </remarks>
/// <param name="ProductId">Id of the product in the Inventory catalog.</param>
/// <param name="ProductName">Name copied at checkout, so messages and the UI are readable without a lookup.</param>
/// <param name="UnitPrice">Price per unit (in cents) that the customer saw and agreed to.</param>
/// <param name="Quantity">How many units are ordered.</param>
public sealed record OrderLine(Guid ProductId, string ProductName, long UnitPrice, int Quantity)
{
    /// <summary>Price × quantity for this line (computed, never stored, so it can never be inconsistent).</summary>
    public long LineTotal => UnitPrice * Quantity;
}
