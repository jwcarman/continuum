# Continuum v1 — Implementation Design

Date: 2026-08-23
Status: Approved

This design covers the implementation decisions for building Continuum v1 from the
functional specification in [2026-08-23-continuum-specification.md](2026-08-23-continuum-specification.md).
The specification is authoritative for semantics and invariants (I1–I11); this
document records the decisions the specification left open.

## 1. Project structure & build

Maven multi-module build, groupId `org.jwcarman.continuum`, initial version
`0.1.0-SNAPSHOT`, GitHub at `jwcarman/continuum`.

```
continuum-parent          (pom root)
├── continuum-bom         (dependency BOM for consumers)
├── continuum-core        (API + SPI + pumps + typed kind clients; deps: slf4j-api, codec-core)
├── continuum-memory      (in-memory ContinuumRepository, for tests/embedded use)
├── continuum-jdbc        (PostgreSQL implementation, plain JDBC via DataSource)
└── continuum-testing     (TCK: reusable abstract test suite both providers run against)
```

Build conventions mirror the `substrate` project **except** there is no
`spring-boot-starter-parent`: `continuum-parent` is standalone and manages its own
`dependencyManagement` (JUnit BOM, AssertJ, Mockito, Testcontainers BOM, slf4j).

- Java 25, UTF-8.
- Apache License 2.0; mycila `license-maven-plugin` `license` profile with the
  standard inline header (`Copyright © ${year} James Carman`).
- Spotless with google-java-format, checked at `validate`.
- `release` profile: central-publishing-maven-plugin (Sonatype, tokenAuth,
  autoPublish), source jar, javadoc jar, GPG signing (loopback pinentry).
- `ci` profile: JaCoCo prepare-agent + XML report via `jacocoArgLine`.
- SonarCloud: organization `jwcarman`, projectKey `jwcarman_continuum`.
- GitHub workflows `maven.yml` (CI) and `maven-publish.yml` (release trigger),
  adapted from substrate.
- Repo files: LICENSE, README.md, CHANGELOG.md, .gitignore, Maven wrapper (mvnw).

Test style (per global Java rules): snake_case test method names with
`@DisplayNameGeneration(ReplaceUnderscores.class)`, `@Nested` classes grouping one
axis of behavior each, no star imports, no FQNs in code, no warning suppression.

## 2. Core API (`continuum-core`)

The public API follows the specification's suggested types verbatim:

- `ComputationId(UUID)`, `ComputationKind(String)`, `ContinuationId(UUID)`,
  `InvocationId(String)` — value records.
- `ComputationStatus` — `PENDING`, `COMPLETED`, `FAILED`.
- `Outcome` — sealed: `Success(byte[] payload)` | `Failure(FailureInfo)`.
- `FailureInfo(FailureKind kind, String message)`; `FailureKind` —
  `EXECUTION_FAILED`, `TIMEOUT_NON_RETRYABLE`, `TIMEOUT_RETRY_EXHAUSTED`,
  `INFRASTRUCTURE_FAILURE`.
- `RetrySemantics` — `RETRYABLE`, `NON_RETRYABLE`.
- `Computation` — id, kind, status, createdAt, deadline, outcome (absent while
  pending), plus retrySemantics, invocationId, dispatchPayload, metadata,
  attemptCount.
- `ComputationRequest(kind, continuationPayload, deadline, retrySemantics,
  invocationId, dispatchPayload, metadata)`.
- `RegistrationResult` — sealed: `Registered(ContinuationId)` | `Resolved(Outcome)`.
- `CompletionResult` — `COMPLETED`, `ALREADY_RESOLVED`, `NOT_FOUND`.
- `CompletionDelivery(computationId, kind, continuationId, continuationPayload,
  outcome)` — the self-contained delivery unit handed to consumers.

```java
public interface Continuum {
    Computation create(ComputationRequest request);
    RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);
    CompletionResult complete(ComputationId id, Outcome outcome);
    Optional<Computation> find(ComputationId id);
}
```

Decisions the specification left open:

1. **ContinuationId assignment** — callers supply only the opaque `byte[]`
   continuation payload; Continuum generates the `ContinuationId` and returns it.
   Library-assigned IDs keep the deduplication key trustworthy.
2. **ID generation** — random (v4) UUIDs for `ComputationId` and `ContinuationId`.
3. **Time** — an injected `java.time.InstantSource` (every `Clock` is an
   `InstantSource`; Continuum never needs a `ZoneId`). Tests use fixed/steppable
   sources. The `Continuum` interface exposes it as `instants()`: there is one
   time authority per `Continuum` instance, and the typed layer (client
   creation defaults, the router's retry adapter) derives all
   `now + timeout` arithmetic from it rather than configuring a second clock —
   deadlines are computed and compared against the same source.
4. **Storage and coordination are opaque `byte[]`** end to end — the outcome
   payload, the continuation payload, and the dispatch payload alike. The
   `Continuum` interface is exactly the specification's byte[] contract (§32):
   no codec accessor, no type parameters, nothing serialization-aware. Generics
   are deliberately rejected on `Continuum` itself: one instance coordinates
   many kinds with different result/continuation/dispatch types, and the
   delivery pump drains a mixed-kind outbox, so no single type assignment is
   honest. Strong typing is provided by `ContinuumClient<R, C, D>`
   (section 2a), where generics bind per kind.
5. **Dispatch payload (retry breadcrumb)** — `ComputationRequest` accepts an
   optional opaque `byte[] dispatchPayload` meaning "how to (re)dispatch this
   work," mirroring the continuation payload's "what to do with the result." It
   is persisted atomically with the computation at create (riding invariant I2's
   transaction), never mutated, never interpreted by Continuum, and handed back
   to the `RetryHandler` via `ExpiredComputation`. Apps with their own durable
   dispatch state may leave it null and use `invocationId` as a foreign key.
   It is a write-once breadcrumb, not mutable workflow state (non-goal §3).

## 2a. `ContinuumClient<R, C, D>` — the typed primary API (`continuum-core`)

The layering mirrors how Nessy rides Substrate: the byte[]-based thing is the
underlying coordination/storage contract (`Continuum` + the SPI), and the
typed API callers actually program against is named for the library.
Serialization is pluggable via `org.jwcarman.codec` (`codec-core`:
`Codec<T>` / `CodecFactory` / `TypeRef` — three files, zero transitive
dependencies, so `continuum-core` depends on it directly alongside slf4j-api).

- `ContinuumClient<R, C, D>` — the typed handle bound to one
  `ComputationKind`, minted from the `Continuum` instance via a configurer
  DSL. `Continuum.client(...)` is a `default` method that builds the client
  purely from its arguments, so the interface itself stays the codec-free
  byte[] contract:

  ```java
  var toolCalls = continuum.client(
      "tool-result",
      ToolCallResult.class, ToolCallContinuation.class, ToolCallDescriptor.class,
      cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
                .deadline(Duration.ofMinutes(5))
                .retries(Retry.atMost(3, (toolCall, ctx) -> {
                    toolRuntime.dispatch(toolCall, ctx.invocationId());
                    return RetryResult.retried();
                })));

  var computation = toolCalls.create(continuation, descriptor, invocationId);
  toolCalls.complete(computation.id(), result);
  ```

  Client config supplies creation defaults — `deadline(Duration)` (per-attempt
  timeout; `create` computes `now + duration`, per-call override available)
  and the kind's `Retry` (below). `codecs(CodecFactory)` resolves the three
  payload codecs, with per-payload `Codec<T>` overrides available. The client
  is built entirely on the public byte[] API: `create(...)` encodes the
  continuation and dispatch payloads; `complete(id, R)` encodes the result;
  registration decodes a `Resolved` outcome.

- `Retry<D>` — the typed retry abstraction: one object that *performs* (or
  schedules) the redispatch itself and reports what it did:

  ```java
  public interface Retry<D> {
      RetryResult onTimeout(D dispatch, RetryContext ctx);

      sealed interface RetryResult {
          record Retried(Duration timeout) implements RetryResult {}  // new deadline = now + timeout
          record RetriedDefault() implements RetryResult {}           // now + the client's configured timeout
          record NotRetried(String reason) implements RetryResult {}

          static RetryResult retried() { ... }
          static RetryResult retried(Duration timeout) { ... }
          static RetryResult notRetried(String reason) { ... }
      }
  }
  ```

  Time vocabulary is deliberately split: **durations at the typed layer,
  instants at the wire/storage layer.** A durable row must hold an absolute
  deadline, so the raw level stays `Instant`
  (`ComputationRequest.deadline`, `Redispatched(Instant)`, `deadline_at`);
  the typed layer only ever speaks timeouts, and the adapter converts
  (`now + timeout`) at the same boundary where codecs convert types to bytes.

  The `RetryResult` values are pure data; the router-built adapter that wraps
  each client's `Retry` into the core `RetryHandler` interprets them with the
  client's config in hand: `Retried(timeout)` → `Redispatched(now + timeout)`;
  `RetriedDefault` → `Redispatched(now + client's configured deadline)`;
  `NotRetried` → `Abandon` → `TIMEOUT_RETRY_EXHAUSTED`. `RetryContext`
  carries `attemptCount`, `invocationId`, kind, metadata, and the expired
  deadline; `D` is the decoded dispatch payload. Declarative policies are
  combinators over this functional core — `Retry.atMost(n, inner)` returns
  `NotRetried("attempts exhausted")` once `ctx.attemptCount() >= n` without
  invoking the inner retry. A client configured with **no** `Retry` creates
  `NON_RETRYABLE` computations — the presence of a `Retry` is what
  `RETRYABLE` means at the typed layer; the `RetrySemantics` enum survives
  only at the wire/storage level.

- `DeliveryRouter` — the pump cannot be generic (it drains a mixed-kind
  outbox), so a router dispatches each `CompletionDelivery` by kind to a
  typed handler registered against a `ContinuumClient`
  (`DeliveryRouter.builder().on(toolCalls, handler)`), which decodes the
  continuation payload and outcome before application code sees them.
  Unrouted kinds go to an explicit fallback: either a registered raw byte[]
  handler or fail-and-release (the delivery backs off rather than vanishing).
  The reaper's typed routing needs no separate registration: each registered
  client carries its own `Retry`, so the router derives the per-kind
  `RetryHandler` from the same registrations deliveries use.
- Concrete formats come from the codec project's backends: `codec-jackson`
  (Jackson 3), `codec-gson`, `codec-protobuf`. A Jackson 2 backend
  (`codec-jackson2`) is a follow-up in the codec repo — Continuum deliberately
  ships no serialization-format modules of its own; its module list stays
  about persistence providers.
- The raw byte[] `Continuum` API remains public and documented — it is what
  the typed layer is built on, and an escape hatch for polyglot payloads.

## 3. Pump components (no threads)

The library never owns a thread or scheduler ("pumped" model — the owning
application calls `pump()` on whatever cadence/scheduler it likes). No correctness
property depends on a pump running (spec I9); pumps only advance liveness.

### DeliveryPump

```java
int pump(); // returns number successfully delivered
```

- Claims up to `batchSize` outbox items under a lease (`workerId`,
  `leaseDuration` configurable).
- Invokes the application-supplied `DeliveryHandler.handle(CompletionDelivery)`
  for each item.
- Success → acknowledge (delete) the outbox item.
- Handler exception → release the item: increment `attemptCount`, push
  `availableAt` back by a configurable backoff. One item's failure never blocks
  the others (I8).
- At-least-once delivery; consumers deduplicate on `ContinuationId` (spec §23).

### TimeoutReaper

```java
int pump(); // returns number of expired computations processed
```

The `RetryHandler` is a **required constructor argument** of the reaper (no
global registry to forget; "no handler" is a compile error, not a runtime
surprise) and is a **decision-returning** callback — retry policy lives in the
application, not in Continuum. (Applications using the typed layer rarely
implement it directly: the `DeliveryRouter` derives it from each registered
client's `Retry<D>`, section 2a.)

```java
public interface RetryHandler {

    RetryDecision onTimeout(ExpiredComputation computation);

    sealed interface RetryDecision {
        record Redispatched(Instant newDeadline) implements RetryDecision {}
        record Abandon(String message) implements RetryDecision {}
    }
}
```

`ExpiredComputation` carries kind, `attemptCount`, `invocationId`,
`dispatchPayload`, `metadata`, and the expired deadline — everything needed to
decide and to re-dispatch, so the handler is stateless and any JVM's reaper can
run it against a computation created by a long-dead process.

For each pending computation past its deadline (spec §29):

- Already terminal → skip (races with completion are safe: completion is
  first-wins atomic).
- `NON_RETRYABLE` → complete as `Failure(TIMEOUT_NON_RETRYABLE)` through the
  normal completion path (outbox fan-out included). The handler is never
  consulted — "never re-request this work" is a guarantee Continuum enforces
  as data (`RetrySemantics`), not a convention a handler could violate.
- `RETRYABLE` → call `onTimeout`:
  - `Redispatched(newDeadline)` means the handler has re-dispatched the work
    (same `InvocationId`); Continuum extends the deadline and increments
    `attemptCount` atomically.
  - `Abandon(message)` → complete as `Failure(TIMEOUT_RETRY_EXHAUSTED, message)`.
  - Handler exception → no decision: leave the computation untouched and log;
    the next `pump()` retries the handler (bounded by pump cadence — no hot
    loop).

Retry state and policy are deliberately split: **`attemptCount` is the only
retry state Continuum persists** (on the computation record, bumped atomically
with the deadline extension). Attempt limits and backoff are policy computed
inside the handler from durable inputs — e.g.
`attemptCount >= 3 ? Abandon : Redispatched(now + timeout)`, with any backoff
curve expressed through the returned `newDeadline`. Per-computation budgets can
ride in `metadata`. Note that timeout-paced retries are self-throttling: attempt
N+1 cannot occur until attempt N's full timeout has elapsed, so no separate
backoff mechanism is built in.

Duplicate retry requests are possible (at-least-once, e.g. two nodes pumping
concurrently) and are made idempotent by the stable `InvocationId` (spec §31).
If no reaper is ever pumped, no correctness property is lost — expired
computations simply remain pending (liveness only).

## 4. Persistence SPI (`org.jwcarman.continuum.spi`)

Semantic atomic operations, not generic CRUD (spec §35). All coordination logic
lives in core's `Continuum` implementation and the pumps; providers implement only
these primitives with the required atomicity:

```java
public interface ContinuumRepository {
    void createComputation(Computation computation, StoredContinuation initial);      // atomic pair (I2)
    RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation c); // atomic vs completion (I5)
    CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt); // terminalize + outbox fan-out atomically (I7)
    Optional<Computation> findComputation(ComputationId id);
    List<ClaimedDelivery> claimDeliveries(String workerId, int limit, Duration lease, Instant now);
    void acknowledgeDelivery(DeliveryId id);
    void releaseDelivery(DeliveryId id, Instant retryAt);   // increments attempt count
    List<Computation> findExpired(Instant now, int limit);
    void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);
}
```

Supporting SPI types: `StoredContinuation(ContinuationId, byte[] payload)`,
`ClaimedDelivery(DeliveryId, CompletionDelivery)`, `DeliveryId(UUID)`,
`RegistrationOutcome` (registered | resolved(outcome) | not found),
`CompletionOutcome` (completed | already resolved | not found).

## 5. `continuum-memory`

In-JVM `ContinuumRepository` backed by maps with per-computation locking, giving
real atomicity for the registration-vs-completion and complete-vs-complete races.
Outbox claiming honors leases and `availableAt` using the injected clock. A
faithful implementation intended for tests and embedded/single-process use — not
a mock.

## 6. `continuum-jdbc`

PostgreSQL over plain JDBC (`javax.sql.DataSource`; no Spring):

- The specification's three tables — `continuum_computation`,
  `continuum_continuation`, `continuum_outbox` — with DDL shipped as a classpath
  resource (`continuum-postgresql.sql`). No migration tooling; applications own
  schema management. The computation table extends the specification's sketch
  with the retry columns: `retry_semantics`, `invocation_id`,
  `dispatch_payload`, `attempt_count`, and `metadata`.
- The provider manages its own transactions (`autoCommit=false`,
  commit/rollback per SPI operation).
- Completion and registration lock the computation row with
  `SELECT ... FOR UPDATE`.
- Outbox claiming uses `FOR UPDATE SKIP LOCKED` so competing consumers never
  block one another.

## 7. `continuum-testing` (TCK)

Abstract JUnit 5 test classes exercising every required concurrency test from
spec §38 against any `Continuum`/`ContinuumRepository`:

- create-crash atomicity,
- register vs complete races (many iterations; every registration yields exactly
  one of Registered/Resolved; Registered implies an eventual delivery),
- complete vs complete (exactly one winner; stored outcome matches the winner),
- completion transaction failure (no partial outbox state),
- multiple concurrent continuations (each Registered has exactly one delivery
  obligation),
- competing consumers (single active lease),
- consumer crash / lease expiry reclaim,
- late registration (outcome returned, nothing persisted),
- timeout vs completion (exactly one terminal outcome).

`continuum-memory` runs the TCK as surefire unit tests; `continuum-jdbc` runs it
as failsafe integration tests against PostgreSQL in Testcontainers.
Provider-specific edge cases (SQL details, injected transaction failures) get
additional tests in their own modules.

## 8. Development approach

TDD throughout, building in dependency order:

1. core (API types, SPI, `Continuum` implementation, pumps),
2. memory provider + TCK (validating both together),
3. jdbc provider against the TCK,
4. bom, docs, CI workflows, publishing setup.
