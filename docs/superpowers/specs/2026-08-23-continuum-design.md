# Continuum v1 — Implementation Design

Date: 2026-08-23
Status: Approved

This design covers the implementation decisions for building Continuum v1 from the
functional specification in [2026-08-23-continuum-specification.md](2026-08-23-continuum-specification.md).
The specification is authoritative for semantics and invariants (I1–I11); this
document records the decisions the specification left open, and the deliberate
deviations from its sketches (each called out inline with its rationale).

## 1. Project structure & build

Maven multi-module build, groupId `org.jwcarman.continuum`, initial version
`0.1.0-SNAPSHOT`, GitHub at `jwcarman/continuum`.

```
continuum-parent          (pom root)
├── continuum-bom         (dependency BOM for consumers)
├── continuum-core        (API + SPI + pumps + typed client; deps: slf4j-api, codec-core)
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

## 2. Data model — presence means pending

The persistence model follows Nessy's Substrate recipes: **a computation record
exists only while pending**. Completion is an atomic ownership transfer — the
pending row is deleted, the memoized outcome is written to a result record, and
the outbox deliveries are materialized, all in one transaction.

```text
continuum_computation   (pending only — presence means pending)
    id, kind, deadline_at, dispatch_payload NULL, result_ttl NULL,
    attempt_count, created_at, last_updated_at

continuum_continuation  (id, computation_id, payload, created_at)

continuum_result        (computation_id, outcome…, completed_at, expires_at NULL)

continuum_outbox        (id, computation_id, continuation_id, kind,
                         continuation_payload, outcome…, available_at,
                         claimed_by, claimed_until, attempt_count, created_at)
```

Consequences:

- **Hot paths stay hot.** The pending table's size is the in-flight count;
  expiry scans and registration locks never wade through terminal rows.
- **`ComputationStatus` is derived, not stored.** Pending table → `PENDING`;
  result table → `COMPLETED`/`FAILED`; neither → not found.
- **`continuum_result` is the memoization requirement (spec §19) made
  concrete.** Late registration reads it to return `Resolved(outcome)` (I5);
  duplicate `complete()` reads it to answer `ALREADY_RESOLVED`. Without it,
  presence-means-pending would break both.
- **Retryability ≡ dispatch payload presence.** There is no `RetrySemantics`
  enum and no `retry_semantics` column: a computation with a `dispatch_payload`
  is retryable; one without is not (nothing to redispatch, non-retryable by
  construction). The reaper's "never re-request non-retryable work" guarantee
  (spec §27) is enforced by this NULLness — payload absent → fail with
  `TIMEOUT_NON_RETRYABLE`, handler never consulted.
- **`attempt_count` starts at 1** — the original dispatch is attempt 1, so
  `atMost(3)` reads literally as "at most 3 total attempts." It is the only
  retry state Continuum persists.
- **`result_ttl` rides the pending computation; completion resolves it.**
  Retention is declared at creation (`resultTtl(Duration)`, nullable = retain
  forever); the completion transaction computes
  `expires_at = completed_at + result_ttl` on the result row. Purge is then
  policy-free: delete result rows whose `expires_at` has passed. After purge,
  a duplicate `complete()` sees `NOT_FOUND` and a late registration throws —
  the retention-dependent ambiguity spec §17 anticipated.
- **Continuations live uniformly in their own table** (including the initial
  one). Multi-row inserts in one transaction are exactly as atomic as one row
  in every store v1 targets, and a single home avoids unioning two places on
  every read. Creation inserts computation + initial continuation atomically
  (I2); completion deletes the continuation rows as it folds them into outbox
  deliveries.

### Deliberate deviations from the specification's sketches

- **No `InvocationId`.** The spec's §28 idempotency key is real, but its home
  is the dispatch payload: the payload is written once at create and handed
  back verbatim on every retry, so a key embedded in it *cannot* drift between
  attempts — invariant I10 enforced structurally rather than by convention.
  Apps that keep dispatch state externally make their payload a small record
  wrapping their own key.
- **No `metadata` map.** It had no consumer in v1; the continuation and
  dispatch payloads already carry any application data.
- **No `RetrySemantics` enum / `version` column / stored status.** Replaced by
  dispatch-payload presence, last_updated_at + row locking, and table
  residency respectively.

## 3. Core API (`continuum-core`)

- `ComputationId(UUID)`, `ComputationKind(String)`, `ContinuationId(UUID)` —
  value records; IDs are random (v4) UUIDs generated by the library. Callers
  supply only payload bytes; Continuum assigns `ContinuationId` (a
  library-assigned dedup key is trustworthy).
- `ComputationStatus` — `PENDING`, `COMPLETED`, `FAILED` (derived; see §2).
- `Outcome` — sealed: `Success(byte[] payload)` | `Failure(FailureInfo)`.
- `FailureInfo(FailureKind kind, String message)`; `FailureKind` —
  `EXECUTION_FAILED`, `TIMEOUT_NON_RETRYABLE`, `TIMEOUT_RETRY_EXHAUSTED`,
  `INFRASTRUCTURE_FAILURE`.
- `Computation` — id, kind, status, createdAt, deadline, dispatchPayload
  (nullable), resultTtl (nullable), attemptCount, outcome (null while pending).
- `ComputationRequest(kind, continuationPayload, deadline, dispatchPayload,
  resultTtl)` — dispatchPayload and resultTtl nullable.
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
    InstantSource instants();
    // plus the default client(...) minting methods — section 4
}
```

Registration against an unknown (or purged) id throws
`ComputationNotFoundException`.

Cross-cutting decisions:

1. **Time** — an injected `java.time.InstantSource` (every `Clock` is an
   `InstantSource`; Continuum never needs a `ZoneId`). Tests use
   fixed/steppable sources. The interface exposes it as `instants()`: one time
   authority per `Continuum` instance; the typed layer derives all
   `now + timeout` arithmetic from it rather than configuring a second clock.
2. **Storage and coordination are opaque `byte[]`** end to end — outcome,
   continuation, and dispatch payloads alike. The `Continuum` interface is the
   spec's byte[] contract (§32): no codec accessor, no type parameters.
   Generics are deliberately rejected on `Continuum` itself: one instance
   coordinates many kinds with different types, and the delivery pump drains a
   mixed-kind outbox, so no single type assignment is honest. Strong typing is
   `ContinuumClient<R, C, D>` (section 4), where generics bind per kind.
3. **Dispatch payload (retry breadcrumb)** — optional opaque `byte[]` meaning
   "how to (re)dispatch this work," mirroring the continuation payload's "what
   to do with the result." Persisted atomically with the computation at create
   (riding I2's transaction), never mutated, never interpreted by Continuum,
   handed back to the retry handler verbatim on every timeout. Its presence
   defines retryability (§2). Embed your external idempotency key in it. It is
   a write-once breadcrumb, not mutable workflow state (spec non-goal §3).

## 4. `ContinuumClient<R, C, D>` — the typed primary API (`continuum-core`)

The layering mirrors how Nessy rides Substrate: the byte[]-based thing is the
underlying coordination/storage contract (`Continuum` + the SPI), and the typed
API callers actually program against is named for the library. Serialization is
pluggable via `org.jwcarman.codec` (`codec-core`: `Codec<T>` / `CodecFactory` /
`TypeRef` — three files, zero transitive dependencies, so `continuum-core`
depends on it directly alongside slf4j-api).

**Construction idiom (house style, as in Nessy):** every configurable component
is built through a named `XxxCustomizer` functional interface — never a bare
`Consumer` — whose lambda receives an `XxxConfig`. The config is an
**interface** exposing only fluent configuration-parameter methods that return
the config itself — no `build()` in the contract. The concrete implementation
behind it is the builder, but the receiver cannot know that: the factory method
constructs it, applies the customizer, and performs the build step privately.
Applies to `Continuum.client(...)`, `Retry.of(...)`, `DeliveryRouter.of(...)`,
`DeliveryPump.of(...)`, and `TimeoutReaper.of(...)`.

- `ContinuumClient<R, C, D>` — the typed handle bound to one
  `ComputationKind`, minted from the `Continuum` instance via the customizer
  DSL (`Continuum.client(...)` is a `default` method that builds the client
  purely from its arguments, so the interface stays the codec-free byte[]
  contract):

  ```java
  var toolCalls = continuum.client(
      "tool-result",
      ToolCallResult.class, ToolCallContinuation.class, ToolCallDescriptor.class,
      cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
                .deadline(Duration.ofMinutes(5))
                .resultTtl(Duration.ofHours(1))
                .retries(Retry.of(r -> r
                    .atMost(3)
                    .handler((toolCall, ctx) ->
                        toolRuntime.dispatch(toolCall, ctx.computationId())))));

  var computation = toolCalls.create(continuation, descriptor);
  toolCalls.complete(computation.id(), result);
  ```

  Client config supplies creation defaults — `deadline(Duration)` (per-attempt
  timeout; `create` computes `now + duration`, per-call override available),
  `resultTtl(Duration)` (memoized-outcome retention; unset = retain forever),
  and the kind's `Retry` (below). `codecs(CodecFactory)` resolves the three
  payload codecs, with per-payload `Codec<T>` overrides available. The client
  is built entirely on the public byte[] API: `create(...)` encodes the
  continuation and dispatch payloads; `complete(id, R)` encodes the result;
  registration decodes a `Resolved` outcome.

  **The three-type client requires `retries(...)`** — declaring a dispatch
  type is declaring retryability. **The two-type overload
  `client(kind, R, C, cfg)` is the non-retryable spelling**: no dispatch type,
  no `retries(...)` permitted, no dispatch payload ever persisted — making
  "dispatch payload without retry" and `Void.class` filler both
  unrepresentable. (This replaces the earlier `Retry.none()` idea: with
  retryability defined by payload presence, a null-object retry has no
  coherent meaning.)

- `Retry<D>` — the typed retry abstraction: one object that *performs* (or
  schedules) the redispatch itself and reports what it did:

  ```java
  public interface Retry<D> {
      RetryResult onTimeout(D dispatch, RetryContext context);

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

  `RetryContext` carries Continuum's durable facts:
  `computationId` (the redispatched worker must know where to `complete()`
  to), `kind`, `attemptCount`, and the expired `deadline`; `D` is the decoded
  dispatch payload — the payload is the application's vocabulary, the context
  is Continuum's facts.

  Time vocabulary is deliberately split: **durations at the typed layer,
  instants at the wire/storage layer.** A durable row must hold an absolute
  deadline, so the raw level stays `Instant`; the typed layer only ever speaks
  timeouts, and the adapter converts (`now + timeout`) at the same boundary
  where codecs convert types to bytes. (`result_ttl` follows the same rule:
  declared as a duration, resolved to an absolute `expires_at` at completion.)

  The `RetryResult` values are pure data; the router-built adapter that wraps
  each client's `Retry` into the core `RetryHandler` interprets them with the
  client's config in hand: `Retried(t)` → `Redispatched(now + t)`;
  `RetriedDefault` → `Redispatched(now + client's configured deadline)`;
  `NotRetried` → `Abandon` → `TIMEOUT_RETRY_EXHAUSTED`.

  The declarative front door is `Retry.of(customizer)` — the house customizer
  idiom one level down. Its config takes `atMost(n)` (n = total attempts,
  matching attempt_count starting at 1), a
  `handler(BiConsumer<D, RetryContext>)` that *only dispatches*, and
  optionally `timeout(Duration)` for retry deadlines differing from first
  attempts. The factory-built `Retry` derives results mechanically: attempts
  exhausted → `NotRetried("attempts exhausted")` without invoking the handler;
  otherwise invoke the consumer and report `RetriedDefault` (or `Retried(t)`
  when `timeout(t)` was configured). A handler exception propagates to the
  reaper's no-decision path (computation untouched, next pump retries).
  Implementing the `Retry<D>` interface directly remains the escape hatch for
  decisions the config cannot express (attempt-dependent backoff, circuit
  breakers) — the same relationship `ContinuumClient` has to raw `Continuum`.

- `DeliveryRouter` — the pump cannot be generic (it drains a mixed-kind
  outbox), so a router dispatches each `CompletionDelivery` by kind to a typed
  handler registered against a `ContinuumClient`
  (`DeliveryRouter.of(r -> r.on(toolCalls, handler))`), which decodes the
  continuation payload and outcome before application code sees them. Unrouted
  kinds go to an explicit fallback: either a registered raw byte[] handler or
  fail-and-release (the delivery backs off rather than vanishing). The
  reaper's typed routing needs no separate registration: each registered
  client carries its own `Retry`, so the router derives the per-kind
  `RetryHandler` from the same registrations deliveries use. An expired
  retryable computation whose kind has no route is left untouched (logged),
  never terminalized by default.
- Concrete formats come from the codec project's backends: `codec-jackson`
  (Jackson 3), `codec-gson`, `codec-protobuf`. A Jackson 2 backend
  (`codec-jackson2`) is a follow-up in the codec repo — Continuum deliberately
  ships no serialization-format modules of its own.
- The raw byte[] `Continuum` API remains public and documented — it is what
  the typed layer is built on, and an escape hatch for polyglot payloads.

## 5. Pump components (no threads)

The library never owns a thread or scheduler ("pumped" model — the owning
application calls `pump()` on whatever cadence/scheduler it likes). No
correctness property depends on a pump running (spec I9); pumps only advance
liveness.

Scheduling is therefore a one-liner in the host's own machinery — e.g.
`scheduler.scheduleWithFixedDelay(deliveryPump::pump, 0, 1, SECONDS)`, Spring
`@Scheduled`, Quartz, or a cron-triggered endpoint. Fixed-*delay* is the
sensible default (a slow batch can't stack overlapping runs on one node),
though overlap is safe regardless: leases and `SKIP LOCKED` make concurrent
pumps correct by design.

### DeliveryPump

```java
int pump(); // processes at most batchSize claimed deliveries, returns count delivered
```

Config knobs (customizer): `batchSize(int)` default 25; `lease(Duration)`
default 30s; `backoff(Duration)` default 30s; `workerId(String)` default
generated (`"worker-" + UUID`); `kinds(ComputationKind...)` default empty =
all kinds. Kind scoping enables per-kind pumps on independent schedules
(`p.kinds(toolCalls.kind())`, pumped every second, beside an approvals pump
pumped every minute) — isolation and cadence per kind, while the unscoped
default keeps one pump draining everything. The router is unaffected: a
scoped pump simply only hands it that kind's deliveries, so one router
instance serves every pump.

- Claims up to `batchSize` outbox items under a lease.
- Invokes the application-supplied `DeliveryHandler.handle(CompletionDelivery)`
  for each item.
- Success → acknowledge (delete) the outbox item.
- Handler exception → release the item: increment its attempt count, push
  `availableAt` back by a configurable backoff. One item's failure never
  blocks the others (I8).
- At-least-once delivery; consumers deduplicate on `ContinuationId` (spec §23).

### TimeoutReaper

```java
int pump(); // processes at most batchSize expired computations, returns the count
```

Each pump handles one bounded batch (`batchSize` configurable, e.g. a dozen)
and never loops internally — the caller owns cadence, and
`while (reaper.pump() > 0)` drains a backlog when wanted. A full-batch return
signals more work probably remains. After an outage this paces redispatch
naturally instead of thundering. Overlapping reapers are safe: overlap yields a
duplicate retry request, covered by at-least-once semantics plus the
idempotency key the app embeds in the dispatch payload.

Config knobs (customizer): `batchSize(int)` default 12 (expired computations
per pump — small, since each may consult a retry handler) and
`kinds(ComputationKind...)` default empty = all kinds (per-kind reapers on
independent schedules, same as the delivery pump).

### ResultPurge

```java
int pump(); // deletes at most batchSize result rows past expires_at, returns the count
```

The third scheduled activity, as its own pump so it gets its own cadence
(purge every ten minutes; reap every fifteen seconds). Policy-free: retention
was resolved onto each result row as `expires_at` at completion, so the purge
is a bounded indexed delete. Config knobs: `batchSize(int)` default 100.
`ResultPurge.of(repo, instants, p -> p.batchSize(200))`.

The `RetryHandler` is a **required constructor argument** of the reaper (no
global registry to forget; "no handler" is a compile error) and is a
**decision-returning** callback — retry policy lives in the application.
(Applications using the typed layer rarely implement it directly: the
`DeliveryRouter` derives it from each registered client's `Retry<D>`, §4.)

```java
public interface RetryHandler {

    RetryDecision onTimeout(Computation computation);

    sealed interface RetryDecision {
        record Redispatched(Instant newDeadline) implements RetryDecision {}
        record Abandon(String message) implements RetryDecision {}
    }
}
```

The expired `Computation` carries everything needed to decide and re-dispatch
(id, kind, attemptCount, dispatchPayload, deadline), so the handler is
stateless and any JVM's reaper can run it against a computation created by a
long-dead process. (The spec's `PendingComputation`/`ExpiredComputation`
sketches are realized as `Computation` — the fields are identical.)

For each pending computation past its deadline (spec §29):

- Already terminal → skip (races with completion are safe: completion is
  first-wins atomic).
- **No dispatch payload** → complete as `Failure(TIMEOUT_NON_RETRYABLE)`
  through the normal completion path (outbox fan-out included). The handler is
  never consulted — never-redispatch is enforced by data (payload NULLness),
  not by convention.
- **Dispatch payload present** → call `onTimeout`:
  - `Redispatched(newDeadline)` means the handler has re-dispatched the work;
    Continuum extends the deadline and increments `attempt_count` atomically.
  - `Abandon(message)` → complete as `Failure(TIMEOUT_RETRY_EXHAUSTED, message)`.
  - Handler exception → no decision: leave the computation untouched and log;
    the next `pump()` retries the handler (bounded by pump cadence — no hot
    loop).

Attempt limits and backoff are policy computed inside the handler from durable
inputs (`attemptCount`, deadline), with any backoff curve expressed through the
returned `newDeadline`. Timeout-paced retries are self-throttling: attempt N+1
cannot occur until attempt N's full timeout has elapsed, so no separate backoff
mechanism is built in. If no reaper is ever pumped, no correctness property is
lost — expired computations simply remain pending (liveness only).

## 6. Persistence SPI (`org.jwcarman.continuum.spi`)

Semantic atomic operations, not generic CRUD (spec §35). All coordination logic
lives in core's `Continuum` implementation and the pumps; providers implement
only these primitives with the required atomicity:

```java
public interface ContinuumRepository {
    void createComputation(Computation computation, StoredContinuation initial); // atomic pair (I2)
    RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation c); // atomic vs completion (I5)
    CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt); // ownership transfer (I7)
    Optional<Computation> findComputation(ComputationId id);   // pending or memoized result
    List<ClaimedDelivery> claimDeliveries(
        String workerId, Set<ComputationKind> kinds, int limit, Duration lease, Instant now);
    void acknowledgeDelivery(DeliveryId id);
    void releaseDelivery(DeliveryId id, Instant retryAt);      // increments delivery attempt count
    List<Computation> findExpired(Set<ComputationKind> kinds, Instant now, int limit);
    void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);
    int purgeResults(Instant now, int limit);                  // delete result rows past expires_at
}
```

`complete` performs the ownership transfer atomically: verify pending → delete
the pending row → write the result row (with resolved `expires_at`) → create
one outbox delivery per registered continuation → delete the continuation
rows. Supporting SPI types: `StoredContinuation(ContinuationId, byte[]
payload)`, `ClaimedDelivery(DeliveryId, CompletionDelivery, int attemptCount)`,
`DeliveryId(UUID)`, `RegistrationOutcome` (registered | resolved(outcome) |
not found), `CompletionOutcome` (completed | already resolved | not found).

## 7. `continuum-memory`

In-JVM `ContinuumRepository` backed by maps (pending computations,
continuations, results, outbox) guarded by a single lock — real atomicity for
the registration-vs-completion and complete-vs-complete races, not a mock.
Outbox claiming honors leases and `availableAt` using caller-supplied `now`
values. Intended for tests and embedded/single-process use.

## 8. `continuum-jdbc`

PostgreSQL over plain JDBC (`javax.sql.DataSource`; no Spring):

- The four tables of §2, DDL shipped as a classpath resource
  (`continuum-postgresql.sql`). No migration tooling; applications own schema
  management.
- The provider manages its own transactions (`autoCommit=false`,
  commit/rollback per SPI operation).
- Completion and registration lock the pending row with `SELECT ... FOR
  UPDATE`; a missing pending row falls through to the result table
  (`ALREADY_RESOLVED` / `Resolved(outcome)`) and then to not-found.
- Outbox claiming uses `FOR UPDATE SKIP LOCKED` so competing consumers never
  block one another.
- Kind-scoped claiming/expiry filters with `kind = ANY(?)`; composite indexes
  `(kind, available_at)` on the outbox and `(kind, deadline_at)` on the
  pending table serve both scoped and unscoped pumps.

## 9. `continuum-testing` (TCK)

Abstract JUnit 5 test classes exercising every required concurrency test from
spec §38 against any `Continuum`/`ContinuumRepository`:

- create-crash atomicity,
- register vs complete races (many iterations; every registration yields
  exactly one of Registered/Resolved; Registered implies an eventual delivery),
- complete vs complete (exactly one winner; stored outcome matches the winner),
- completion transaction failure (no partial outbox state; provider-specific
  injection),
- multiple concurrent continuations (each Registered has exactly one delivery
  obligation),
- competing consumers (single active lease),
- consumer crash / lease expiry reclaim,
- late registration (outcome returned, nothing persisted),
- timeout vs completion (exactly one terminal outcome),
- result purge (expired results deleted; unexpired and no-TTL results retained;
  purged ids answer NOT_FOUND / throw on registration).

`continuum-memory` runs the TCK as surefire unit tests; `continuum-jdbc` runs
it as failsafe integration tests against PostgreSQL in Testcontainers.
Provider-specific edge cases (SQL details, injected transaction failures) get
additional tests in their own modules.

## 10. Development approach

TDD throughout, building in dependency order:

1. core (API types, SPI, `Continuum` implementation, pumps),
2. memory provider + TCK (validating both together),
3. typed layer (`ContinuumClient`, `Retry`, `DeliveryRouter`),
4. jdbc provider against the TCK,
5. bom, docs, CI workflows, publishing setup.
