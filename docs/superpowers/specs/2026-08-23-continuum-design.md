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
├── continuum-core        (API + SPI + typed client; deps: slf4j-api, codec-core)
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
exists only while pending**. Every terminalization — success, producer-reported
failure, or expiry — runs the same atomic ownership transfer: the pending row is
deleted, the outcome is written to a result record, and the outbox deliveries
are materialized, all in one transaction.

```text
continuum_computation   (pending only — presence means pending)
    id, kind, deadline_at, dispatch_payload NULL, attempt_count,
    created_at, last_updated_at

continuum_continuation  (id, computation_id, payload, created_at)

continuum_result        (computation_id, kind, outcome_type, outcome_payload,
                         expiry_kind, message, deadline_at, attempt_count,
                         created_at, completed_at)

continuum_outbox        (id, computation_id, continuation_id, kind,
                         continuation_payload, outcome_type, outcome_payload,
                         expiry_kind, message, available_at, claimed_by,
                         claimed_until, attempt_count, created_at)
```

Consequences:

- **Hot paths stay hot.** The pending table's size is the in-flight count;
  expiry scans and registration locks never wade through terminal rows.
- **No stored status anywhere.** Computation status is derived from table
  residency plus the result row's outcome arm; delivery state is derived from
  lease fields and row presence (a row existing means the delivery is still
  owed — spec §24; acknowledgment is deletion). Every "status" in the system
  is a reading of durable facts, never a mutable label.
- **`continuum_result` is the memoization requirement (spec §19) made
  concrete — for every outcome arm.** Late registration reads it to return
  `Resolved(outcome)` (I5); a duplicate or late `complete()` reads it to
  answer `ALREADY_RESOLVED` (critically: a slow producer completing *after*
  expiry learns it lost the race rather than seeing a phantom NOT_FOUND, and
  can never replace the sealed outcome — I4). Failures and expiries are
  memoized identically to successes.
- **Retryability ≡ dispatch payload presence.** There is no `RetrySemantics`
  enum and no `retry_semantics` column: a computation with a
  `dispatch_payload` is retryable; one without is not (nothing to redispatch,
  non-retryable by construction). "Never re-request non-retryable work" (spec
  §27) is enforced by this NULLness.
- **`attempt_count` starts at 1** — the original dispatch is attempt 1, so
  `atMost(3)` reads literally as "at most 3 total attempts." It is the only
  retry state Continuum persists.
- **Result retention is purge-time policy, not stored state.** There is no
  TTL column: `purgeExpiredResults(batchSize, ttl)` deletes this kind's
  result rows with `completed_at < now - ttl`. Retention policy lives at the
  pump call site, like retry policy. After purge, a duplicate `complete()`
  sees `NOT_FOUND` and a late registration throws — the retention-dependent
  ambiguity spec §17 anticipated.
- **Continuations live uniformly in their own table** (including the initial
  one). Creation inserts computation + initial continuation atomically (I2);
  the completion transfer deletes them as it folds them into outbox
  deliveries.

### Deliberate deviations from the specification's sketches

- **Three-arm `Outcome`.** The spec sketched `Success | Failure` with timeout
  as a `FailureKind`. v1 promotes expiry to a first-class arm (§3): a
  producer reporting failure and a deadline lapsing with no answer are
  different facts — a known "no" versus "never heard back" — and consumers
  switch on exactly that three-way distinction.
- **No `InvocationId`.** The spec's §28 idempotency key is real, but its home
  is the dispatch payload: the payload is written once at create and handed
  back verbatim on every retry, so a key embedded in it *cannot* drift
  between attempts — invariant I10 enforced structurally. Apps that keep
  dispatch state externally make their payload a small record wrapping their
  own key.
- **No `metadata` map.** It had no consumer in v1; the continuation and
  dispatch payloads already carry any application data.
- **No `RetrySemantics` enum / stored status / `version` column.** Replaced
  by dispatch-payload presence, table residency, and row locking.
- **`PendingComputation`/`ExpiredComputation` are realized as `Computation`**
  — the fields are identical.

## 3. Core API (`continuum-core`)

- `ComputationId(UUID)`, `ComputationKind(String)`, `ContinuationId(UUID)` —
  value records; IDs are random (v4) UUIDs generated by the library. Callers
  supply only payload bytes; Continuum assigns `ContinuationId` (a
  library-assigned dedup key is trustworthy).
- `Outcome` — sealed, three arms:

  ```java
  public sealed interface Outcome {
      record Success(byte[] payload) implements Outcome {}
      record Failure(String message) implements Outcome {}            // producer reported failure
      record Expired(ExpiryKind kind, String message) implements Outcome {} // deadline passed, retries done
  }
  public enum ExpiryKind { RETRY_DISALLOWED, RETRY_EXHAUSTED }
  ```

  `Expired` is minted only by timeout processing: `Continuum.complete()`
  rejects it with `IllegalArgumentException` (the client pump methods write it
  through the SPI). `Failure` carries only the producer's message — no
  `FailureInfo`/`FailureKind` taxonomy: `Expired` keeps a kind because
  Continuum mints both values and consumers branch on them; a producer's
  failure classification is app vocabulary, and genuinely structured error
  data belongs inside the app's own result type `R` (e.g. a sealed
  success/error), not in Continuum's envelope.
- `ComputationStatus` — `PENDING`, `COMPLETED`, `FAILED`, `EXPIRED`; derived:
  pending table → `PENDING`, otherwise a 1:1 reading of the result row's
  outcome arm.
- `Computation` — id, kind, status, createdAt, deadline, dispatchPayload
  (nullable), attemptCount, outcome (null while pending).
- `ComputationRequest(kind, continuationPayload, deadline, dispatchPayload)` —
  dispatchPayload nullable (its presence makes the computation retryable).
- `RegistrationResult` — sealed: `Registered(ContinuationId)` | `Resolved(Outcome)`.
- `CompletionResult` — `COMPLETED`, `ALREADY_RESOLVED`, `NOT_FOUND`.
- `CompletionDelivery(computationId, kind, continuationId, continuationPayload,
  outcome)` — the self-contained delivery unit (outcome denormalized into the
  outbox, spec §21).

```java
public interface Continuum {
    Computation create(ComputationRequest request);
    RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);
    CompletionResult complete(ComputationId id, Outcome outcome);   // rejects Outcome.Expired
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
   `now + timeout` arithmetic from it. Durations at the typed layer, absolute
   instants at the wire/storage layer, converted at the client boundary.
2. **Storage and coordination are opaque `byte[]`** end to end. The
   `Continuum` interface is the spec's byte[] contract (§32): no codec
   accessor, no type parameters. Generics are deliberately rejected on
   `Continuum` itself (one instance, many kinds, no honest single type
   assignment); strong typing is `ContinuumClient<R, C, D>` (section 4),
   where generics bind per kind. The raw API remains public and documented —
   the escape hatch for polyglot payloads; raw users drive the SPI's
   claim/ack/expire operations directly for pumping.
3. **Dispatch payload (retry breadcrumb)** — optional opaque `byte[]` meaning
   "how to (re)dispatch this work," mirroring the continuation payload's
   "what to do with the result." Persisted atomically with the computation at
   create (riding I2's transaction), never mutated, never interpreted by
   Continuum, handed back verbatim on every timeout. Its presence defines
   retryability. Embed your external idempotency key in it. It is a
   write-once breadcrumb, not mutable workflow state (spec non-goal §3).

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
Applies to `Continuum.client(...)` and `Retry.of(...)`.

### Minting clients

```java
var toolCalls = continuum.client(
    "tool-result",
    ToolCallResult.class, ToolCallContinuation.class, ToolCallDescriptor.class,
    cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
              .deadline(Duration.ofMinutes(5)));

var approvals = continuum.client(
    "approval",
    Decision.class, ApprovalContinuation.class,        // two-type: non-retryable
    cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
              .deadline(Duration.ofDays(3)));
```

`Continuum.client(...)` is a `default` method building the client purely from
its arguments. `ClientConfig<R, C, D>`: `codecs(CodecFactory)` resolves the
payload codecs (per-payload `Codec<T>` overrides available);
`deadline(Duration)` is the per-attempt timeout default (`create` computes
`now + duration`; per-call override available). **The two-type overload is the
non-retryable spelling**: no dispatch type, no dispatch payload ever
persisted — "dispatch payload without retry support" and `Void.class` filler
are both unrepresentable. Retry *policy* does not live on the client config —
it is supplied at the pump site (below).

### Creating, completing, registering

```java
var computation = toolCalls.create(continuation, descriptor);       // three-type
var computation2 = approvals.create(approvalContinuation);          // two-type
toolCalls.complete(computation.id(), result);                       // encodes, Outcome.Success
TypedRegistration<R> r = toolCalls.register(computationId, otherContinuation);
```

All built on the raw byte[] API. `TypedOutcome<R>` mirrors `Outcome`:
`Success<R>(R value)` | `Failure<R>(String)` | `Expired<R>(ExpiryKind,
String)`; `TypedRegistration<R>` is `Registered(ContinuationId)` |
`Resolved(TypedOutcome<R>)`.

### Pumping — methods on the client, scheduled by the application

There are no pump/worker/router classes. The client is bound to its kind and
owns its codecs, so the three recurring activities are **batch methods on the
client**; the application schedules them on whatever cadence and machinery it
likes, per kind:

```java
public final class ContinuumClient<R, C, D> {
    ...
    int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer);
    int retryExpiredComputations(int batchSize, Retry<D> retry);   // three-type only
    int failExpiredComputations(int batchSize);                    // two-type only
    int purgeExpiredResults(int batchSize, Duration ttl);
}
```

Each call processes one bounded batch and returns the count — the drain
signal (`while (client.deliverResults(...) > 0)` after downtime). Scheduling
is the host's own machinery, fixed-delay recommended:

```java
scheduler.scheduleWithFixedDelay(() ->
    toolCalls.deliverResults(25, (cont, outcome) -> switch (outcome) {
        case Success<ToolCallResult>(var result) -> backlog.recordResult(cont, result);
        case Failure<ToolCallResult>(var message) -> backlog.recordFailure(cont, message);
        case Expired<ToolCallResult>(var kind, var msg) -> backlog.recordTimeout(cont, kind, msg);
    }), 0, 1, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.retryExpiredComputations(12, Retry.of(r -> r
        .atMost(3)
        .handler((toolCall, ctx) -> toolRuntime.dispatch(toolCall, ctx.computationId())))),
    5, 15, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.purgeExpiredResults(200, Duration.ofHours(1)), 1, 10, TimeUnit.MINUTES);
```

Semantics:

- **`deliverResults`** — claims up to `batchSize` of this kind's outbox items
  under a lease, decodes, invokes the consumer per item. Success →
  acknowledge (delete). Consumer exception → release with backoff (attempt
  count incremented); one item's failure never blocks the others (I8).
  At-least-once; consumers deduplicate on `ContinuationId` (spec §23). Every
  outcome arm arrives through this one channel — success, failure, and expiry
  (spec §29: failures follow the same delivery path); a continuation is
  guaranteed exactly one eventual delivery *whatever* happened, so there is
  no separate timeout-notification channel to miss.
- **`retryExpiredComputations`** — for each of this kind's pending
  computations past deadline: invoke the supplied `Retry<D>` with the decoded
  dispatch payload. `Retried`/`RetriedDefault` → extend deadline
  (`now + timeout` / `now + client deadline`), increment `attempt_count`
  atomically. `NotRetried(reason)` → terminalize as
  `Expired(RETRY_EXHAUSTED, reason)` through the normal transfer (outbox
  fan-out included). `Retry` exception → leave untouched, log; next pump
  retries (bounded by pump cadence — no hot loop). Timeout-paced retries are
  self-throttling: attempt N+1 cannot occur until attempt N's full timeout
  elapses.
- **`failExpiredComputations`** — the two-type counterpart: expired
  computations terminalize as `Expired(RETRY_DISALLOWED, ...)`. No `Retry`
  parameter — there is nothing to consult, enforced by the client shape.
- **`purgeExpiredResults`** — deletes up to `batchSize` of this kind's result
  rows with `completed_at < now - ttl`. Policy-free storage; policy at the
  call site.

Concurrency/ops properties: every app instance can run all pumps identically —
no leader election; leases and `SKIP LOCKED` make overlapping pumps across
nodes correct, merely occasionally redundant (duplicate retry requests are
at-least-once, made idempotent by the key the app embeds in the dispatch
payload). Crash-anywhere is the default: claimed deliveries reappear on lease
expiry, half-reaped batches are re-found, purges are idempotent deletes. If a
pump is never scheduled, no correctness property is lost — deliveries wait,
expired computations sit pending, results accumulate (liveness only, I9).

### `Retry<D>`

One object that *performs* (or schedules) the redispatch itself and reports
what it did:

```java
public interface Retry<D> {
    RetryResult onTimeout(D dispatch, RetryContext context);

    sealed interface RetryResult {
        record Retried(Duration timeout) implements RetryResult {}  // new deadline = now + timeout
        record RetriedDefault() implements RetryResult {}           // now + the client's configured deadline
        record NotRetried(String reason) implements RetryResult {}

        static RetryResult retried() { ... }
        static RetryResult retried(Duration timeout) { ... }
        static RetryResult notRetried(String reason) { ... }
    }
}
```

`RetryContext` carries Continuum's durable facts: `computationId` (the
redispatched worker must know where to `complete()` to), `kind`,
`attemptCount`, and the expired `deadline`; `D` is the decoded dispatch
payload — the payload is the application's vocabulary, the context is
Continuum's facts. The `RetryResult` values are pure data; the client method
interprets them with its config in hand.

The declarative front door is `Retry.of(customizer)`: config takes `atMost(n)`
(n = total attempts, matching attempt_count starting at 1), a
`handler(BiConsumer<D, RetryContext>)` that *only dispatches*, and optionally
`timeout(Duration)` for retry deadlines differing from first attempts. Derived
mechanically: attempts exhausted → `NotRetried` without invoking the handler;
otherwise invoke and report `RetriedDefault` (or `Retried(t)`). Implementing
`Retry<D>` directly is the escape hatch for decisions the config cannot
express (attempt-dependent backoff, circuit breakers). Retry policy travels as
code at the pump site — whichever node pumps supplies the same policy from the
same source.

Concrete formats come from the codec project's backends: `codec-jackson`
(Jackson 3), `codec-gson`, `codec-protobuf`; `codec-jackson2` is a follow-up
in the codec repo — Continuum ships no serialization-format modules of its
own.

## 5. Persistence SPI (`org.jwcarman.continuum.spi`)

Semantic atomic operations, not generic CRUD (spec §35). All coordination
logic lives in core (`DefaultContinuum`, the client pump methods); providers
implement only these primitives with the required atomicity. Pumping
operations are kind-scoped (the client is kind-bound; raw users pass the kind
explicitly):

```java
public interface ContinuumRepository {
    void createComputation(Computation computation, StoredContinuation initial); // atomic pair (I2)
    RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation c); // atomic vs completion (I5)
    CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt); // ownership transfer (I7)
    Optional<Computation> findComputation(ComputationId id);   // pending or memoized result
    List<ClaimedDelivery> claimDeliveries(String workerId, ComputationKind kind, int limit, Duration lease, Instant now);
    void acknowledgeDelivery(DeliveryId id);
    void releaseDelivery(DeliveryId id, Instant retryAt);      // increments delivery attempt count
    List<Computation> findExpired(ComputationKind kind, Instant now, int limit);
    void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);
    int purgeResults(ComputationKind kind, Instant olderThan, int limit);
}
```

`complete` performs the ownership transfer atomically: verify pending → delete
the pending row → write the result row → create one outbox delivery per
registered continuation → delete the continuation rows. The same operation
serves success, failure, and expiry outcomes. Supporting SPI types:
`StoredContinuation(ContinuationId, byte[] payload)`,
`ClaimedDelivery(DeliveryId, CompletionDelivery, int attemptCount)`,
`DeliveryId(UUID)`, `RegistrationOutcome` (registered | resolved(outcome) |
not found), `CompletionOutcome` (completed | already resolved | not found),
`ContinuumPersistenceException`.

## 6. `continuum-memory`

In-JVM `ContinuumRepository` backed by maps (pending computations,
continuations, results, outbox) guarded by a single lock — real atomicity for
the registration-vs-completion and complete-vs-complete races, not a mock.
Outbox claiming honors leases and `availableAt` using caller-supplied `now`
values. Intended for tests and embedded/single-process use.

## 7. `continuum-jdbc`

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
  block one another; all pump queries filter by kind. Indexes:
  `(kind, available_at)` on the outbox, `(kind, deadline_at)` on the pending
  table, `(kind, completed_at)` on results.

## 8. `continuum-testing` (TCK)

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
- timeout vs completion (exactly one terminal outcome; late complete after
  expiry answers ALREADY_RESOLVED),
- expiry outcomes delivered through the normal path (Expired arm, correct
  ExpiryKind for RETRY_EXHAUSTED vs RETRY_DISALLOWED),
- result purge (old results deleted, recent retained; purged ids answer
  NOT_FOUND / throw on registration).

`continuum-memory` runs the TCK as surefire unit tests; `continuum-jdbc` runs
it as failsafe integration tests against PostgreSQL in Testcontainers.
Provider-specific edge cases (SQL details, injected transaction failures) get
additional tests in their own modules.

## 9. Development approach

TDD throughout, building in dependency order:

1. core (API types, SPI, `DefaultContinuum`),
2. memory provider + TCK (validating both together),
3. typed layer (`ContinuumClient` with pump methods, `Retry`),
4. jdbc provider against the TCK,
5. bom, docs, CI workflows, publishing setup.
