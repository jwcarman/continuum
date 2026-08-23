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
├── continuum-core        (API + SPI + pump components; deps: slf4j-api only)
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
  pending), plus retrySemantics, invocationId, metadata, attemptCount.
- `ComputationRequest(kind, continuationPayload, deadline, retrySemantics,
  invocationId, metadata)`.
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
   sources.
4. **Payloads are opaque `byte[]`** end to end. No serialization framework in
   core; applications encode/decode their own continuation payloads and results.

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

For each pending computation past its deadline (spec §29):

- Already terminal → skip (races with completion are safe: completion is
  first-wins atomic).
- `NON_RETRYABLE` → complete as `Failure(TIMEOUT_NON_RETRYABLE)` through the
  normal completion path (outbox fan-out included).
- `RETRYABLE` and attempts remain → invoke the application's
  `RetryHandler.retry(computation)` with the same `InvocationId`, increment the
  attempt count, and push the deadline forward by a configurable extension.
- `RETRYABLE` and attempts exhausted (configurable `maxAttempts` on the reaper) →
  complete as `Failure(TIMEOUT_RETRY_EXHAUSTED)`.

Duplicate retry requests are possible (at-least-once) and are made idempotent by
the stable `InvocationId` (spec §31).

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
  schema management.
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
