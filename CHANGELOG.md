# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **`continuum-jdbc` now refuses databases that merely impersonate PostgreSQL.**
  CockroachDB and YugabyteDB report `PostgreSQL` through every metadata field a
  driver exposes and *accept* `FOR UPDATE SKIP LOCKED`, so pointing the
  repository at one used to connect, parse, run, and warn nobody — while the
  lock semantics the outbox's competing-consumer guarantee rests on went
  unverified. On first use (not at construction — wiring a bean opens no
  connection) the repository now detects the actual platform via
  `org.jwcarman.accent:accent` (zero transitive dependencies) and throws unless
  it finds genuine PostgreSQL 9.5+, naming what it found:
  `unsupported database platform: CockroachDB v24.1.0 (reports as PostgreSQL
  13.0.0)`. Also catches PostgreSQL < 9.5, which previously failed confusingly
  at claim time. `JdbcContinuumRepository.assumePostgreSql(dataSource)` bypasses
  detection for operators who know better.


## [0.3.0] - 2026-08-24

### Breaking changes

- **`deliverResults` now hands the consumer one `TypedDelivery` instead of two
  loose values.** The old `(continuation, outcome)` shape could not see what the
  pump already knew, which made the timestamps added in 0.1.0 unreachable from
  the API almost everyone uses.

  ```java
  // before
  client.deliverResults(BatchSize.of(25), (continuation, outcome) -> ...);

  // after
  client.deliverResults(BatchSize.of(25), delivery ->
      ... delivery.continuation() ... delivery.outcome() ...);
  ```

  `TypedDelivery` adds `computationId`, `continuationId`, `submittedAt`,
  `completedAt`, `elapsedTime()` and `deliveryAttempt()` — so a consumer can
  correlate a log line, record end-to-end duration as the outcome arrives, and
  implement a give-up or dead-letter policy without dropping to the raw SPI.

  This replaces the two-argument overload rather than adding to it. Keeping both
  made an *overloaded* method reference (`list::add`) ambiguous, and left two
  ways to express one thing.

- **`ClaimedDelivery.attemptCount` is now `deliveryAttempt`.** `attemptCount`
  meant *dispatch* attempts on `Computation`/`ExpiryContext` and *delivery*
  attempts here — one word, two meanings. `attemptCount` now has exactly one.

### Changed

- Upgraded to codec 0.4.0, whose BOM no longer re-exports Spring Boot's
  dependency management. Continuum declares `codec-core` directly rather than
  importing `codec-bom`, so this changes nothing here — but a project importing
  both BOMs was still exposed from the codec side until now.

### Fixed

- `deliverResults` javadoc states the acknowledge/release contract outright:
  returning acknowledges the delivery, throwing releases it with backoff. The
  consume-without-acting case — returning to drop a delivery you have decided is
  stale — was previously only inferable from prose.


## [0.2.0] - 2026-08-24

### Fixed

- **`continuum-bom` no longer leaks the build's dependency pins.** It inherited
  `continuum-parent`, and a BOM's *effective* `dependencyManagement` is what
  consumers import — so importing it re-exported `junit-bom`,
  `testcontainers-bom`, AssertJ, Mockito, Logback and the PostgreSQL driver,
  silently overriding the consumer's own versions. Mixed-version JUnit fails
  during test discovery rather than resolution, so it presented as a
  `NoSuchMethodError` that looked nothing like a BOM problem. The BOM is now
  parentless and manages only the artifacts this project owns. **If you added a
  workaround pinning `org.junit:junit-bom` ahead of `continuum-bom`, you can
  drop it.**

### Changed

- **Identities are time-ordered UUIDv7 rather than random UUIDv4.** Every
  identity is a primary key, and v4 scatters inserts across the whole index
  keyspace. Same 128 bits, same `UUID`, no schema migration, and v4 identities
  minted by 0.1.0 remain valid — a table will simply hold both. Note that a v7
  identity discloses roughly when it was minted, which `submitted_at` already
  records.
- **`continuum-core` no longer drags Spring onto your classpath.** codec 0.2.0
  declared `spring-boot-autoconfigure` at compile scope, so `continuum-core`
  transitively pulled the Spring context stack — contradicting the no-Spring
  positioning the JDBC provider rests on. Upgrading to codec 0.3.0 reduces the
  compile tree to `slf4j-api`, `codec-core` and `java-uuid-generator`. If you
  were (accidentally) compiling against Spring by way of `continuum-core`, you
  now need to declare it yourself.

### Documentation

- `persistence.md` now states that PostgreSQL means PostgreSQL. CockroachDB and
  YugabyteDB report `PostgreSQL` through the product name and version any
  detector reads, and **accept** `FOR UPDATE SKIP LOCKED` rather than rejecting
  it — so pointing `continuum-jdbc` at one connects, parses, runs, and warns
  nobody, while the lock semantics the outbox rests on may not hold. Neither is
  tested or supported.

## [0.1.0] - 2026-08-24

Initial release: the byte[] `Continuum` core, typed clients
(`ContinuumClient` / `RetryableContinuumClient`), application-pumped
delivery/expiry/purge, in-memory and PostgreSQL providers, a provider TCK,
and a Spring Boot starter.

Deadlines are mandatory — that is what makes "one computation, one *eventual*
outcome" true. An open-ended wait is expressed as a decision taken at each
lapse rather than as an absent deadline, so the deadline stays a dead-man's
switch and giving up is explicit and carries a reason:

- `ExpiryContext` carries the computation's `submittedAt` and the pump's
  `observedAt`, with `elapsedTime()` measured on Continuum's `InstantSource`.
  A wall-clock give-up rule ("stop waiting seven days after submission") needs
  no arithmetic over attempt counts, which is wrong once attempts carry
  differing timeouts. Both `Retry` and `Expiry` receive it.
- `ContinuumClient.failExpiredComputations(BatchSize, Expiry)` lets a
  **non-retryable** kind keep waiting or give up at each lapse — without
  inventing a dispatch payload it must never use, which would trade the
  compile-time non-retryability guarantee for a convention.
- `CompletionDelivery` carries `submittedAt`, `completedAt`, and
  `elapsedTime()`, denormalized onto the outbox row so a consumer can record
  end-to-end duration when the outcome arrives, even after the memoized result
  is purged.

Timestamp vocabulary is uniform: `submitted_at` is when the computation was
submitted, `completed_at` when it reached a terminal outcome, and `created_at`
means only "this row was written".

See the [documentation](https://jwcarman.github.io/continuum/) for the full
tour.

[0.3.0]: https://github.com/jwcarman/continuum/releases/tag/0.3.0
[0.2.0]: https://github.com/jwcarman/continuum/releases/tag/0.2.0
[0.1.0]: https://github.com/jwcarman/continuum/releases/tag/0.1.0
