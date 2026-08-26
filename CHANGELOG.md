# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.5.0] - 2026-08-26

### Changed

- codec 0.6.0 (from 0.4.0). Purely additive upstream — new backends and
  transforms (`codec-crypto`, `codec-fory`, `codec-jsonb`, `codec-zstd`,
  `Base64Codec`); nothing continuum uses changed.
- Test infrastructure: Testcontainers 2.0.5 and JUnit 6.1.3, every module on
  the same JUnit line; test output no longer defaults to DEBUG.

### Added

- **`continuum-mongo`: MongoDB persistence, certified.** `MongoContinuumRepository`
  over a plain `MongoClient` passes the full TCK, concurrency suites included,
  on MongoDB 8.2 every build; MongoDB 5.0+ replica sets are supported. Four
  collections mirror the JDBC tables; every multi-document operation is a
  transaction; the outbox claim is a per-document `findOneAndUpdate`
  compare-and-set — what `SKIP LOCKED` does in SQL, with no locking clause.
  Instants are BSON `date` (millisecond precision). `ensureIndexes()` creates
  the four query indexes idempotently. On first use the repository refuses, by
  name and with the fix in the message, what cannot meet the TCK: standalone
  servers (no multi-document transactions — one node with `--replSet` is
  enough), MongoDB before 5.0, and Amazon DocumentDB, Azure Cosmos DB and
  FerretDB until certified. `assumeMongoDb` bypasses detection.
- **`continuum.persistence.type`** (`jdbc` | `mongo` | `memory`) in the Spring
  Boot auto-configuration. Unnecessary with one durable provider present; with
  both `continuum-jdbc`+`DataSource` and `continuum-mongo`+`MongoClient`
  available and the property unset, startup fails naming it — Spring
  Session's `store-type` rule. Plus `continuum.mongo.database` and
  `continuum.mongo.ensure-indexes`.

## [0.4.0] - 2026-08-25

### Added

- **`continuum-jdbc` supports SQL Server 2012+, certified.** Full TCK,
  concurrency suites included, on every build. SQL Server is the only certified
  platform whose locking comes from a different mechanism rather than a
  different spelling: it has no `FOR UPDATE` clause, so the pending-row lock
  becomes a `WITH (UPDLOCK, ROWLOCK)` table hint and the skip-locked claim
  becomes `WITH (UPDLOCK, READPAST, ROWLOCK)`. accent's `supportsSkipLocked()`
  is deliberately `false` for SQL Server — that predicate covers the
  `FOR UPDATE SKIP LOCKED` clause specifically — so admission gates on the 2012
  version floor (`OFFSET/FETCH`) instead. Also adds `continuum-sqlserver.sql`
  (`CHAR(36)`, `DATETIME2(6)`, `VARBINARY(MAX)`), and the purge subquery now
  orders by `completed_at` on every platform: SQL Server's `OFFSET/FETCH`
  requires an `ORDER BY`, and purging oldest-first is the right order anyway.

- **`continuum-jdbc` supports Oracle 23ai+, certified.** Full TCK, concurrency
  suites included, on every build. Oracle brought the dialect seam its first
  genuine behavioral difference: a row-limited read cannot be locked (Oracle's
  `FETCH FIRST` is an inline view), so the claim query carries no limit there
  and the provider stops fetching after `limit` rows, locking each as read —
  the Oracle AQ dequeue idiom. Plus `FETCH FIRST ? ROWS ONLY` in place of
  `LIMIT ?`, and a `continuum-oracle.sql` reference DDL (`VARCHAR2(36)`,
  `BLOB`, `CLOB`). The certification suite pools connections via UCP; an
  unpooled `OracleDataSource` opens a physical session per operation and the
  TCK's race loops exhaust Oracle Free's session cap.

- **H2 2.3+ is certified for test/embedded use.** It passes the full TCK in
  both default and PostgreSQL-compatibility modes, and the reference PostgreSQL
  DDL now serves it verbatim (`TIMESTAMP(6) WITH TIME ZONE` is spelled out
  rather than the `TIMESTAMPTZ` alias H2 rejects — identical on PostgreSQL). A
  Spring Boot test context wiring an H2 `DataSource` can now exercise the real
  JDBC provider — SQL, schema, dialect — without a container, which
  `continuum-memory` cannot offer since it bypasses SQL entirely. Not a
  production database; the certified list says so.

- **`continuum-jdbc` now supports MySQL 8+ and MariaDB 10.6+, certified.** All
  three platforms — PostgreSQL 9.5+, MySQL 8.4, MariaDB 11.4 — pass the full TCK
  battery, concurrency suites included, and MariaDB is certified through both
  its own driver and mysql-connector-j (the pairing that reports `MySQL` and
  betrays the real engine only in the version string — detection disambiguates
  it). The certification suites run as ordinary integration tests, so every
  build re-certifies all three.

  What actually varies between platforms turned out to be almost nothing: one
  JDBC-layer fact (PostgreSQL binds native `uuid`, MySQL/MariaDB bind
  `CHAR(36)` strings — UUIDv7's canonical text sorts identically to its byte
  order, so time-ordered index locality survives), the type names in a new
  `continuum-mysql.sql` reference DDL (`DATETIME(6)` rather than `TIMESTAMP`,
  which ends at 2038), and one purge statement rewritten into a derived-table
  form both platform families accept. The `ContinuumDialect` seam carries
  exactly that and nothing else, and
  `JdbcContinuumRepository.withDialect(dataSource, dialect)` is the extension
  point for a platform certified outside this project. `assumePostgreSql`
  remains for operators bypassing detection.

  Platform detection now selects the dialect rather than merely passing
  judgment: genuine PostgreSQL 9.5+, MySQL 8+, and MariaDB 10.6+ are admitted;
  everything else is refused by name, wire-compatible impostors with their real
  engine version.

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

  The refusal is now evidence-backed, not precautionary: the TCK was run
  against both wire-compatible platforms (six runs each, ~300 races per
  platform). CockroachDB v24.1 failed every run, twice silently — both racing
  transactions committed, `Registered` was returned, and the delivery was never
  created. YugabyteDB 2024.1 failed every run loudly (`Restart read required`),
  with no silent violation observed. The skip-locked capability itself held on
  both; what broke is `FOR UPDATE` mutual exclusion composing with the
  ownership transfer. The certification harnesses ship in continuum-jdbc's test
  sources as `*CertificationExperiment` — deliberately not `*IT`, so they never
  run in a default build — runnable on demand via
  `mvn -pl continuum-jdbc verify -Dit.test=CockroachCertificationExperiment`.


### Changed

- `JdbcContinuumRepository` assembles the four dialect-dependent statements
  exactly once, when the dialect resolves, rather than at each call site; the
  string-keyed dialects share a `StringUuidDialect` base. No behavioral change.
- Routine dependency and plugin bumps; test-scope drivers now certify on
  H2 2.4, mysql-connector-j 26.7, mssql-jdbc 13.4, ojdbc11 23.26.

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

[0.5.0]: https://github.com/jwcarman/continuum/releases/tag/0.5.0
[0.4.0]: https://github.com/jwcarman/continuum/releases/tag/0.4.0
[0.3.0]: https://github.com/jwcarman/continuum/releases/tag/0.3.0
[0.2.0]: https://github.com/jwcarman/continuum/releases/tag/0.2.0
[0.1.0]: https://github.com/jwcarman/continuum/releases/tag/0.1.0
