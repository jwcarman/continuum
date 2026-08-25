# Persistence Providers

## JDBC (`continuum-jdbc`)

`JdbcContinuumRepository` runs over a plain `DataSource` — no Spring, no pool
opinions, driver scope `provided` (your app supplies it). Claiming uses
`FOR UPDATE SKIP LOCKED` so competing consumers never block one another; a
lowest-common-denominator ANSI provider would forfeit exactly the concurrency
properties the outbox depends on, so support means certification, not syntax.

| Platform | Status | Reference DDL |
|---|---|---|
| PostgreSQL 9.5+ | **Certified** — full TCK, every release | `continuum-postgresql.sql` |
| MySQL 8+ | **Certified** — full TCK, every release | `continuum-mysql.sql` |
| MariaDB 10.6+ | **Certified** — full TCK, both drivers (its own and mysql-connector-j) | `continuum-mysql.sql` |
| Oracle 23ai+ | **Certified** — full TCK, every release | `continuum-oracle.sql` |
| H2 2.3+ | **Certified for test/embedded use** — full TCK in default and PostgreSQL modes; not a production database | `continuum-postgresql.sql` |
| CockroachDB | **Refused** — failed certification, twice silently | — |
| YugabyteDB | **Refused** — failed certification loudly; a retry layer could revisit | — |

The provider detects the actual platform on first use and selects the matching
dialect; everything below the certified rows is refused by name.

!!! warning "PostgreSQL means PostgreSQL"
    Wire-compatible databases — CockroachDB, YugabyteDB — report `PostgreSQL`
    through the product name and version that any detector reads, and they
    **accept** `FOR UPDATE SKIP LOCKED` rather than rejecting it. Neither is
    tested or supported here.

    Parse success is not semantic support, and `claimDeliveries` is the one
    operation where the difference is load-bearing — the competing-consumer
    guarantee above rests on PostgreSQL's lock semantics specifically.

    **Since 0.4.0 the repository refuses impostors instead of silently
    accepting them.** On first use it detects the actual platform (via
    [accent](https://github.com/jwcarman/accent), one `SELECT version()`
    round trip, once per instance) and throws unless it finds genuine
    PostgreSQL 9.5+ — naming what it found, real engine version included.
    An operator who knows better can bypass detection with
    `JdbcContinuumRepository.assumePostgreSql(dataSource)`, accepting the
    silent-failure risk the guard exists to remove.

    This is no longer a hedge: **the TCK has been run against both.** On
    CockroachDB v24.1, six of six runs failed — usually a serialization retry
    error (SQLSTATE 40001) the provider does not retry, and twice the silent
    form: both racing transactions committed, `registerContinuation` returned
    `Registered`, and the delivery was never created. No error anywhere. On
    YugabyteDB 2024.1, six of six runs failed loudly and identically
    (`Restart read required`, its client-must-retry signal), with no silent
    violation observed in ~300 races.

    Note the skip-locked capability itself held on both — the claiming and
    racing suites passed every run. What broke is `FOR UPDATE` mutual
    exclusion composing with the ownership transfer, which is exactly why
    "accepts the syntax" and "keeps the contract" are different claims, and
    why the guard refuses what the TCK has not certified.

### Schema — yours, not ours

Continuum **never creates or migrates schema**. The reference DDL ships as a
classpath resource:

```
org/jwcarman/continuum/jdbc/continuum-postgresql.sql
```

Copy it into your migration tool (e.g. Flyway's `V1__continuum.sql`) and let
your existing discipline own it. The file is idempotent
(`CREATE ... IF NOT EXISTS`), and the integration tests execute the shipped
resource verbatim — so the file you copy is the file that is proven.

Four tables: `continuum_computation` (pending only — presence means pending),
`continuum_continuation`, `continuum_result` (memoized terminal outcomes),
and `continuum_outbox` (active delivery obligations only).

## In-memory (`continuum-memory`)

`InMemoryContinuumRepository` is a faithful implementation, not a mock — it
passes the same TCK as the JDBC provider, including the concurrency
battery. Single JVM, no durability across restarts: right for unit tests and
embedded/single-process use.

## Writing your own provider

Implement `ContinuumRepository` — ten semantic operations, not generic CRUD.
The contract's heart is atomicity: creation persists the computation and its
initial continuation as one unit; registration is atomic against completion;
completion performs the ownership transfer (delete pending, write result,
fan out outbox) in one transaction.

Then certify it: depend on `continuum-testing` (test scope) and extend the
TCK —

```java
class MyProviderTckTest extends ContinuumTck {
  @Override
  protected ContinuumRepository createRepository() {
    return new MyContinuumRepository(...);
  }
}
```

The TCK exercises lifecycle semantics, registration-vs-completion and
complete-vs-complete races, competing consumers, lease expiry, late
registration, expiry outcomes, and purge behavior. A `MutableInstantSource`
is provided so the suite controls time.
