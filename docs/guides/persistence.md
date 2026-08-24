# Persistence Providers

## PostgreSQL (`continuum-jdbc`)

`JdbcContinuumRepository` runs over a plain `DataSource` — no Spring, no pool
opinions, driver scope `provided` (your app supplies it). It is deliberately
PostgreSQL-flavored: claiming uses `FOR UPDATE SKIP LOCKED` so competing
consumers never block one another, and the schema uses `UUID`, `TIMESTAMPTZ`,
and `BYTEA`. A lowest-common-denominator ANSI provider would forfeit exactly
the concurrency properties the outbox depends on; other databases deserve
their own providers certified against the TCK.

!!! warning "PostgreSQL means PostgreSQL"
    Wire-compatible databases — CockroachDB, YugabyteDB — report `PostgreSQL`
    through the product name and version that any detector reads, and they
    **accept** `FOR UPDATE SKIP LOCKED` rather than rejecting it. Neither is
    tested or supported here.

    So the failure mode is silent: the driver connects, the metadata says
    PostgreSQL, the claim query parses and runs, and nothing warns you. But
    parse success is not semantic support, and `claimDeliveries` is the one
    operation where the difference is load-bearing — the competing-consumer
    guarantee above rests on PostgreSQL's lock semantics specifically.

    Certify against the TCK before trusting either. The concurrency battery
    asserts observable contract rather than mechanism, so it can settle the
    question in both directions.

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
passes the same TCK as the PostgreSQL provider, including the concurrency
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
