# Multi-dialect JDBC support via accent

**Status:** design note, nothing implemented
**Date:** 2026-08-24
**Target:** 0.4.0 or later — see Sequencing

---

## The question

`continuum-jdbc` is PostgreSQL-only. `accent` (`org.jwcarman.accent:accent`) now
exists to identify which database a `DataSource` is actually talking to. What
does that buy us, and what does it not?

## What accent gives us

`Accent.of(dataSource)` returns a sealed `Platform` — fourteen arms plus
`Unknown` — and one capability predicate:

```java
boolean supportsSkipLocked()
```

Documented narrowly: whether the platform supports `FOR UPDATE SKIP LOCKED`
*with genuine skip-locked semantics* — a second transaction reading locked rows
skips rather than blocks. Every arm returning `true` is backed by a contention
test in accent's integration suite, not a syntax check.

Measured by running accent 0.1.0-SNAPSHOT rather than reading its source:

| Platform | `supportsSkipLocked()` |
|---|---|
| PostgreSQL 9.5+ | `true` (9.4 → `false`) |
| CockroachDB 24.1 | `true` |
| YugabyteDB | `true` |
| MySQL 8+ | `true` (5.7 → `false`) |
| MariaDB 10.6+ | `true` (10.5 → `false`) |
| Oracle 23 | `true` |
| H2 2.3 | `true` |
| SQL Server 2022 | `false` |
| Db2, HSQLDB, SQLite, Derby, Unknown | `false` |

Two properties that matter to us:

- **`false` is always safe.** A caller falls back to plain `FOR UPDATE`, which
  blocks instead of skipping — slower under contention, never incorrect.
- **SQL Server is deliberately `false`.** Its nearest equivalent is `WITH
  (UPDLOCK, READPAST)`, a different statement with different semantics. accent
  declines to conflate them, which is the right call and means SQL Server needs
  a genuinely different claim query rather than a flag.

**Dependency cost: zero.** accent has no compile or runtime dependencies. That
matters here — `continuum-jdbc` carries only `continuum-core` and a `provided`
driver, and the no-Spring, dependency-light posture is asserted in
`persistence.md` and rests under the JDBC provider's whole design.

## What accent does not give us

Detection is roughly a tenth of the problem. The rest is ours:

### Schema types

The reference DDL is Postgres-specific in 26 places:

| Type | Occurrences | Needs mapping to |
|---|---|---|
| `TIMESTAMPTZ` | 12 | `TIMESTAMP WITH TIME ZONE`, or `DATETIME2` on SQL Server |
| `UUID` | 7 | `CHAR(36)`/`BINARY(16)` where there is no native type |
| `BYTEA` | 5 | `BLOB`, `VARBINARY(MAX)`, `LONGBLOB` |

accent says *which* dialect. It does not write DDL. Note the schema is
application-owned — Continuum never runs it — so this is about shipping correct
*reference* DDL per dialect, not about migration machinery.

### Claiming beyond skip-locked

`claimDeliveries` is the dialect-sensitive operation and the one place the
difference is load-bearing: the outbox's competing-consumer guarantee rests on
its locking semantics. SQL Server needs a different statement, not a different
flag.

### Certification

The TCK's concurrency battery asserts *observable contract* rather than
mechanism — "the two workers' claims sum to one", "exactly one winner whose
outcome is stored". That is what makes it able to certify a dialect honestly,
and it is the real gate. accent tells you what you are talking to; the TCK tells
you whether Continuum's guarantees hold there.

## Proposed shape

### Detect once, at construction

`Accent.of(dataSource)` opens a connection and runs `SELECT version()`. That is
a startup cost, paid once and cached on the repository — never per query.

### Do not branch on `Platform` inside the repository

Tempting, and wrong. Pattern-matching `Platform` at each call site scatters
dialect knowledge across the class and makes every new database a diff through
`JdbcContinuumRepository`.

Instead, a narrow seam holding only what actually varies:

```java
public interface ContinuumDialect {
    String claimDeliveriesSql();   // the FOR UPDATE SKIP LOCKED / READPAST / plain variant
    String binaryColumnType();     // BYTEA | BLOB | VARBINARY(MAX)
    // add only on demand — resist a general capability bag
}
```

`Platform` *selects* a dialect; it does not become one. Three consequences worth
having:

- Dialect knowledge lives in one place per database.
- Someone can supply their own dialect for a database we have not taught, without
  waiting on a release — the extension point accent deliberately does not offer,
  and does not need to, because it is the *vocabulary*.
- The repository stays readable: one `dialect.claimDeliveriesSql()` rather than a
  switch.

### `Unknown` fails loudly

Never guess. A database accent does not recognise should refuse at construction
with a message naming the product string, not silently receive PostgreSQL SQL.
The same applies to a recognised platform with no dialect implementation.

### Keep detection optional

The constructor taking an explicit dialect stays. Auto-detection is a
convenience, not a requirement: someone who knows their database should be able
to say so and skip the startup query entirely.

## Sequencing

**Do the fan-out ceiling first.** `complete()` writes one outbox delivery per
registered continuation in a single transaction, with nothing capping
continuations per computation. DynamoDB's `TransactWriteItems` caps at 100 items,
so a document-store provider cannot complete a computation with ~98+
continuations. That constrains what a non-SQL provider can do *at all*, it lives
in the SPI contract rather than any provider, and 1.0 freezes it. Multi-dialect
JDBC is additive by comparison.

**Then dialects, one at a time, each TCK-certified before it ships.** Suggested
order by value and by how much new machinery each forces:

1. **CockroachDB / YugabyteDB** — closest to free. Postgres wire protocol,
   Postgres types, and accent has measured both honouring skip-locked under
   contention. Mostly a certification exercise, and it would convert
   `persistence.md`'s current hedge into a supported claim.
2. **MySQL / MariaDB** — first real type mapping (`BLOB`, no native `UUID`), but
   skip-locked works on 8+/10.6+.
3. **SQL Server** — first genuinely different claim statement. Do it last, and
   only if wanted.

## Open questions

1. Does the `Unknown` refusal belong at construction or first use? Construction
   fails fast but makes a misconfigured test environment fail at wiring rather
   than at the operation that cares.
2. Should `ContinuumDialect` be sealed? Same trade accent faced — sealing gives
   exhaustiveness and forecloses third-party dialects. Leaning open here, because
   unlike accent's vocabulary this is an extension point, not a closed
   description of the world.
3. Should the reference DDL ship per dialect as separate classpath resources
   (`continuum-postgresql.sql`, `continuum-mysql.sql`) or be generated from the
   dialect? Separate files are dumber and more reviewable; generation cannot
   drift.
4. Does accent belong in `continuum-jdbc` as a hard dependency, or optional with
   detection degrading to "you must supply a dialect"? Zero transitives makes
   hard cheap, but optional keeps the module's dependency count at zero.

## Note on how the accent facts above were gathered

The capability table was produced by constructing each `Platform` arm and calling
`supportsSkipLocked()` — running the code, not reading it.

An earlier attempt parsed `Platform.java` with a regex and reported that
`SqlServer` returns `true`, contradicting accent's own javadoc, and that `Db2`
returns `true`. Both were wrong: `record SqlServer(Version version) implements
Platform {}` has an empty body and inherits `false`, and the regex ran past the
empty body to attribute the *next* arm's override to it.

Worth recording because the mis-parse produced clean, plausible, well-formed
output. Prefer executing over parsing when the answer is a value the code can
just tell you.
