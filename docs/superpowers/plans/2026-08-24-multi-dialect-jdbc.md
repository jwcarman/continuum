# Multi-dialect JDBC support via accent

**Status:** plan — accent 0.1.0 is released; phases 0–1 are actionable
**Date:** 2026-08-24 (revised same day: accent released, sequencing corrected)
**Target:** phase 0 in 0.4.0

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

Measured by running the code rather than reading it — first against
0.1.0-SNAPSHOT, then re-verified against the released 0.1.0 jar fetched from
Central (same table). The release also added `EngineVersion` to the
Postgres-impostor arms, so `CockroachDB` carries both the wire-compat version
pgjdbc reports (13.0) and the real engine version (v24.1) as data:

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

## Sequencing (corrected)

An earlier revision of this note said to do the fan-out ceiling first. That was
wrong, and is corrected here: the `TransactWriteItems` 100-item cap is a
document-store property. PostgreSQL has no per-transaction statement cap, and no
non-SQL provider exists or is planned. The fan-out item is reduced to a
pre-1.0 contract-wording decision (mark the transaction-size/continuation-count
coupling provisional in the SPI javadoc; decide the final wording before 1.0)
and does not gate anything below.

## The plan, phased

### Phase 0 — the guard (0.4.0): use accent to close the hole we documented

The first value is not new databases. `persistence.md` currently warns that
CockroachDB/YugabyteDB report as PostgreSQL and are silently accepted — connect,
parse, run, no warning anywhere. accent is precisely the `SELECT version()`
guard that closes it, and someone else has now built and contention-tested it.

- Add `org.jwcarman.accent:accent:0.1.0` to `continuum-jdbc` (zero transitives;
  verified on the released pom, scoped to the top-level dependencies block).
- `JdbcContinuumRepository(DataSource)` detects at construction:
  - `Platform.PostgreSQL` with `supportsSkipLocked()` → proceed (also catches
    PostgreSQL < 9.5, which today would fail confusingly at claim time).
  - Anything else → refuse loudly, naming what was detected — including the
    engine version for impostors, which accent now carries as data:
    "detected CockroachDB v24.1 (reports as PostgreSQL 13.0); certified
    platforms: PostgreSQL 9.5+".
- Escape hatch for the operator who knows better: a second constructor that
  skips detection. Today that can simply be the existing behavior behind an
  explicit opt-in; in phase 1 it becomes "supply your own dialect".
- `persistence.md`'s warning gains a sentence: since 0.4.0 the provider refuses
  wire-compatible impostors at construction rather than silently accepting them.

This phase changes no SQL and adds no dialect machinery. It converts a
documented silent failure into a loud one.

### Phase 1 — the dialect seam (with or after phase 0)

Extract `ContinuumDialect` (claim SQL + binary column type, nothing else) and
make the Postgres dialect the only implementation. Pure refactor, TCK stays
green, public behavior unchanged. `Platform` selects the dialect; the
explicit-dialect constructor becomes the escape hatch.

### Phase 2 — certify CockroachDB and YugabyteDB (first real widening)

> **OUTCOME (2026-08-25): CockroachDB FAILS certification. The guard stays shut,
> now with evidence.** Six of six runs failed, ~300 races total, in two distinct
> modes:
>
> 1. **Loud (common):** SQLSTATE 40001 `RETRY_SERIALIZABLE` — CockroachDB aborts
>    one side of a contended register-vs-complete pair and requires the client to
>    retry; `inTransaction` does not, so it surfaces as
>    `ContinuumPersistenceException`. Roughly one or two per 50-race battery.
> 2. **Silent (disqualifying):** observed twice — both transactions committed,
>    `registerContinuation` returned `Registered`, and the delivery was never
>    created. No error anywhere. The invariant "Registered means a delivery will
>    exist" simply did not hold. A retry-on-40001 wrapper would fix mode 1 and do
>    nothing for this.
>
> Mechanism consistent with the evidence: both transactions open with
> `SELECT ... FOR UPDATE` on the same computation row, which on PostgreSQL
> serialises the pair entirely. The observed interleaving requires that mutual
> exclusion to have failed at least once — consistent with CockroachDB's
> best-effort/unreplicated `FOR UPDATE` locks. Not fully diagnosed at the
> engine level, and does not need to be: the TCK asserts observable contract,
> and the contract observably broke.
>
> Note what this does NOT contradict: accent's `supportsSkipLocked() = true`
> for CockroachDB stands — the Claiming and Racing suites passed every run.
> What failed is a different property (plain `FOR UPDATE` mutual exclusion
> composing with a predicate read inside the ownership transfer), which is
> exactly why the capability predicate is narrow and the TCK is the gate.
>
> **YugabyteDB (2026-08-25): fails certification too, but differently — and the
> difference matters.** Six of six runs, ~300 races: always exactly one error,
> always the same test (`register_vs_complete_race`), always the loud form —
> `Restart read required`, YugabyteDB's client-must-retry signal, surfacing as
> `ContinuumPersistenceException` because `inTransaction` does not retry.
> **Zero silent violations observed.**
>
> So the two exclusions are different in kind. YugabyteDB fails in a way that
> is in principle fixable from continuum's side: an `inTransaction` retry on
> serialization-failure SQLSTATEs is a contained change that could make it
> certifiable in a later phase. CockroachDB's silent mode cannot be fixed by
> anything continuum does, because continuum never learns it happened —
> "not supported yet, with a path" versus "not supportable on this evidence."
>
> The experiments live on as `CockroachCertificationExperiment` /
> `YugabyteCertificationExperiment` — deliberately not named `*IT`, so they
> never run in a default build (the answer is a failure; CI would be
> permanently red). Run on demand:
> `mvn -pl continuum-jdbc verify -Dit.test=CockroachCertificationExperiment`.


accent measured both honouring skip-locked under contention; the TCK is the
gate that turns that into a supported claim. Run `JdbcContinuumTckIT` against
`cockroachdb/cockroach` and `yugabytedb/yugabyte` Testcontainers. If green:
same dialect as Postgres, guard widened to admit them, `persistence.md` hedge
replaced with a supported-platforms table. If red: the guard keeps refusing,
and the doc gains an evidence-backed exclusion instead of a hedge. Either
outcome is strictly better than today.

Prediction to test, not assume: both pass with the unmodified Postgres dialect.
Cockroach has no `pg_advisory_lock`, but continuum uses none; the schema types
all exist there.

### Phase 3 — MySQL / MariaDB (first real dialect work)

> **OUTCOME (2026-08-25): all three suites certified — MySQL 8.4, MariaDB 11.4
> native, and MariaDB via mysql-connector-j. 104 TCK tests green across four
> platforms including PostgreSQL.** The concurrency heart passed on the first
> real attempt; the only failures en route were one comment-shearing bug in the
> test schema helper and one genuinely non-portable statement — the purge's
> `IN (SELECT ... LIMIT ?)`, rewritten as a derived table both families accept.
> The dialect seam ended up smaller than planned: UUID binding plus a DDL
> resource pointer, zero SQL methods.


First genuine type mapping: `continuum-mysql.sql` reference DDL (`BINARY(16)`
or `CHAR(36)` for UUID, `LONGBLOB`, `TIMESTAMP(6)`), a second
`ContinuumDialect`, TCK against both images and both drivers — the
mysql-connector-j-against-MariaDB pairing included, since accent exists
precisely because that pairing lies about identity.

### Unplanned: H2 (2026-08-25) — passes; ADMITTED as test/embedded tier (James, same day)

> Not in the original phases; run because H2 is every Spring Boot shop's test
> database and the guard refuses it by name. **26/26 in PostgreSQL compatibility
> mode AND in default mode**, ~2 seconds each, in-process, no container. The
> only friction: H2 rejects the `TIMESTAMPTZ` alias and wants
> `TIMESTAMP(6) WITH TIME ZONE` — which PostgreSQL also accepts, so the
> reference DDL could serve both verbatim if admission is granted.
>
> Admission is deliberately NOT implemented: certifying that the contract holds
> is this experiment's job; deciding whether an in-memory test database belongs
> on the certified list — with whatever production-blessing that implies — is
> James's. The case for: Boot users could integration-test the real JDBC
> provider (SQL, schema, dialect) without containers, which `continuum-memory`
> cannot offer since it bypasses SQL entirely. The case against: "certified"
> currently connotes production-supportable, and H2 is not that.

### Phase 4 — SQL Server, only if wanted

First genuinely different claim statement (`WITH (UPDLOCK, READPAST)`), which
accent's predicate deliberately does not cover. Do last; skip indefinitely
absent demand.

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
4. ~~Hard or optional dependency?~~ Resolved: hard. accent 0.1.0 is a
   zero-transitive leaf (verified on the released pom), the guard is the point
   of phase 0 and an optional guard guards nothing, and the escape-hatch
   constructor already serves whoever cannot tolerate the startup query.

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
