# MongoDB persistence: `continuum-mongo`

**Status:** design, approved in conversation; awaiting written review
**Date:** 2026-08-25
**Target:** 0.5.0

---

## Why

`continuum-jdbc` covers the five mainstream SQL engines. The model itself is
not SQL-shaped: the repository SPI needs an atomic ownership transfer, a
conditional claim with a lease, and ordered scans. A feasibility spike on
2026-08-25 ported the ten-operation SPI to MongoDB one-to-one and passed the
full TCK — 26/26, concurrency suites included — on the first real run, in
5.6 seconds. MongoDB is the one NoSQL store with both the install base and the
transactional guarantees the model requires; this spec turns the spike into a
shippable module.

## Decisions (each settled in conversation)

| Decision | Choice | Alternative rejected |
|---|---|---|
| Client contract | `MongoContinuumRepository(MongoClient, String databaseName)` | `MongoDatabase` alone — the driver offers no way back to the client, and sessions need it |
| Driver | `org.mongodb:mongodb-driver-sync`, plain `Document` API | Spring Data `MongoTemplate` — drags Spring Data into a driver-only module |
| Timestamps | BSON `date`, millisecond precision, documented per platform | int64 epoch-micros — exact JDBC parity, opaque numbers in the database |
| Topology | replica set required; standalone refused on first use | single-document design (computation + outbox embedded) — runs on standalone, but the claim becomes per-computation |
| Indexes | `ensureIndexes()` public and idempotent; Boot calls it by default | auto-create on first use — violates "no side effects before first operation is invited" |
| Boot selection | `continuum.persistence.type` = `jdbc` \| `mongo` \| `memory`; absent → auto-detect; ambiguous → fail naming the property | first-in-order wins (Spring Cache style) — silently guessing which store holds durable state |

## Module

`org.jwcarman.continuum:continuum-mongo`. Dependencies: `continuum-core` and
`mongodb-driver-sync` (compile). Test: `continuum-testing`, Testcontainers
`mongodb`. Added to `continuum-bom` and, as an optional dependency, to
`continuum-autoconfigure`, mirroring `continuum-jdbc` — the starter declares
neither.

## Storage

Four collections named as the JDBC tables. Field names are camelCase; the
timestamp vocabulary is unchanged (`submittedAt`, `completedAt`, `createdAt`
for row bookkeeping only).

| Collection | `_id` | Fields |
|---|---|---|
| `continuum_computation` | computation id | `kind`, `deadlineAt`, `dispatchPayload` (binary, nullable), `attemptCount`, `submittedAt`, `lastUpdatedAt` |
| `continuum_continuation` | continuation id | `computationId`, `payload` (binary), `createdAt` |
| `continuum_result` | computation id | `kind`, `outcome`, `deadlineAt`, `attemptCount`, `submittedAt`, `completedAt` |
| `continuum_outbox` | delivery id | `computationId`, `continuationId`, `kind`, `continuationPayload`, `outcome`, `availableAt`, `claimedBy`, `claimedUntil`, `attemptCount`, `createdAt`, `submittedAt`, `completedAt` |

Identities are UUIDv7 stored as their canonical 36-character string — the same
representation the string-keyed JDBC dialects use, so time-ordering survives.

`outcome` is an embedded document: `{type: SUCCESS|FAILURE|EXPIRED, payload?,
expiryKind?, message?}` — the four JDBC outcome columns folded into one value.

Instants are BSON `date` (UTC epoch millis). The module never touches
`java.util.Date`: the driver's default registry encodes `Instant` directly, and
the collections are opened with a `DocumentCodecProvider` whose
`BsonTypeClassMap` maps `DATE_TIME → Instant`, so reads return `Instant` too.
Sub-millisecond precision is truncated on write; the persistence guide states
this alongside the JDBC platforms' microsecond precision.

### Indexes

`ensureIndexes()` creates, idempotently:

- `continuum_computation {kind: 1, deadlineAt: 1}` — `findExpired`
- `continuum_continuation {computationId: 1}` — fan-out on `complete`
- `continuum_result {kind: 1, completedAt: 1}` — `purgeResults`
- `continuum_outbox {kind: 1, availableAt: 1}` — `claimDeliveries`

The repository never calls it on its own. Plain-Java users call it once at
startup; the Boot auto-configuration calls it by default. The list is
documented in the persistence guide as the reference "schema".

## Semantics

Every operation that touches more than one document runs in
`ClientSession.withTransaction`, which retries `TransientTransactionError`
(write conflicts) and `UnknownTransactionCommitResult` for the driver's
120-second window. Single-document operations run without a session.

- **`createComputation`** — transaction: refuse if a result with this id
  exists; insert the computation and the initial continuation. Duplicate `_id`
  (error 11000) → `ContinuumPersistenceException`, the analogue of a primary
  key violation.
- **`registerContinuation`** — transaction: `findOneAndUpdate` the pending
  document (setting `lastUpdatedAt`). This is the pending-row lock in Mongo
  form: the update takes a write intent on the document, so a concurrent
  `complete()` deleting it conflicts and one side retries. Present → insert
  the continuation, `Registered`. Absent → result present → `Resolved(outcome)`;
  otherwise `NotFound`.
- **`complete`** — transaction: `findOneAndDelete` the pending document. Absent
  → `ALREADY_RESOLVED` if a result exists, else `NOT_FOUND`. Present → insert
  the result, insert one outbox document per continuation (`insertMany`),
  delete the continuations, `COMPLETED`.
- **`findComputation`** — pending document → `PENDING`; else result →
  terminal `Computation` (status from the outcome, no dispatch payload); else
  empty.
- **`claimDeliveries`** — up to `limit` iterations of `findOneAndUpdate` with
  filter `{kind, availableAt ≤ now, claimedUntil null or ≤ now}`, sort
  `availableAt`, update `{claimedBy, claimedUntil: now + lease}`. Each call is
  a single-document compare-and-set: whichever claimer's update matches first
  owns the document, and the others' filters no longer match — `SKIP LOCKED`
  by CAS, with no locking clause. One round-trip per claimed delivery; at pump
  batch sizes this is immaterial, and the guide says so.
- **`acknowledgeDelivery`** — `deleteOne`.
- **`releaseDelivery`** — `updateOne`: clear the claim, set `availableAt` to
  `retryAt`, increment `attemptCount`.
- **`findExpired`** — `find {kind, deadlineAt ≤ now}`, sort `deadlineAt`,
  limit.
- **`extendDeadline`** — `updateOne`: `deadlineAt`, `attemptCount`,
  `lastUpdatedAt`.
- **`purgeResults`** — find ids `{kind, completedAt < olderThan}` sorted by
  `completedAt` (oldest first), limit; `deleteMany` by id; return the count.

Bookkeeping timestamps follow `JdbcContinuumRepository` exactly: `createdAt`
and the initial `lastUpdatedAt` derive from the instants the SPI already
passes (`submittedAt` at creation, `completedAt` for outbox documents), and
the updates in `registerContinuation` and `extendDeadline` stamp
`lastUpdatedAt` with the server clock via `$currentDate` — the analogue of
JDBC's `CURRENT_TIMESTAMP`. The repository never consults a clock of its own.

## Guard

On first use — never at construction; wiring a bean opens no connection — the
repository runs `hello` and `buildInfo` once and refuses, with the fix in the
message, anything that cannot meet the TCK:

| Detected | Message (shape) |
|---|---|
| standalone (`hello` has no `setName` and `msg` is not `isdbgrid`) | `MongoDB 8.2 standalone — continuum needs a replica set; a single node started with --replSet is enough` |
| MongoDB < 5.0 | `MongoDB 4.4.29 — continuum needs 5.0+` |
| Amazon DocumentDB, Azure Cosmos DB (Mongo API), FerretDB — identified from `buildInfo`/`hello` | `Amazon DocumentDB 5.0 (reports as MongoDB 5.0.0) — not certified; see the persistence guide` |

Impostor-by-host checks run before the version floor, so a Cosmos instance
reporting 4.2 is named as Cosmos, not as old MongoDB.

The message ends with the `assumeMongoDb` escape hatch, the analogue of
`assumePostgreSql`. The floor is 5.0 because transactions on sharded clusters,
the `hello` command and the driver's own support floor all land there, and
4.x is end-of-life.

The guard is unit-tested with a fake `MongoClient`/`MongoDatabase` returning
scripted `hello`/`buildInfo` documents, the way `PlatformGuardTest` mocks
`DatabaseMetaData`; the refusals are never exercised against containers.

## Certification

`MongoContinuumTckIT` extends `ContinuumTck` against `mongo:8.2` (Testcontainers
`MongoDBContainer`, which initiates a single-node replica set) through the
detecting constructor, so the guard's admission is under test on every build
alongside the eight JDBC configurations. The spike's
`MongoCertificationExperiment` is superseded and not kept. `mongo:8.0` is
avoided deliberately: it refuses to start on Linux kernels ≥ 6.19
(SERVER-121912), which Docker Desktop currently ships.

## Spring Boot

`MongoContinuumAutoConfiguration`, conditional on `MongoContinuumRepository`
on the classpath and a `MongoClient` bean, registered after Boot's Mongo
auto-configuration and before `ContinuumAutoConfiguration` (the in-memory
fallback), as `JdbcContinuumAutoConfiguration` is today.

Properties:

| Property | Default | Meaning |
|---|---|---|
| `continuum.persistence.type` | auto-detect | `jdbc`, `mongo` or `memory`. With exactly one persistence module present it is unnecessary. With both `continuum-jdbc` and `continuum-mongo` present and both a `DataSource` and a `MongoClient` bean, startup fails naming this property and both candidates — the Spring Session `store-type` precedent. |
| `continuum.mongo.database` | `spring.mongodb.database` (Boot 4), then `spring.data.mongodb.database` (Boot 3), then `test` | database name |
| `continuum.mongo.ensure-indexes` | `true` | call `ensureIndexes()` at startup |

## Documentation

- `docs/guides/persistence.md`: a MongoDB section — the replica-set
  requirement and why (standalone has no multi-document transactions), the
  version floor, millisecond precision, the index list, the refused impostors,
  the claim's per-document round-trip.
- `docs/guides/spring-boot.md`: the three properties and the selection rule.
- `README.md` module table; `CHANGELOG.md` Unreleased entry.

## Out of scope

- Amazon DocumentDB, Cosmos DB and FerretDB certification. Refused until run
  through the TCK, exactly as CockroachDB and YugabyteDB are on JDBC.
- A single-document design for standalone servers.
- TTL indexes for result expiry: purge stays explicit and application-pumped
  on every store.
