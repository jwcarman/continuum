# Spring Boot

Add the starter:

```xml
<dependency>
    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-spring-boot-starter</artifactId>
    <version>0.3.0</version>
</dependency>
```

A `Continuum` bean is auto-configured. Repository selection:

1. An application-defined `ContinuumRepository` bean always wins.
2. With `continuum-jdbc` on the classpath **and** a `DataSource` bean,
   you get durable persistence on a certified platform — PostgreSQL 9.5+,
   MySQL 8+, or MariaDB 10.6+ (`JdbcContinuumRepository`).
   Ordering against Boot's own `DataSourceAutoConfiguration` is handled, so
   a Boot-auto-configured DataSource counts.
3. Otherwise the starter falls back to the **in-memory repository and logs a
   warning** — computations will not survive restarts. Fine for tests; not
   for production.

An application-defined `InstantSource` bean is honored (handy for
deterministic tests); otherwise the system clock is used.

## Transactions

**Continuum manages its own transactions and does not join yours.**
`JdbcContinuumRepository` calls `dataSource.getConnection()` directly, so it
never consults Spring's `TransactionSynchronizationManager`. Inside an
`@Transactional` method it checks out a **second, unrelated** pooled connection
and commits on it independently.

The consequence is worth stating plainly: if your surrounding transaction rolls
back, Continuum's write has already committed and **stays committed**.

```java
@Transactional
public void schedule(Order order) {
    orders.save(order);                     // your transaction
    toolCalls.create(...);                  // a DIFFERENT transaction; commits immediately
    throw new IllegalStateException();      // rolls back the save — NOT the create
}
```

Each SPI operation is atomic in itself — that guarantee is real and TCK-certified
— but it is atomic *alone*, never jointly with your work. Two rules follow:

- **Don't rely on `@Transactional` to undo Continuum calls.** If a computation
  must not outlive a failed unit of work, complete or expire it explicitly on
  the failure path.
- **Never call the pumps from inside `@Transactional`.** `claimDeliveries` uses
  `FOR UPDATE SKIP LOCKED` on rows your ambient transaction may also hold, which
  risks lock contention, and a lease only means anything once committed.
  Scheduled pump methods should have no ambient transaction at all.

### Signalling a failed delivery

Inside `deliverResults`, **throw** — that is the failure signal. Continuum
catches any `RuntimeException` from your consumer, logs a warning, and releases
the delivery with its attempt count incremented and its next availability pushed
out by the call-site backoff. The delivery is redelivered later; it is never
lost, and one failure never aborts the rest of the batch. Returning normally is
what acknowledges a delivery, and acknowledged deliveries are deleted.

```java
toolCalls.deliverResults(BatchSize.of(25), delivery -> {
    backlog.record(delivery.continuation(), delivery.outcome());  // throwing releases for retry
});
```

Because the acknowledgment is a separate transaction from whatever your consumer
did, the two cannot be atomic: a crash between them redelivers. This is why
**consumers must be idempotent** — `ContinuationId` is the stable deduplication
key. See [Delivery](../concepts/delivery.md).

Note that nothing caps the retries: a delivery that always throws is retried
forever, paced by the backoff. Continuum will not give up on your behalf, but
the delivery tells you how many attempts have been made, so you can:

```java
toolCalls.deliverResults(BatchSize.of(25), delivery -> {
    if (delivery.deliveryAttempt() >= 10) {
        deadLetter.record(delivery.continuationId(), delivery.outcome());
        return;                                  // returning acknowledges: stop redelivering
    }
    backlog.record(delivery.continuation(), delivery.outcome());
});
```

`deliveryAttempt()` counts *delivery* attempts, and is distinct from
`Computation.attemptCount()`, which counts *dispatch* attempts — redelivering
an outcome is not re-running the work.

Joining a caller-managed transaction is a contemplated future direction, not a
current capability. Design accordingly.

## What stays yours

- **Clients** are application `@Bean`s — they carry your kinds, types, and
  codecs. (With `codec-autoconfigure` on the classpath, inject the
  `CodecFactory` bean into your client definitions.)
- **Pumping** is your `@Scheduled` methods, per kind, per cadence — see
  [Pumping & Scheduling](pumping.md).
- **Schema** is your migration discipline — see
  [Persistence Providers](persistence.md). The starter never executes DDL.
