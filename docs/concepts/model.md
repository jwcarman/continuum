# The Computation Model

A computation is a **durable, memoized eventual value**. It has a globally
unique `ComputationId`, belongs to a named `ComputationKind`, begins pending
with at least one continuation registered, and eventually resolves to exactly
one terminal outcome.

```text
CREATE ──▶ PENDING ──(complete / expire)──▶ TERMINAL
                                              │
                                              ├─▶ delivery to continuation A
                                              ├─▶ delivery to continuation B
                                              └─▶ late registrants get the
                                                  memoized outcome immediately
```

## Presence means pending

The persistence model stores a computation row **only while it is pending**.
Every terminalization — success, producer-reported failure, or expiry — runs
the same atomic ownership transfer: the pending row is deleted, the outcome is
written to a result record, and one outbox delivery per registered
continuation is materialized, all in one transaction.

Consequences:

- **No stored status anywhere.** A computation's status is *derived*: pending
  table → `PENDING`; result record → `COMPLETED`/`FAILED`/`EXPIRED` (a 1:1
  reading of the outcome). Delivery state is likewise derived from lease
  fields and row presence. Every "status" is a reading of durable facts,
  never a mutable label.
- **Hot paths stay hot.** The pending table's size is the in-flight count.

## Three-arm outcomes

```java
sealed interface Outcome {
    record Success(byte[] payload) {}
    record Failure(String message) {}              // the producer said "no"
    record Expired(ExpiryKind kind, String message) {}  // no answer before the deadline
}
```

A producer reporting failure and a deadline lapsing with no answer are
different facts — a known "no" versus "never heard back" — and consumers
switch on exactly that three-way distinction. `Expired` is minted only by
timeout processing; `Continuum.complete()` rejects it, so a producer cannot
forge an expiry. `ExpiryKind` tells you which reap path did it:
`RETRY_DISALLOWED` (the kind was never retryable) or `RETRY_EXHAUSTED`
(retrying was possible and gave up).

## Memoized results

Terminal outcomes are retained in the result record so that:

- a **late registration** returns `Resolved(outcome)` immediately instead of
  falling into a gap,
- a **duplicate or slow `complete()`** answers `ALREADY_RESOLVED` — a producer
  finishing *after* expiry learns it lost the race, and can never replace the
  sealed outcome.

Retention is bounded by [purging](../guides/pumping.md#purging): after a
result is purged, the computation behaves as never known.

## Registration race safety

Registration is atomic with respect to completion. A caller registering
interest receives exactly one of *durable registration* or *the
already-resolved outcome* — never neither. If registration wins the race, the
continuation is guaranteed a delivery; if completion wins, the memoized
outcome comes back and nothing is persisted.

## Payloads are opaque bytes

The outcome payload, the continuation payload ("what to do with the result"),
and the dispatch payload ("how to restart the work") are all opaque `byte[]`
at the storage and coordination layer. Serialization is the caller's concern —
the [typed clients](../guides/getting-started.md) put codecs over this
boundary via [codec](https://github.com/jwcarman/codec).
