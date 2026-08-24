# Pumping & Scheduling

Continuum owns **no threads and no schedulers**. Three recurring activities
are batch methods on the client; your application calls them on whatever
cadence and machinery it likes. No correctness property depends on a pump
running — if one never runs, deliveries wait, expired computations sit
pending, results accumulate. Liveness degrades; correctness never does.

```java
scheduler.scheduleWithFixedDelay(() ->
    toolCalls.deliverResults(BatchSize.of(25), (continuation, outcome) -> switch (outcome) {
        case TypedOutcome.Success<ToolCallResult>(var result) -> backlog.recordResult(continuation, result);
        case TypedOutcome.Failure<ToolCallResult>(var message) -> backlog.recordFailure(continuation, message);
        case TypedOutcome.Expired<ToolCallResult>(var kind, var message) -> backlog.recordTimeout(continuation, kind);
    }), 0, 1, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.retryExpiredComputations(BatchSize.of(12), Retry.of(r -> r
        .atMost(3)
        .handler((descriptor, ctx) -> toolRuntime.dispatch(descriptor, ctx.computationId())))),
    5, 15, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.purgeExpiredResults(BatchSize.of(200), ResultTtl.ofHours(1)),
    1, 10, TimeUnit.MINUTES);
```

Spring users: the same three calls as `@Scheduled` methods. Fixed-*delay* is
the sensible default so a slow batch can't stack overlapping runs on one node
— though overlap is safe regardless.

## Value-typed parameters

Pump parameters are small value types — `BatchSize.of(25)`,
`Lease.ofSeconds(45)`, `Backoff.ofSeconds(10)`, `ResultTtl.ofHours(1)` — so
adjacent durations can't be transposed and invariants (positive, at least 1)
are enforced at construction.

## Delivering

`deliverResults(batchSize, consumer)` claims up to a batch of this kind's
outbox deliveries under a 30-second lease, decodes each, and invokes your
consumer. Success acknowledges (deletes) the delivery; an exception releases
it with a 30-second backoff and an incremented attempt count. Override both
per call site: `deliverResults(batchSize, lease, backoff, consumer)` — the
lease must exceed your worst-case consumer time.

Every call returns the count processed — the drain signal:
`while (client.deliverResults(...) > 0)` chews through a backlog after
downtime, batch by batch.

## Expiring

Each client shape has exactly one expiry pump — the verb tells you what
expiry means for that kind. `retryExpiredComputations` (on
`RetryableContinuumClient`) consults the supplied
[`Retry`](../concepts/retry.md) for every overdue computation;
`failExpiredComputations` (on `ContinuumClient`) expires them
unconditionally as `RETRY_DISALLOWED`. Each pump handles one bounded batch —
after an outage, redispatch is paced instead of thundering.

## Purging

`purgeExpiredResults(batchSize, ttl)` deletes this kind's memoized results
older than the call-site TTL. Retention policy is code at the pump site, like
retry policy — there is no TTL column. Results only need to outlive the last
plausible late registrant or slow producer; they are a coordination memo, not
an audit log. After purge, the computation behaves as never known.

## Operational properties

- **Every app instance can run every pump identically** — no leader election.
  Leases and `SKIP LOCKED` make overlapping pumps correct, merely
  occasionally redundant.
- **Crash anywhere leaves nothing to repair** — claimed deliveries reappear on
  lease expiry, half-reaped batches are re-found, purges are idempotent.
- **Per-kind cadence** falls out naturally: each client pumps its own kind, so
  tool results can pump every second while approvals pump every minute.
