# Delivery

Every terminalization fans out one **outbox delivery** per registered
continuation. Deliveries are self-contained: each carries the computation id,
kind, continuation id, continuation payload, and the outcome — a consumer
needs nothing else to act.

## One channel for every outcome

Success, producer-reported failure, and expiry all arrive through the same
`deliverResults` consumer. A continuation is guaranteed **exactly one eventual
delivery, whatever happened** — there is no separate timeout-notification
channel to subscribe to, miss, or handle inconsistently.

```java
toolCalls.deliverResults(BatchSize.of(25), delivery -> {
    var continuation = delivery.continuation();
    switch (delivery.outcome()) {
        case TypedOutcome.Success<ToolCallResult>(var result) -> backlog.recordResult(continuation, result);
        case TypedOutcome.Failure<ToolCallResult>(var message) -> backlog.recordFailure(continuation, message);
        case TypedOutcome.Expired<ToolCallResult>(var kind, var message) -> backlog.recordTimeout(continuation, kind);
    }
});
```

## Competing consumers and leases

Any number of workers may pump deliveries concurrently. Claiming uses
`FOR UPDATE SKIP LOCKED` (PostgreSQL), so claimers never block each other, and
each claimed delivery is held under a **lease**. If the worker dies
mid-processing, the lease lapses and another worker reclaims the delivery —
crash recovery with nothing to repair. The lease must exceed your worst-case
consumer time, or another node may reclaim mid-processing (legal, but noisy).

The outbox row's `claimed_by` column records `pid@hostname` of the holder, so
an operator staring at the table can identify a stuck worker.

## At-least-once, and what to do about it

A consumer may see the same delivery more than once — a crash after processing
but before acknowledgment redelivers. **Consumers must be idempotent**;
`ContinuationId` is the stable deduplication key.

A consumer that throws does not lose the delivery: it is released with its
attempt count incremented and its availability pushed back by the call-site
**backoff**, so a poison delivery paces its retries instead of hot-looping.
One delivery's failure never blocks the others in a batch.

Acknowledged deliveries are **deleted** — the outbox holds active obligations
only. A row existing means a delivery is still owed; absence means Continuum
no longer owns it.
