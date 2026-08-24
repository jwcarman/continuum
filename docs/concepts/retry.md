# Expiry & Retry

Every computation has a deadline. Timeout detection is durable — it never
depends on an in-memory timer surviving — and is driven by pumping the expiry
methods (see [Pumping](../guides/pumping.md)).

## Retryability is data

A computation with a **dispatch payload** is retryable; one without is not.
There is no flag to keep in sync: no breadcrumb, nothing to redispatch,
non-retryable by construction. At the typed layer the client's *shape* is the
declaration — the three-type `RetryableContinuumClient<R, C, D>` requires a
dispatch object at `create`; the two-type `ContinuumClient<R, C>` cannot carry
one.

## The dispatch payload is a write-once breadcrumb

It means "how to (re)dispatch this work," is persisted atomically with the
computation at create, never mutated, never interpreted by Continuum, and is
handed back verbatim on every timeout. **Embed your external idempotency key
in it** — because the payload cannot change between attempts, the key cannot
drift, and the external system can safely deduplicate re-executions.

## The Retry object

Reaping a retryable kind requires a `Retry<D>` — one object that *performs*
(or schedules) the redispatch itself and reports what it did:

```java
Retry.of(r -> r
    .atMost(3)                                    // total attempts; the original dispatch is attempt 1
    .handler((toolCall, ctx) ->                   // only dispatches — decisions are derived
        toolRuntime.dispatch(toolCall, ctx.computationId())));
```

The context carries Continuum's durable facts — `computationId` (the
redispatched worker must know where to `complete()` to), `kind`,
`attemptCount`, and the expired deadline. Attempt count is the **only** retry
state Continuum persists; limits and backoff are policy computed inside the
retry from durable inputs.

Implementing `Retry<D>` directly is the escape hatch for decisions the
customizer cannot express (attempt-dependent backoff, circuit breakers):
return `retried()` (extend by the client's deadline), `retried(Duration)`
(explicit timeout), or `notRetried(reason)` (terminalize as
`Expired(RETRY_EXHAUSTED, reason)`).

Timeout-paced retries are self-throttling: attempt N+1 cannot occur until
attempt N's full timeout has elapsed, so no separate retry-backoff mechanism
exists.

## Non-retryable expiry

The two-type client's reap takes no `Retry` — every overdue computation
terminalizes as `Expired(RETRY_DISALLOWED, "deadline ... passed")`,
unconditionally. The expiry then follows the exact same delivery path as any
other outcome.

## At-least-once redispatch

Two nodes reaping concurrently may both request a redispatch. That is
deliberate — exactly-once would require the distributed coordination Continuum
refuses to fake. The idempotency key riding in the dispatch payload is what
makes the duplicate harmless.
