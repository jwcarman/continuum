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
`attemptCount`, `submittedAt`, the expired deadline, and `observedAt` (when
Continuum saw the lapse). Attempt count is the **only** retry state Continuum
persists; limits and backoff are policy computed inside the retry from durable
inputs.

## Waiting indefinitely, and giving up on a wall clock

Deadlines are mandatory — every computation has one, and that is what makes
"one computation, one **eventual** outcome" true. A computation that could
never expire would leave its continuations waiting forever, and the guarantee
would quietly stop being a guarantee.

That does not mean you must know, at submission time, how long the work will
take. For an open-ended wait — a human approval that may sit for days — model
the kind as **retryable** and let the retry decide, on each lapse, whether to
keep waiting. The deadline becomes a *dead-man's switch* rather than a
prediction, and the retry carries the policy:

```java
Retry<ApprovalRequest> approvals = (request, ctx) ->
    ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
        ? RetryResult.notRetried("no response within 7 days")
        : RetryResult.retried();                      // keep waiting one more deadline
```

Give the kind a short-ish deadline (say a day). Each lapse re-asks the
question; `retried()` extends by another deadline, and the computation waits
as long as it legitimately needs to. When the rule finally says stop, the
outcome is `Expired(RETRY_EXHAUSTED, "no response within 7 days")` — an
explicit, attributable decision carrying its reason, delivered through the
normal path, rather than silence.

Use **`ctx.elapsedTime()`** for the give-up rule, not arithmetic of your own:

- It measures from `submittedAt` to `observedAt` on Continuum's own
  `InstantSource`. Calling `Instant.now()` inside a retry reads the wall clock
  instead, which diverges from a `MutableInstantSource` and makes tests of
  your own give-up rule lie.
- Every context in a single pump run shares one `observedAt`, so a batch
  cannot disagree with itself about what time it is.
- Deriving elapsed time from `attemptCount × timeout` is wrong the moment
  attempts carry differing timeouts — which `retried(Duration)` explicitly
  allows.

The dead-man's switch is the point, with its scope stated precisely: it fires
when **the thing you are waiting on disappears** — the approver who never
answers, the worker that never reports. The deadline still lapses, the retry
still gets asked, and the continuation still hears back. An unbounded deadline
could not do that.

It does *not* protect you against the pump dying, because the renewal decision
runs inside the pump. No reaper means neither renewal nor expiry, which is the
same standstill an unbounded deadline would give you. Pump liveness is your
concern either way — see [Pumping](../guides/pumping.md).

## Open-ended waits without a dispatch payload

The example above uses a `Retry`, which means a retryable kind and therefore a
dispatch payload. But the work that most often waits open-endedly — a human
approval — is exactly the work that must **never** be redispatched. Minting a
three-type client and inventing a dummy breadcrumb to satisfy `create` would
throw away the guarantee this page opens with: the client's shape is the
declaration. A kind carrying a breadcrumb it must never use is safe only by
convention, and the next edit that adds a `.handler(...)` executes the side
effect twice.

So the two-type client's expiry pump takes the same kind of decision, without
any dispatch payload:

```java
approvals.failExpiredComputations(BatchSize.of(50), ctx ->
    ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
        ? ExpiryResult.expired("no response within 7 days")
        : ExpiryResult.extended());          // keep waiting one more deadline
```

`extended()` waits another client deadline, `extended(Duration)` waits exactly
that long, and `expired(reason)` terminalizes as
`Expired(RETRY_DISALLOWED, reason)`. Extending never increments the attempt
count — nothing was dispatched. A throwing policy leaves the computation
untouched for the next pump.

Both pumps receive the same `ExpiryContext`, so a give-up rule reads
identically whether or not the kind is retryable. The only difference is that
a `Retry` also redispatches.

The no-argument `failExpiredComputations(batchSize)` remains the unconditional
form, expiring everything overdue with `expired after 2 days, 3 hours`.

Implementing `Retry<D>` directly is the escape hatch for decisions the
customizer cannot express (attempt-dependent backoff, circuit breakers):
return `retried()` (extend by the client's deadline), `retried(Duration)`
(explicit timeout), or `notRetried(reason)` (terminalize as
`Expired(RETRY_EXHAUSTED, reason)`).

Timeout-paced retries are self-throttling: attempt N+1 cannot occur until
attempt N's full timeout has elapsed, so no separate retry-backoff mechanism
exists.

## Non-retryable expiry

The two-type client's reap takes no `Retry` — nothing can be redispatched, so
every expiry is `Expired(RETRY_DISALLOWED, ...)`. The no-argument form expires
every overdue computation unconditionally; the `Expiry` overload above lets the
wait continue instead. Either way the expiry follows the exact same delivery
path as any other outcome.

## At-least-once redispatch

Two nodes reaping concurrently may both request a redispatch. That is
deliberate — exactly-once would require the distributed coordination Continuum
refuses to fake. The idempotency key riding in the dispatch payload is what
makes the duplicate harmless.
