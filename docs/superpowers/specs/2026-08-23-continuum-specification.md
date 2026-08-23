# Continuum — Durable Computation Specification

## 1. Overview

**Continuum** is a Java library for coordinating durable asynchronous computations across process and machine boundaries.

A Continuum computation represents an operation whose result may not be available until some arbitrary point in the future. The process that creates the computation does not need to remain alive while the computation executes.

A computation:

- has a globally unique identifier,
- belongs to a named **kind** or **lane**,
- begins in a pending state,
- has at least one continuation registered when it is created,
- may acquire additional continuations while pending,
- eventually resolves to exactly one terminal outcome,
- memoizes that outcome,
- durably delivers the outcome to every continuation registered before resolution,
- immediately returns the memoized outcome to callers that attempt to register after resolution.

Continuum does **not** execute the underlying work itself.

The external system performing the work receives the `ComputationId` and eventually reports the outcome:

```java
continuum.complete(computationId, result);
```

Continuum's responsibility is durable coordination of the result.

---

# 2. Goals

Continuum should provide:

- durable computation identity,
- asynchronous completion across process boundaries,
- durable continuation registration,
- multiple continuations for a single computation,
- memoized terminal outcomes,
- race-free registration versus completion,
- durable result delivery,
- competing-consumer delivery processing,
- durable timeout detection,
- retry semantics,
- crash recovery,
- pluggable persistence.

A computation may survive:

- JVM termination,
- process restart,
- machine failure,
- deployment,
- long periods of inactivity.

No correctness guarantee should depend on a live thread, Java object, callback closure, `Future`, or JVM remaining present.

---

# 3. Non-goals

Continuum is not intended to be:

- a workflow engine,
- a durable Java call-stack implementation,
- a DAG execution engine,
- a distributed scheduler,
- a general message broker,
- a pub/sub system,
- a replacement for Temporal or Restate,
- an exactly-once side-effect mechanism.

Version 1 should avoid:

- `allOf`,
- `anyOf`,
- computation graphs,
- child workflows,
- compensation/sagas,
- arbitrary workflow state,
- durable lambdas,
- streaming results.

These capabilities may be composed above Continuum if needed.

---

# 4. Core concept

A computation is a durable, memoized eventual value.

Conceptually:

```text
CREATE
  |
  | computation + initial continuation
  v
PENDING
  |
  | external work occurs somewhere
  |
  | complete(id, outcome)
  v
TERMINAL
  |
  +------> durable delivery to registered continuation A
  |
  +------> durable delivery to registered continuation B
  |
  +------> durable delivery to registered continuation C
```

After terminalization:

```text
registerContinuation(id, D)
              |
              v
       outcome returned
       immediately
```

The computation itself does not execute work.

---

# 5. Computation identity

Every computation has a globally unique `ComputationId`.

Suggested representation:

```java
public record ComputationId(UUID value) {
}
```

The implementation may use UUID, ULID, or another identifier with equivalent global uniqueness properties.

The identifier is opaque.

It must not encode:

- kind,
- tenant,
- application,
- continuation,
- external operation identity.

A result producer needs only the ID to report completion:

```java
continuum.complete(computationId, outcome);
```

The producer does not need to know the computation's kind or its continuations.

---

# 6. Computation kind / lane

Every computation belongs to a named **kind**.

Examples:

```text
tool-result
approval
external-job
report-generation
payment-result
```

Suggested type:

```java
public record ComputationKind(String value) {
}
```

The term **lane** may be used conceptually to describe kinds:

```text
Continuum

tool-result lane
    C1
    C2
    C3

approval lane
    C4
    C5

external-job lane
    C6
```

The API should probably use `ComputationKind` rather than `Lane` because `kind` describes the persisted classification without requiring users to understand the metaphor.

Kinds may be useful for:

- storage partitioning,
- operational visibility,
- metrics,
- recovery workers,
- timeout processing,
- application integration.

A `ComputationId` remains globally unique regardless of kind.

Therefore:

```java
complete(computationId, outcome)
```

does not require the kind.

---

# 7. Computation state

A computation has three logical states:

```java
public enum ComputationStatus {
    PENDING,
    COMPLETED,
    FAILED
}
```

Potential cancellation semantics may be added later.

A terminal computation is immutable.

Valid transitions are:

```text
PENDING -> COMPLETED
PENDING -> FAILED
```

No transition is permitted from a terminal state.

Exactly one terminal outcome may win.

---

# 8. Computation record

Conceptually:

```java
public record Computation(
    ComputationId id,
    ComputationKind kind,
    ComputationStatus status,
    Instant createdAt,
    Instant deadline,
    Outcome outcome
) {
}
```

`outcome` is absent while pending.

The persisted relational representation might resemble:

```text
continuum_computation
---------------------
id              UUID / VARCHAR PRIMARY KEY
kind            VARCHAR NOT NULL
status          VARCHAR NOT NULL
deadline_at     TIMESTAMP
result          BLOB / JSON / JSONB
created_at      TIMESTAMP NOT NULL
completed_at    TIMESTAMP NULL
version         BIGINT NOT NULL
```

The precise physical representation belongs to the persistence implementation.

---

# 9. Outcome

A computation resolves exactly once to an outcome.

Suggested model:

```java
public sealed interface Outcome
    permits Outcome.Success, Outcome.Failure {

    record Success(byte[] payload) implements Outcome {
    }

    record Failure(FailureInfo failure) implements Outcome {
    }
}
```

Suggested failure:

```java
public record FailureInfo(
    FailureKind kind,
    String message
) {
}
```

Potential initial failure kinds:

```java
public enum FailureKind {
    EXECUTION_FAILED,
    TIMEOUT_NON_RETRYABLE,
    TIMEOUT_RETRY_EXHAUSTED,
    INFRASTRUCTURE_FAILURE
}
```

The serialization mechanism should be configurable rather than requiring Java serialization.

JSON is a reasonable default representation.

---

# 10. Continuations

A continuation describes what should receive a computation's eventual outcome.

A continuation is:

- durable,
- serializable,
- application-defined,
- opaque to the Continuum core.

Conceptually:

```java
public record Continuation(
    ContinuationId id,
    byte[] payload
) {
}
```

The payload might represent something such as:

```json
{
  "agentId": "research-agent",
  "toolInvocationId": "R123/call_7"
}
```

or:

```json
{
  "orderId": "O1234",
  "nextState": "PAYMENT_CONFIRMED"
}
```

Continuum does not need to understand these values.

The application consuming the continuation does.

---

# 11. Continuation identity

Every continuation should have its own unique identity.

Suggested representation:

```java
public record ContinuationId(UUID value) {
}
```

This identity is useful for:

- outbox delivery identity,
- deduplication,
- diagnostics,
- tracing,
- competing consumers.

The pair:

```text
ComputationId + ContinuationId
```

uniquely identifies one obligation to deliver one computation outcome to one continuation.

---

# 12. Mandatory initial continuation

A computation MUST be created with at least one continuation.

The following should not be possible:

```java
continuum.create(kind, deadline);
```

Instead:

```java
Computation computation = continuum.create(
    kind,
    deadline,
    continuation
);
```

Creation must atomically persist:

```text
computation
+
initial continuation
```

Either both exist or neither exists.

This establishes the invariant:

> Every Continuum computation has at least one durable consumer before it can be exposed to external execution.

---

# 13. Additional continuations

Additional consumers may register interest in an existing computation.

Conceptual API:

```java
RegistrationResult registerContinuation(
    ComputationId computationId,
    Continuation continuation
);
```

This operation has two possible results:

```java
public sealed interface RegistrationResult {

    record Registered(
        ContinuationId continuationId
    ) implements RegistrationResult {
    }

    record Resolved(
        Outcome outcome
    ) implements RegistrationResult {
    }
}
```

The operation means:

> If this computation is still pending, durably register this continuation. Otherwise, give me its outcome now.

No caller actually blocks.

Therefore names such as `await()` should be avoided.

---

# 14. Atomic registration semantics

Continuation registration must be atomic with respect to computation completion.

There is an unavoidable race:

```text
Thread A                         Thread B

registerContinuation()          complete()
        |                           |
        v                           v
      PENDING                     PENDING
        |                           |
        +-------- race -------------+
```

Exactly one ordering must become durable.

## Registration wins

If registration commits first:

```text
continuation inserted
        |
        v
completion occurs
        |
        v
outbox delivery created
```

The newly registered continuation MUST receive the outcome.

## Completion wins

If completion commits first:

```text
computation becomes terminal
        |
        v
registration checks computation
        |
        v
returns memoized outcome
```

The late continuation is NOT persisted.

This guarantees:

> A caller registering interest can never fall into a gap where it receives neither a registered continuation nor the completed result.

---

# 15. Multiple continuations

A computation may have any number of continuations.

For example:

```text
Computation C17
    |
    +-- Continuation A
    +-- Continuation B
    +-- Continuation C
```

When C17 resolves:

```text
C17 -> Result R
        |
        +-- delivery A(R)
        +-- delivery B(R)
        +-- delivery C(R)
```

Each delivery is independent.

Failure to deliver continuation B must not prevent delivery to A or C.

This is not intended to turn Continuum into a general pub/sub system.

All continuations subscribe to exactly one thing:

> The single terminal outcome of one specific computation.

---

# 16. Continuation persistence

A relational implementation might use:

```text
continuum_continuation
----------------------
id                  UUID / VARCHAR PRIMARY KEY
computation_id      UUID / VARCHAR NOT NULL
payload             BLOB / JSON / JSONB NOT NULL
created_at          TIMESTAMP NOT NULL
```

with:

```text
FOREIGN KEY computation_id
    REFERENCES continuum_computation(id)
```

There may be zero additional continuations, but there must always be at least the initial continuation while the computation is pending.

---

# 17. Completion

The external result producer completes a computation using only its globally unique ID:

```java
CompletionResult complete(
    ComputationId computationId,
    Outcome outcome
);
```

Suggested result:

```java
public enum CompletionResult {
    COMPLETED,
    ALREADY_RESOLVED,
    NOT_FOUND
}
```

The exact distinction between `ALREADY_RESOLVED` and `NOT_FOUND` depends on retention policy.

The first successful terminalization wins.

Subsequent attempts must never replace the existing outcome.

---

# 18. Completion transaction

Completion is one of the central correctness boundaries in Continuum.

The implementation must atomically:

1. verify the computation is pending,
2. store the terminal outcome,
3. change the computation status,
4. identify all currently registered continuations,
5. create one outbox delivery for every continuation.

Conceptually:

```text
BEGIN

lock computation C17

verify status == PENDING

UPDATE C17
    status = COMPLETED
    result = R

for each continuation of C17:
    INSERT outbox delivery

COMMIT
```

If the transaction fails, none of the state changes become visible.

---

# 19. Result memoization

Terminal computation outcomes are retained for some configurable period.

This enables late registration:

```text
C17 already completed with R

registerContinuation(C17, X)
              |
              v
          return R
```

No continuation is registered because no future notification is required.

This behavior makes a Continuum computation effectively a durable memoized eventual value.

Retention policy must eventually determine when terminal computation records may be garbage-collected.

---

# 20. Outbox

Continuum requires a durable outbox.

The outbox represents:

> A terminal computation outcome that still needs to be delivered to a previously registered continuation.

Conceptual schema:

```text
continuum_outbox
----------------
id                  UUID / VARCHAR PRIMARY KEY
computation_id      UUID / VARCHAR NOT NULL
continuation_id     UUID / VARCHAR NOT NULL
payload             BLOB / JSON / JSONB NOT NULL
available_at        TIMESTAMP NOT NULL
claimed_by          VARCHAR NULL
claimed_until       TIMESTAMP NULL
attempt_count       INTEGER NOT NULL
created_at          TIMESTAMP NOT NULL
```

Each continuation produces its own outbox item.

---

# 21. Outbox payload

The outbox must contain enough information for a consumer to process the delivery without reconstructing transient state.

Conceptually:

```java
public record CompletionDelivery(
    ComputationId computationId,
    ComputationKind kind,
    ContinuationId continuationId,
    byte[] continuation,
    Outcome outcome
) {
}
```

This object may be serialized as the outbox payload.

This makes an outbox item self-contained.

---

# 22. Competing consumers

Continuum should support multiple outbox consumers operating concurrently.

Conceptually:

```text
                 OUTBOX
               /   |   \
              /    |    \
             v     v     v
         worker1 worker2 worker3
```

Exactly one worker should hold a delivery lease at a time.

Typical lifecycle:

```text
AVAILABLE
    |
    | claim
    v
LEASED
    |
    +-- success --> DELETE
    |
    +-- failure --> release / lease expires
```

The implementation should not rely on permanent worker ownership.

If a worker dies, another worker must eventually be able to reclaim the delivery.

---

# 23. Delivery semantics

Continuum should provide **at-least-once delivery** of continuation notifications.

A consumer may receive the same delivery more than once if a crash occurs after processing but before acknowledging/deleting the outbox item.

Therefore continuation consumers should be idempotent.

The stable:

```text
ContinuationId
```

or outbox delivery ID can be used as a deduplication key.

Continuum should not claim exactly-once delivery to arbitrary external systems.

---

# 24. Successful delivery

After a consumer successfully and durably processes an outbox item, the item should be deleted.

The outbox represents active delivery obligations only.

Therefore:

```text
outbox row exists
    =
delivery remains outstanding
```

and:

```text
outbox row absent
    =
Continuum no longer owns that delivery obligation
```

Delivered rows should not remain mixed with pending rows merely for historical purposes.

Audit/history should be a separate concern if required.

---

# 25. Transactional destination delivery

If the continuation consumer writes to storage that can participate in the same transaction as Continuum, the ideal operation is:

```text
BEGIN

write destination

delete outbox item

COMMIT
```

This produces an atomic ownership transfer:

```text
Continuum outbox
      |
      | atomic
      v
destination
```

At every committed point, the result is durably owned by one side.

For destinations that cannot participate in the same transaction, at-least-once delivery and consumer idempotency are required.

---

# 26. Timeout semantics

A pending computation may have a deadline:

```java
Instant deadline();
```

A deadline means:

> If no outcome has been reported by this time, the computation requires recovery processing.

Timeout detection must itself be durable.

It must not depend on an in-memory Java timer surviving.

---

# 27. Retry semantics

A computation should declare whether its external operation may safely be retried.

Initial model:

```java
public enum RetrySemantics {
    RETRYABLE,
    NON_RETRYABLE
}
```

For `RETRYABLE`:

> The application asserts that repeating the logical external operation is safe.

For `NON_RETRYABLE`:

> Continuum must not automatically request another execution after the result becomes uncertain.

Continuum does not determine why an operation is safe to retry.

---

# 28. Logical invocation identity

Retryable work needs a stable logical invocation identity.

Continuum should permit the creator to supply one:

```java
public record InvocationId(String value) {
}
```

This identity may be used by the external execution system for idempotency.

For example:

```text
InvocationId = Nessy ToolInvocationId
```

On every retry, the same invocation ID must be supplied.

The invocation ID is distinct from `ComputationId`.

```text
ComputationId
    identifies the eventual value

InvocationId
    identifies the logical external operation
```

---

# 29. Timeout processing

When a deadline expires, the timeout processor loads the computation.

If it is already terminal:

```text
do nothing
```

If it remains pending and is `RETRYABLE`:

```text
request redispatch
using the same InvocationId
```

If it remains pending and is `NON_RETRYABLE`:

```text
resolve as Failure(
    TIMEOUT_NON_RETRYABLE
)
```

The failure then follows the exact same continuation/outbox delivery path as any other outcome.

---

# 30. External execution boundary

Continuum does not execute arbitrary application work.

The application is responsible for dispatching the work initially.

A retry handler may be registered so Continuum can request redispatch:

```java
public interface RetryHandler {

    void retry(
        PendingComputation computation
    );
}
```

The application determines how to translate that request into actual work.

---

# 31. Idempotency

Continuum cannot guarantee exactly-once external side effects.

Consider:

```text
invoke operation
       |
       v
external side effect succeeds
       |
       X
executor crashes
       |
       v
no result reported
```

Continuum cannot determine whether the side effect occurred.

Therefore the external operation implementation owns idempotency.

Continuum assists by providing a stable `InvocationId`.

For a retryable operation:

```text
attempt 1 -> InvocationId T123
attempt 2 -> InvocationId T123
attempt 3 -> InvocationId T123
```

The external system may use T123 as an idempotency key.

---

# 32. Creation API

An initial API might look like:

```java
public interface Continuum {

    Computation create(
        ComputationRequest request
    );

    RegistrationResult registerContinuation(
        ComputationId id,
        Continuation continuation
    );

    CompletionResult complete(
        ComputationId id,
        Outcome outcome
    );

    Optional<Computation> find(
        ComputationId id
    );
}
```

Request:

```java
public record ComputationRequest(
    ComputationKind kind,
    Continuation continuation,
    Instant deadline,
    RetrySemantics retrySemantics,
    InvocationId invocationId,
    Map<String, String> metadata
) {
}
```

The continuation is mandatory.

---

# 33. Example: Nessy durable tool call

Nessy receives a model response containing a deferred tool call.

Nessy creates:

```text
kind = "tool-result"

invocationId =
    Nessy ToolInvocationId

continuation = {
    agentId,
    toolInvocationId
}

deadline =
    now + tool timeout

retrySemantics =
    tool registration policy
```

Continuum returns:

```text
ComputationId C17
```

Nessy dispatches the tool with:

```text
ComputationId = C17
InvocationId = T42
```

The Nessy process may now disappear.

Later, the tool reports:

```java
continuum.complete(
    C17,
    Outcome.success(result)
);
```

Continuum atomically:

```text
C17 -> COMPLETED

+

outbox delivery for Nessy continuation
```

A Continuum consumer receives the delivery and inserts the corresponding tool-result observation into Nessy's durable observation backlog.

The agent may then resume on any available runtime.

---

# 34. Example: second interested party

While C17 remains pending, another component wants the same result.

It calls:

```java
registerContinuation(C17, continuationB);
```

If C17 remains pending:

```text
Registered(continuationBId)
```

is returned.

When C17 completes:

```text
outbox delivery A
outbox delivery B
```

are generated.

If C17 had already completed, the call instead returns:

```text
Resolved(outcome)
```

and continuation B is never persisted.

---

# 35. Persistence SPI

The core library should define the semantic operations it requires without exposing JDBC assumptions.

The persistence implementation must support:

- computation creation,
- continuation creation,
- atomic computation + initial continuation creation,
- atomic continuation registration versus terminalization,
- atomic terminalization + outbox materialization,
- competing-consumer outbox claiming,
- outbox acknowledgement/removal,
- timeout discovery,
- optimistic or pessimistic concurrency control.

The SPI should be designed around these semantic operations rather than exposing a generic CRUD repository unless generic CRUD proves sufficient.

This is important because Continuum's correctness depends more on atomic state transitions than ordinary entity persistence.

---

# 36. Initial modules

Suggested Maven structure:

```text
continuum-parent
|
+-- continuum-core
|
+-- continuum-memory
|
+-- continuum-jdbc
|
+-- continuum-testing
```

Potential future integrations:

```text
continuum-spring
continuum-substrate
continuum-restate
continuum-temporal
```

`continuum-core` should remain dependency-light.

---

# 37. Required invariants

The implementation must preserve the following invariants.

### I1 — Global identity

Every computation has a globally unique opaque `ComputationId`.

### I2 — Initial consumer

A computation cannot exist without its initial continuation being durably registered.

### I3 — Single outcome

A computation resolves at most once.

### I4 — Immutable terminal state

A terminal outcome can never be replaced.

### I5 — Registration race safety

A continuation registration receives either:

- durable registration, or
- the already-resolved outcome.

Never neither.

### I6 — Registered continuation delivery

Every continuation committed before terminalization produces a durable delivery obligation.

### I7 — Atomic completion

Terminal outcome persistence and outbox materialization occur atomically.

### I8 — Independent deliveries

Failure delivering one continuation does not prevent other continuation deliveries.

### I9 — Crash recovery

No correctness property depends on the process that created, completed, or delivered a computation remaining alive.

### I10 — Stable retry identity

Retries of the same logical operation retain the same `InvocationId`.

### I11 — No false exactly-once guarantee

Continuum never claims to guarantee exactly-once external side effects.

---

# 38. Required concurrency tests

The test suite should aggressively exercise races.

## Create crash

Failure during initial computation + continuation transaction results in neither being persisted.

## Register versus complete

Run registration and completion concurrently many times.

Every registration must result in exactly one of:

```text
Registered
```

or:

```text
Resolved(outcome)
```

If `Registered` wins, an outbox delivery must eventually exist.

## Complete versus complete

Two different outcomes race to complete the same computation.

Exactly one wins.

The stored outcome must correspond to the winner.

## Completion transaction failure

Inject failure while creating outbox entries.

Neither terminal state nor partial outbox state may commit.

## Multiple continuations

Register many continuations concurrently.

Complete concurrently.

Every continuation that successfully returned `Registered` must have exactly one logical delivery obligation.

## Competing consumers

Multiple workers race for the same outbox item.

Only one active lease should exist.

## Consumer crash

Worker claims an item and dies.

After lease expiration, another worker must be able to process it.

## Late registration

Register after terminalization.

Verify the outcome is returned immediately and no continuation/outbox row is created.

## Timeout versus completion

Timeout processing and successful completion race.

Exactly one terminal outcome wins.

---

# 39. Version 1 success scenario

A successful Continuum v1 should demonstrate:

```text
JVM A
    |
    | create C17 + continuation A
    v
external work dispatched

JVM A dies

JVM B
    |
    | register continuation B
    v
REGISTERED

JVM B dies

external worker
    |
    | complete(C17, R)
    v

database transaction:
    C17 = COMPLETED(R)
    outbox A created
    outbox B created

worker C
    |
    | claim A
    | deliver A
    | acknowledge A
    v

worker C dies

worker D
    |
    | claim B
    | deliver B
    | acknowledge B
    v

hours later

JVM E
    |
    | registerContinuation(C17, C)
    v

RESOLVED(R)
```

No process participating earlier in the lifecycle needs to remain alive.

---

# 40. Core definition

Continuum can be summarized as:

> **Continuum is a durable, distributed eventual-value primitive. A computation has one globally unique identity, one terminal outcome, and one or more durable continuations interested in that outcome.**

Or, more succinctly:

> **One computation. One eventual result. Any number of durable continuations.**

That should be the conceptual center of the library.