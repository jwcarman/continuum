# Getting Started

## Coordinates

```xml
<dependency>
    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-core</artifactId>
    <version>0.3.0</version>
</dependency>
```

Or import `org.jwcarman.continuum:continuum-bom` and add modules version-free.
Add `continuum-jdbc` for PostgreSQL persistence, `continuum-memory` for tests.

Serialization is pluggable through [codec](https://github.com/jwcarman/codec).
The examples below use the Jackson 3 backend:

```xml
<dependency>
    <groupId>org.jwcarman.codec</groupId>
    <artifactId>codec-jackson</artifactId>
    <version>0.4.0</version>
</dependency>
```

(`codec-jackson2` covers Jackson 2 applications; `codec-gson` and
`codec-protobuf` work the same way, or implement `Codec<T>` directly.)
Continuum logs through `slf4j-api` only — bring your own provider.

## Wire the core

```java
Continuum continuum = new DefaultContinuum(new JdbcContinuumRepository(dataSource));
```

The single-argument constructor uses the system clock; pass an
`InstantSource` explicitly when tests need to control time.

## Mint a typed client per kind

A *kind* names a category of computation — `tool-result`, `approval`,
`payment-result`. The client's **shape declares retryability**:

```java
// Three types = retryable: R result, C continuation, D dispatch
var toolCalls = continuum.client(
    "tool-result",
    ToolCallResult.class, ToolCallContinuation.class, ToolCallDescriptor.class,
    cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
              .deadline(Duration.ofMinutes(5)));

// Two types = non-retryable: no dispatch type exists
var approvals = continuum.client(
    "approval",
    Decision.class, ApprovalContinuation.class,
    cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
              .deadline(Duration.ofDays(3)));
```

Config is creation-time facts only: codecs and the per-attempt deadline.
Daemon policy (batch sizes, leases, retry behavior, retention) lives at the
[pump call sites](pumping.md).

## Create, dispatch, complete

```java
// Start a durable computation; dispatching the work is your job.
var computation = toolCalls.create(new ToolCallContinuation(agentId, callId), descriptor);
toolRuntime.dispatch(descriptor, computation.id());     // your transport

// Any process, any time later: the worker reports back.
toolCalls.complete(computationId, new ToolCallResult(...));
// ...or, if the work itself failed:
toolCalls.fail(computationId, "tool rejected the input");
```

The creating process may now die — the computation, its continuation, and the
breadcrumb needed to retry it are durable.

## Receive results

Results arrive by [pumping deliveries](pumping.md). Additional parties can
register interest in a pending computation with
`register(computationId, continuation)`, which returns either a registration
or — if the computation already resolved — the decoded outcome immediately.

## The raw API

Everything above is a typed skin over a byte[] contract
(`Continuum.create/registerContinuation/complete/find` with opaque payloads).
The raw API is public and documented — the escape hatch for polyglot payloads
or exotic serialization.
