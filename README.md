# Continuum

[![CI](https://github.com/jwcarman/continuum/actions/workflows/maven.yml/badge.svg)](https://github.com/jwcarman/continuum/actions/workflows/maven.yml)
[![CodeQL](https://github.com/jwcarman/continuum/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/jwcarman/continuum/actions/workflows/github-code-scanning/codeql)
[![Docs](https://github.com/jwcarman/continuum/actions/workflows/docs.yml/badge.svg)](https://jwcarman.github.io/continuum/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/dynamic/xml?url=https://raw.githubusercontent.com/jwcarman/continuum/main/pom.xml&query=//*[local-name()='maven.compiler.release']/text()&label=Java&color=orange)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/maven-central/v/org.jwcarman.continuum/continuum-core)](https://central.sonatype.com/artifact/org.jwcarman.continuum/continuum-core)

[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=jwcarman_continuum&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=jwcarman_continuum)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=jwcarman_continuum&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=jwcarman_continuum)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=jwcarman_continuum&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=jwcarman_continuum)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=jwcarman_continuum&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=jwcarman_continuum)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=jwcarman_continuum&metric=coverage)](https://sonarcloud.io/summary/new_code?id=jwcarman_continuum)

Durable computation coordination for Java.

**[Documentation](https://jwcarman.github.io/continuum/)**

> **One computation. One eventual result. Any number of durable continuations.**

A Continuum computation represents an operation whose result may not arrive
until some arbitrary point in the future — a deferred tool call, a human
approval, an external job. The process that creates the computation does not
need to remain alive while the work executes: Continuum durably coordinates
the result, delivering it to every registered continuation exactly as promised,
across process restarts, machine failures, and deployments.

Continuum does **not** execute the work itself. The external system performing
the work receives the `ComputationId` and eventually reports the outcome.

## Coordinates

```xml
<dependency>
    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-core</artifactId>
    <version>0.3.0</version>
</dependency>
```

Or import the BOM (`org.jwcarman.continuum:continuum-bom`) and add modules
version-free.

Serialization is pluggable through [codec](https://github.com/jwcarman/codec)
(`org.jwcarman.codec`). The quick start below uses the Jackson 3 backend —
a separate dependency:

```xml
<dependency>
    <groupId>org.jwcarman.codec</groupId>
    <artifactId>codec-jackson</artifactId>
    <version>0.4.0</version>
</dependency>
```

(`codec-jackson2` covers Jackson 2 applications; `codec-gson` and
`codec-protobuf` work the same way, or implement `Codec<T>` directly.) Continuum logs through `slf4j-api` only — add the
SLF4J provider your application already uses (e.g. `logback-classic`) or
expect SLF4J's NOP warning.

| Module | Purpose |
|---|---|
| `continuum-core` | API, typed clients, persistence SPI |
| `continuum-memory` | In-memory persistence for tests/embedded use |
| `continuum-jdbc` | PostgreSQL persistence |
| `continuum-testing` | TCK for certifying persistence providers |
| `continuum-spring-boot-starter` | Boot auto-configuration (with `continuum-autoconfigure`) |

## Quick start

Wire the core once, then mint a typed client per computation *kind*:

```java
Continuum continuum = new DefaultContinuum(new JdbcContinuumRepository(dataSource));

var toolCalls = continuum.client(
    "tool-result",
    ToolCallResult.class, ToolCallContinuation.class, ToolCallDescriptor.class,
    cfg -> cfg.codecs(new JacksonCodecFactory(mapper))
              .deadline(Duration.ofMinutes(5)));
```

Create a computation and dispatch the work (dispatching is your job —
Continuum keeps the breadcrumb):

```java
var computation = toolCalls.create(new ToolCallContinuation(agentId, callId), descriptor);
toolRuntime.dispatch(descriptor, computation.id());   // your transport
```

Anywhere, any process, any time later, the worker reports back:

```java
toolCalls.complete(computationId, new ToolCallResult(...));   // or .fail(id, "reason")
```

## Spring Boot

Add `continuum-spring-boot-starter` and a `Continuum` bean is auto-configured:
with `continuum-jdbc` on the classpath and a `DataSource` defined you get
durable PostgreSQL persistence; otherwise the starter falls back to the
in-memory repository and logs a warning (fine for tests, not for production —
computations will not survive restarts). An application-defined
`ContinuumRepository` or `InstantSource` bean always wins. Clients stay
app-defined `@Bean`s (they carry your types and codecs), and pumping is your
`@Scheduled` methods, as below.

## Pumping

Continuum owns no threads. The three recurring activities are batch methods on
the client; schedule them however you like (fixed-delay recommended):

```java
scheduler.scheduleWithFixedDelay(() ->
    toolCalls.deliverResults(BatchSize.of(25), delivery -> {
        var continuation = delivery.continuation();
        switch (delivery.outcome()) {
            case TypedOutcome.Success<ToolCallResult>(var result) -> backlog.recordResult(continuation, result);
            case TypedOutcome.Failure<ToolCallResult>(var message) -> backlog.recordFailure(continuation, message);
            case TypedOutcome.Expired<ToolCallResult>(var kind, var message) -> backlog.recordTimeout(continuation, kind);
        }
    }), 0, 1, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.retryExpiredComputations(BatchSize.of(12), Retry.of(r -> r
        .atMost(3)
        .handler((descriptor, ctx) -> toolRuntime.dispatch(descriptor, ctx.computationId())))),
    5, 15, TimeUnit.SECONDS);

scheduler.scheduleWithFixedDelay(() ->
    toolCalls.purgeExpiredResults(BatchSize.of(200), ResultTtl.ofHours(1)), 1, 10, TimeUnit.MINUTES);
```

Every instance of your application can run all pumps identically — no leader
election. Leases and `SKIP LOCKED` make overlapping pumps correct; crashes
anywhere leave nothing to repair. If a pump never runs, no correctness
property is lost — only liveness waits.

Non-retryable kinds mint a two-type client (no dispatch type); its reap takes
no `Retry` and expires overdue computations as `RETRY_DISALLOWED`:

```java
var approvals = continuum.client("approval", Decision.class, ApprovalContinuation.class,
    cfg -> cfg.codecs(codecs).deadline(Duration.ofDays(3)));
approvals.failExpiredComputations(BatchSize.of(10));
```

## Design highlights

- **Presence means pending** — a computation row exists only while pending;
  every terminalization atomically moves it to a memoized result row and fans
  out outbox deliveries.
- **Three-arm outcomes** — `Success(byte[])` / `Failure(String)` /
  `Expired(ExpiryKind, String)`: a producer saying "no" and a deadline lapsing
  are different facts, and consumers switch on exactly that.
- **Retryability is data** — a computation with a dispatch payload is
  retryable; one without is not. Embed your idempotency key in the payload;
  it comes back verbatim on every retry attempt.
- **At-least-once delivery** — consumers deduplicate on `ContinuationId`.
- **No stored status anywhere** — status is derived from durable facts, never
  a mutable label.

See `docs/superpowers/specs/` for the full specification and design.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
