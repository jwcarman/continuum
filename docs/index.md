# Continuum

Durable computation coordination for Java.

> **One computation. One eventual result. Any number of durable continuations.**

A Continuum computation represents an operation whose result may not arrive
until some arbitrary point in the future — a deferred tool call, a human
approval, an external job. The process that creates the computation does not
need to remain alive while the work executes: Continuum durably coordinates
the result and delivers it to every registered continuation, across process
restarts, machine failures, and deployments.

Continuum does **not** execute the work itself. Your application dispatches
the work; the external system performing it receives the `ComputationId` and
eventually reports the outcome. Continuum's responsibility is durable
coordination of that outcome.

## What survives

A computation survives JVM termination, process restart, machine failure,
deployment, and long periods of inactivity. No correctness property depends on
a live thread, Java object, callback closure, or JVM remaining present.

## What Continuum is not

Not a workflow engine, not a DAG executor, not a distributed scheduler, not a
message broker, and not an exactly-once side-effect mechanism (no such thing
exists across process boundaries — Continuum gives you at-least-once delivery
plus a stable place to carry your idempotency key instead).

## Modules

| Module | Purpose |
|---|---|
| `continuum-core` | API, typed clients, persistence SPI |
| `continuum-memory` | In-memory persistence for tests/embedded use |
| `continuum-jdbc` | PostgreSQL persistence |
| `continuum-spring-boot-starter` | Boot auto-configuration (with `continuum-autoconfigure`) |
| `continuum-testing` | TCK for certifying persistence providers |
| `continuum-bom` | Bill of materials |

Start with [Getting Started](guides/getting-started.md), or read
[The Computation Model](concepts/model.md) to understand what's underneath.
