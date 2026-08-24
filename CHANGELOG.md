# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.0] - 2026-08-24

Initial release: the byte[] `Continuum` core, typed clients
(`ContinuumClient` / `RetryableContinuumClient`), application-pumped
delivery/expiry/purge, in-memory and PostgreSQL providers, a provider TCK,
and a Spring Boot starter.

Deadlines are mandatory — that is what makes "one computation, one *eventual*
outcome" true. An open-ended wait is expressed as a decision taken at each
lapse rather than as an absent deadline, so the deadline stays a dead-man's
switch and giving up is explicit and carries a reason:

- `ExpiryContext` carries the computation's `submittedAt` and the pump's
  `observedAt`, with `elapsedTime()` measured on Continuum's `InstantSource`.
  A wall-clock give-up rule ("stop waiting seven days after submission") needs
  no arithmetic over attempt counts, which is wrong once attempts carry
  differing timeouts. Both `Retry` and `Expiry` receive it.
- `ContinuumClient.failExpiredComputations(BatchSize, Expiry)` lets a
  **non-retryable** kind keep waiting or give up at each lapse — without
  inventing a dispatch payload it must never use, which would trade the
  compile-time non-retryability guarantee for a convention.
- `CompletionDelivery` carries `submittedAt`, `completedAt`, and
  `elapsedTime()`, denormalized onto the outbox row so a consumer can record
  end-to-end duration when the outcome arrives, even after the memoized result
  is purged.

Timestamp vocabulary is uniform: `submitted_at` is when the computation was
submitted, `completed_at` when it reached a terminal outcome, and `created_at`
means only "this row was written".

See the [documentation](https://jwcarman.github.io/continuum/) for the full
tour.

[0.1.0]: https://github.com/jwcarman/continuum/releases/tag/0.1.0
