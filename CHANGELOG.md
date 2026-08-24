# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Core API: `Continuum`/`DefaultContinuum`, three-arm `Outcome`
  (`Success`/`Failure`/`Expired`), derived `ComputationStatus`, opaque byte[]
  payloads end to end.
- Typed clients: `ContinuumClient<R, C>` (non-retryable kinds) and
  `RetryableContinuumClient<R, C, D>` (retryable kinds) minted via
  `continuum.client(...)` customizers, with pluggable serialization through
  `org.jwcarman.codec`.
- Application-pumped batch methods: `deliverResults`,
  `reapExpiredComputations` (retry-consulting and always-fail shapes),
  `purgeExpiredResults`; `Retry.of(...)` declarative retry customizer.
- Persistence SPI (`ContinuumRepository`) with presence-means-pending
  semantics and kind-scoped pumping operations.
- `continuum-memory`: in-memory provider for tests and embedded use.
- `continuum-jdbc`: PostgreSQL provider (`FOR UPDATE SKIP LOCKED` claiming,
  atomic ownership-transfer completion).
- `continuum-testing`: TCK exercising the full concurrency battery, run by
  both providers.
- `continuum-bom`.
