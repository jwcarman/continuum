# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.0] - 2026-08-24

### Added

- Core API: `Continuum`/`DefaultContinuum`, three-arm `Outcome`
  (`Success`/`Failure`/`Expired`), derived `ComputationStatus`, opaque byte[]
  payloads end to end.
- Typed clients: `ContinuumClient<R, C>` (non-retryable kinds) and
  `RetryableContinuumClient<R, C, D>` (retryable kinds) minted via
  `continuum.client(...)` customizers, with pluggable serialization through
  `org.jwcarman.codec`.
- Application-pumped batch methods: `deliverResults`,
  `retryExpiredComputations` / `failExpiredComputations`,
  `purgeExpiredResults`; `Retry.of(...)` declarative retry customizer.
- Persistence SPI (`ContinuumRepository`) with presence-means-pending
  semantics and kind-scoped pumping operations.
- `continuum-memory`: in-memory provider for tests and embedded use.
- `continuum-jdbc`: PostgreSQL provider (`FOR UPDATE SKIP LOCKED` claiming,
  atomic ownership-transfer completion).
- `continuum-testing`: TCK exercising the full concurrency battery, run by
  both providers.
- Value-typed pump parameters (`BatchSize`, `Lease`, `Backoff`, `ResultTtl`)
  with validated construction and unit factories.
- `continuum-bom`.
- `continuum-spring-boot-starter` + `continuum-autoconfigure`: auto-configures
  `JdbcContinuumRepository` (when `continuum-jdbc` and a `DataSource` are
  present) or falls back to the in-memory repository with a warning, and wires
  a `Continuum` over it (honoring app-defined `ContinuumRepository` /
  `InstantSource` beans).

[0.1.0]: https://github.com/jwcarman/continuum/releases/tag/0.1.0
