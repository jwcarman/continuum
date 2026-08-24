# Spring Boot

Add the starter:

```xml
<dependency>
    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-spring-boot-starter</artifactId>
    <version>${continuum.version}</version>
</dependency>
```

A `Continuum` bean is auto-configured. Repository selection:

1. An application-defined `ContinuumRepository` bean always wins.
2. With `continuum-jdbc` on the classpath **and** a `DataSource` bean,
   you get durable PostgreSQL persistence (`JdbcContinuumRepository`).
   Ordering against Boot's own `DataSourceAutoConfiguration` is handled, so
   a Boot-auto-configured DataSource counts.
3. Otherwise the starter falls back to the **in-memory repository and logs a
   warning** — computations will not survive restarts. Fine for tests; not
   for production.

An application-defined `InstantSource` bean is honored (handy for
deterministic tests); otherwise the system clock is used.

## What stays yours

- **Clients** are application `@Bean`s — they carry your kinds, types, and
  codecs. (With `codec-autoconfigure` on the classpath, inject the
  `CodecFactory` bean into your client definitions.)
- **Pumping** is your `@Scheduled` methods, per kind, per cadence — see
  [Pumping & Scheduling](pumping.md).
- **Schema** is your migration discipline — see
  [Persistence Providers](persistence.md). The starter never executes DDL.
