/*
 * Copyright © 2026 James Carman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jwcarman.continuum.jdbc;

import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.cockroachdb.CockroachContainer;

/**
 * The certification experiment behind the platform guard: does CockroachDB satisfy Continuum's
 * observable contract with the <em>unmodified</em> PostgreSQL provider — same SQL, same DDL?
 *
 * <p>accent measured CockroachDB genuinely honouring {@code FOR UPDATE SKIP LOCKED} under
 * contention, which predicts a pass; the TCK is the gate that turns that prediction into (or
 * refutes it as) a supportable claim. Construction goes through {@link
 * JdbcContinuumRepository#assumePostgreSql} because the guard would — correctly — refuse
 * CockroachDB today: this run is exactly the evidence a future widening of the guard would rest on,
 * so it must bypass it.
 *
 * <p>One anticipated hazard, deliberately left to the battery to expose rather than worked around:
 * CockroachDB runs SERIALIZABLE by default and aborts contended transactions with retry errors
 * (SQLSTATE 40001) that PostgreSQL's READ COMMITTED never surfaces here. {@code inTransaction} does
 * not retry, so the racing tests are where a failure would show first.
 */
// Deliberately NOT named *IT: this must never run in a default build, because the answer is
// that CockroachDB FAILS certification — six of six runs, ~300 races: usually a 40001
// RETRY_SERIALIZABLE surfacing as ContinuumPersistenceException, and at least twice the silent
// form — both transactions committed, Registered returned, no delivery ever created. Run it on
// demand with: mvn -pl continuum-jdbc verify -Dit.test=CockroachCertificationExperiment
class CockroachCertificationExperiment extends ContinuumTck {

  private static final CockroachContainer COCKROACH =
      new CockroachContainer("cockroachdb/cockroach:latest-v24.1");

  static {
    COCKROACH.start();
  }

  @Override
  protected ContinuumRepository createRepository() {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(COCKROACH.getJdbcUrl());
    dataSource.setUser(COCKROACH.getUsername());
    dataSource.setPassword(COCKROACH.getPassword());
    TckSchema.applySchema(dataSource, ContinuumDialect.POSTGRESQL);
    TckSchema.truncate(dataSource);
    return JdbcContinuumRepository.assumePostgreSql(dataSource);
  }
}
