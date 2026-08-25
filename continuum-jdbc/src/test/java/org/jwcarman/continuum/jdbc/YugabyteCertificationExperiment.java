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
import org.testcontainers.containers.YugabyteDBYSQLContainer;

/**
 * The YugabyteDB half of the certification experiment — see {@link
 * CockroachCertificationExperiment} for the full rationale. Same unmodified PostgreSQL SQL and DDL,
 * same deliberate {@link JdbcContinuumRepository#assumePostgreSql} bypass of the guard this run
 * exists to inform.
 *
 * <p>The connection deliberately uses pgjdbc against the YSQL port rather than the YugabyteDB smart
 * driver: accent's reconnaissance found {@code com.yugabyte.Driver} registers itself for {@code
 * jdbc:postgresql:} URLs and can silently capture them, so the URL is built explicitly and handed
 * to a {@code PGSimpleDataSource} — no {@code DriverManager} routing involved.
 */
class YugabyteCertificationExperiment extends ContinuumTck {

  private static final YugabyteDBYSQLContainer YUGABYTE =
      new YugabyteDBYSQLContainer("yugabytedb/yugabyte:2024.1.0.0-b129");

  static {
    YUGABYTE.start();
  }

  @Override
  protected ContinuumRepository createRepository() {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(
        "jdbc:postgresql://"
            + YUGABYTE.getHost()
            + ":"
            + YUGABYTE.getMappedPort(5433)
            + "/"
            + YUGABYTE.getDatabaseName());
    dataSource.setUser(YUGABYTE.getUsername());
    dataSource.setPassword(YUGABYTE.getPassword());
    TckSchema.applyAndTruncate(dataSource);
    return JdbcContinuumRepository.assumePostgreSql(dataSource);
  }
}
