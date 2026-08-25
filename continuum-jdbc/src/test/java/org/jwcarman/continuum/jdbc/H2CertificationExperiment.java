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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.jdbcx.JdbcDataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

/**
 * The H2 certification experiment: does H2 2.3 in PostgreSQL compatibility mode satisfy the
 * observable contract with the unmodified Postgres dialect and DDL? Not production-motivated — H2
 * is every Spring Boot shop's test database, the guard currently refuses it by name, and knowing
 * whether that refusal is protecting anyone is worth ten in-process minutes.
 *
 * <p>accent's contention harness measured H2 2.3 accepting {@code FOR UPDATE SKIP LOCKED}; whether
 * its MVStore locking composes with the ownership transfer is what this run answers. Construction
 * goes through {@link JdbcContinuumRepository#withDialect} — the extension point's first real
 * exercise — because detection would (correctly, pending this evidence) refuse H2.
 *
 * <p>Deliberately not named {@code *IT} until the verdict is in. Run on demand: {@code mvn -pl
 * continuum-jdbc verify -Dit.test=H2CertificationExperiment}
 */
class H2CertificationExperiment extends ContinuumTck {

  private static final JdbcDataSource DATA_SOURCE = new JdbcDataSource();

  static {
    DATA_SOURCE.setURL(
        "jdbc:h2:mem:continuum-tck;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
    applyPostgresSchemaWithH2Spellings();
  }

  /**
   * The one translation H2's PostgreSQL mode needs: it rejects the {@code TIMESTAMPTZ} alias and
   * wants the spelled-out form. Everything else in the Postgres DDL — {@code UUID}, {@code BYTEA},
   * the index statements — it accepts as written.
   */
  private static void applyPostgresSchemaWithH2Spellings() {
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement();
        InputStream schema =
            H2CertificationExperiment.class.getResourceAsStream(
                ContinuumDialect.POSTGRESQL.schemaResource())) {
      String ddl =
          new String(schema.readAllBytes(), StandardCharsets.UTF_8)
              .replace("TIMESTAMPTZ", "TIMESTAMP(6) WITH TIME ZONE");
      for (String sql : ddl.replaceAll("--[^\n]*", "").split(";")) {
        if (!sql.trim().isEmpty()) {
          statement.execute(sql.trim());
        }
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("failed to apply H2 schema", e);
    }
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return JdbcContinuumRepository.withDialect(DATA_SOURCE, ContinuumDialect.POSTGRESQL);
  }
}
