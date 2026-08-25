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
import javax.sql.DataSource;

/**
 * Applies the reference PostgreSQL schema and empties every table — shared by the Postgres TCK run
 * and the wire-compatible certification runs (CockroachDB, YugabyteDB), which deliberately use the
 * <em>unmodified</em> Postgres DDL: whether it works there unchanged is part of what those runs
 * measure.
 */
final class TckSchema {

  private TckSchema() {}

  static void applyAndTruncate(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        InputStream schema =
            TckSchema.class.getResourceAsStream(
                "/org/jwcarman/continuum/jdbc/continuum-postgresql.sql")) {
      statement.execute(new String(schema.readAllBytes(), StandardCharsets.UTF_8));
      statement.execute(
          "TRUNCATE continuum_outbox, continuum_result, continuum_continuation, continuum_computation");
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("failed to prepare schema", e);
    }
  }
}
