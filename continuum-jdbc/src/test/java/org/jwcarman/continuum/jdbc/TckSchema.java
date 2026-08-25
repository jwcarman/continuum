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
 * Schema lifecycle for the certification suites, portable across every dialect under test.
 *
 * <p>Three MySQL facts shape this class, none of which PostgreSQL forced on the earlier version:
 * the driver will not execute a multi-statement script in one {@code execute}, so the DDL is split
 * and run statement by statement; {@code CREATE INDEX} has no {@code IF NOT EXISTS}, so the schema
 * is applied once per container (from each suite's static initializer) rather than per test; and
 * {@code TRUNCATE} refuses any table referenced by a foreign key, so per-test cleanup is ordered
 * {@code DELETE}s — child before parent — which every platform accepts.
 */
final class TckSchema {

  private TckSchema() {}

  /** Applies the dialect's reference DDL. Call once per container, not per test. */
  static void applySchema(DataSource dataSource, ContinuumDialect dialect) {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        InputStream schema = TckSchema.class.getResourceAsStream(dialect.schemaResource())) {
      String ddl = new String(schema.readAllBytes(), StandardCharsets.UTF_8);
      // Strip line comments BEFORE splitting on semicolons: a semicolon inside a comment would
      // otherwise shear the comment into a fragment that gets executed as a "statement".
      String uncommented = ddl.replaceAll("--[^\n]*", "");
      for (String sql : uncommented.split(";")) {
        String trimmed = sql.trim();
        if (!trimmed.isEmpty()) {
          statement.execute(trimmed);
        }
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("failed to apply schema", e);
    }
  }

  /** Empties every table, child before parent. Call before each test. */
  static void truncate(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM continuum_outbox");
      statement.execute("DELETE FROM continuum_result");
      statement.execute("DELETE FROM continuum_continuation");
      statement.execute("DELETE FROM continuum_computation");
    } catch (SQLException e) {
      throw new IllegalStateException("failed to truncate schema", e);
    }
  }
}
