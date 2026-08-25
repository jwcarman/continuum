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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * What actually varies between certified platforms — which is deliberately almost nothing.
 *
 * <p>The query shapes are identical across PostgreSQL, MySQL 8+ and MariaDB 10.6+: {@code FOR
 * UPDATE SKIP LOCKED}, {@code LIMIT ?} and the rest are valid unchanged on all three. What differs
 * is one JDBC-layer fact — PostgreSQL has a native {@code uuid} type that pgjdbc binds via {@code
 * setObject}/{@code getObject}, while MySQL and MariaDB store identities as {@code CHAR(36)} and
 * bind them as strings — plus the type names in each platform's reference DDL, which live in the
 * per-dialect {@code .sql} resource rather than here.
 *
 * <p>Time-ordering survives the string representation: identities are UUIDv7, whose canonical text
 * form sorts identically to its byte order, so {@code CHAR(36)} keys keep the index-locality
 * property the v7 switch bought.
 *
 * <p>Open rather than sealed, on purpose: unlike a vocabulary describing the world, this is an
 * extension point — someone certifying a platform we have not can supply their own dialect via
 * {@link JdbcContinuumRepository#withDialect} without waiting on a release. Resist adding methods
 * speculatively; every one is a promise to be correct on every certified platform forever.
 */
public interface ContinuumDialect {

  /** PostgreSQL 9.5+: native {@code uuid} columns, bound as objects. */
  ContinuumDialect POSTGRESQL = new PostgresDialect();

  /** MySQL 8+ and MariaDB 10.6+: {@code CHAR(36)} columns, bound as canonical UUID strings. */
  ContinuumDialect MYSQL = new MySqlDialect();

  /**
   * Binds an identity parameter.
   *
   * @param statement the statement
   * @param index the 1-based parameter index
   * @param uuid the identity
   * @throws SQLException on driver failure
   */
  void setUuid(PreparedStatement statement, int index, UUID uuid) throws SQLException;

  /**
   * Reads an identity column.
   *
   * @param row the result row
   * @param column the column label
   * @return the identity
   * @throws SQLException on driver failure
   */
  UUID getUuid(ResultSet row, String column) throws SQLException;

  /**
   * The classpath location of this dialect's reference DDL.
   *
   * @return an absolute classpath resource path
   */
  String schemaResource();

  final class PostgresDialect implements ContinuumDialect {
    private PostgresDialect() {}

    @Override
    public void setUuid(PreparedStatement statement, int index, UUID uuid) throws SQLException {
      statement.setObject(index, uuid);
    }

    @Override
    public UUID getUuid(ResultSet row, String column) throws SQLException {
      return row.getObject(column, UUID.class);
    }

    @Override
    public String schemaResource() {
      return "/org/jwcarman/continuum/jdbc/continuum-postgresql.sql";
    }
  }

  final class MySqlDialect implements ContinuumDialect {
    private MySqlDialect() {}

    @Override
    public void setUuid(PreparedStatement statement, int index, UUID uuid) throws SQLException {
      statement.setString(index, uuid.toString());
    }

    @Override
    public UUID getUuid(ResultSet row, String column) throws SQLException {
      return UUID.fromString(row.getString(column));
    }

    @Override
    public String schemaResource() {
      return "/org/jwcarman/continuum/jdbc/continuum-mysql.sql";
    }
  }
}
