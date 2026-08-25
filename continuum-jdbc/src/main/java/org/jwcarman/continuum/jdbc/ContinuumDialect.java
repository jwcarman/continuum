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
 * <p>The query shapes are nearly identical across PostgreSQL, MySQL, MariaDB, Oracle and SQL
 * Server. What differs: how identities bind (PostgreSQL has a native {@code uuid} type; the others
 * store 36-character strings), the type names in each platform's reference DDL (in the per-dialect
 * {@code .sql} resource, not here), the spelling of the row-limit clause, and two real behavioral
 * differences — Oracle cannot lock through a row-limited read, so its claim query stops fetching
 * after {@code limit} rows instead of saying so in SQL; and SQL Server has no {@code FOR UPDATE}
 * clause at all, expressing both the blocking pending-row lock and the skip-locked claim as table
 * hints ({@code UPDLOCK, ROWLOCK} and {@code UPDLOCK, READPAST, ROWLOCK}).
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
   * Oracle 23ai+: {@code VARCHAR2(36)} identities, {@code FETCH FIRST} row limiting, and — the one
   * genuinely different behavior — a locking read that cannot carry a row limit.
   */
  ContinuumDialect ORACLE = new OracleDialect();

  /**
   * SQL Server 2012+: {@code CHAR(36)} identities, {@code OFFSET/FETCH} row limiting, and locking
   * expressed as table hints rather than a trailing {@code FOR UPDATE} clause — {@code UPDLOCK,
   * ROWLOCK} to lock, plus {@code READPAST} to skip rows another claimer holds.
   */
  ContinuumDialect SQLSERVER = new SqlServerDialect();

  /**
   * How a locking read is spelled: a table hint placed immediately after the table name, and a
   * suffix appended after {@code ORDER BY}/limit. Every dialect but SQL Server uses the suffix form
   * ({@code FOR UPDATE} / {@code FOR UPDATE SKIP LOCKED}); SQL Server uses only the hint form.
   *
   * @param tableHint text after the table name, or empty
   * @param suffix text at the end of the statement, or empty
   */
  record Locking(String tableHint, String suffix) {}

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

  /**
   * The row-limiting clause, with one positional parameter for the count, to append after {@code
   * ORDER BY}: {@code LIMIT ?} on PostgreSQL/MySQL, {@code FETCH FIRST ? ROWS ONLY} on Oracle.
   *
   * @return the clause including its leading space
   */
  default String limitClause() {
    return " LIMIT ?";
  }

  /**
   * Whether {@link #limitClause()} may accompany {@code FOR UPDATE SKIP LOCKED}. Oracle rejects a
   * row-limited locking read (its row limiting is an inline view, and views cannot be locked), so
   * there the claim query omits the clause and the provider stops reading after {@code limit} rows
   * instead — Oracle locks rows as they are fetched, which is the Oracle AQ dequeue idiom.
   *
   * @return true if the claim query may carry the limit clause
   */
  default boolean limitsLockingReads() {
    return true;
  }

  /**
   * The blocking lock taken on a computation's pending row before mutating it — the single lock
   * ordering every multi-statement transaction opens with.
   *
   * @return the locking syntax
   */
  default Locking pendingRowLock() {
    return new Locking("", " FOR UPDATE");
  }

  /**
   * The non-blocking lock taken when claiming outbox rows: competing claimers must skip, not wait.
   *
   * @return the locking syntax
   */
  default Locking claimLock() {
    return new Locking("", " FOR UPDATE SKIP LOCKED");
  }

  /**
   * Shared identity binding for every platform without a native UUID type: identities travel as
   * their canonical 36-character text form, which sorts identically to UUIDv7 byte order.
   */
  abstract class StringUuidDialect implements ContinuumDialect {
    @Override
    public void setUuid(PreparedStatement statement, int index, UUID uuid) throws SQLException {
      statement.setString(index, uuid.toString());
    }

    @Override
    public UUID getUuid(ResultSet row, String column) throws SQLException {
      return UUID.fromString(row.getString(column));
    }
  }

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

  final class OracleDialect extends StringUuidDialect {
    private OracleDialect() {}

    @Override
    public String schemaResource() {
      return "/org/jwcarman/continuum/jdbc/continuum-oracle.sql";
    }

    @Override
    public String limitClause() {
      return " FETCH FIRST ? ROWS ONLY";
    }

    @Override
    public boolean limitsLockingReads() {
      return false;
    }
  }

  final class SqlServerDialect extends StringUuidDialect {
    private SqlServerDialect() {}

    @Override
    public String schemaResource() {
      return "/org/jwcarman/continuum/jdbc/continuum-sqlserver.sql";
    }

    @Override
    public String limitClause() {
      return " OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY";
    }

    @Override
    public Locking pendingRowLock() {
      return new Locking(" WITH (UPDLOCK, ROWLOCK)", "");
    }

    @Override
    public Locking claimLock() {
      return new Locking(" WITH (UPDLOCK, READPAST, ROWLOCK)", "");
    }
  }

  final class MySqlDialect extends StringUuidDialect {
    private MySqlDialect() {}

    @Override
    public String schemaResource() {
      return "/org/jwcarman/continuum/jdbc/continuum-mysql.sql";
    }
  }
}
