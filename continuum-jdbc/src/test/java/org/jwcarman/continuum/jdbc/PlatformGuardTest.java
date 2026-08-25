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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * The platform guard exists because wire-compatible databases lie: CockroachDB reports {@code
 * PostgreSQL 13.0.0} through every metadata field and accepts {@code FOR UPDATE SKIP LOCKED}, so
 * without detection the claim query parses, runs, and warns nobody while the lock semantics the
 * outbox rests on go unverified. These tests drive the guard through mocked JDBC objects that
 * answer exactly what real drivers were measured to answer (accent's observed-strings data).
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PlatformGuardTest {

  /**
   * A DataSource whose connection reports the given metadata, answers {@code SELECT version()} with
   * {@code selectVersion} (accent queries it only for the Postgres family), and returns empty
   * results for any prepared statement so a permitted operation can complete.
   */
  private static DataSource database(
      String productName, String productVersion, int major, int minor, String selectVersion)
      throws SQLException {
    DataSource dataSource = mock();
    Connection connection = mock();
    DatabaseMetaData metaData = mock();
    Statement statement = mock();
    ResultSet versionResult = mock();
    PreparedStatement prepared = mock();
    ResultSet emptyResult = mock();

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    // accent's of(Connection) walks back to the connection via the metadata for SELECT version()
    when(metaData.getConnection()).thenReturn(connection);
    when(metaData.getDatabaseProductName()).thenReturn(productName);
    when(metaData.getDatabaseProductVersion()).thenReturn(productVersion);
    when(metaData.getDatabaseMajorVersion()).thenReturn(major);
    when(metaData.getDatabaseMinorVersion()).thenReturn(minor);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT version()")).thenReturn(versionResult);
    when(versionResult.next()).thenReturn(true);
    when(versionResult.getString(1)).thenReturn(selectVersion);
    when(connection.prepareStatement(anyString())).thenReturn(prepared);
    when(prepared.executeQuery()).thenReturn(emptyResult);
    when(emptyResult.next()).thenReturn(false);
    return dataSource;
  }

  private static void anyOperation(JdbcContinuumRepository repository) {
    repository.findComputation(ComputationId.random());
  }

  @Nested
  class Permitting {
    @Test
    void genuine_postgresql_passes_and_operations_proceed() throws SQLException {
      var repository =
          new JdbcContinuumRepository(
              database(
                  "PostgreSQL",
                  "17.10 (Debian 17.10-1.pgdg13+1)",
                  17,
                  10,
                  "PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on aarch64-unknown-linux-gnu"));

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mysql_8_passes() throws SQLException {
      var repository = new JdbcContinuumRepository(database("MySQL", "8.4.11", 8, 4, "unused"));
      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mariadb_via_its_own_driver_passes() throws SQLException {
      var repository =
          new JdbcContinuumRepository(
              database("MariaDB", "11.4.12-MariaDB-ubu2404", 11, 4, "unused"));
      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mariadb_reached_through_mysql_connector_is_recognised_and_passes() throws SQLException {
      // Measured: mysql-connector-j against a MariaDB server reports productName MySQL; only the
      // version string names the real engine. accent disambiguates to the MariaDB arm, whose
      // 10.6+ skip-locked floor 11.4 clears.
      var repository =
          new JdbcContinuumRepository(
              database("MySQL", "11.4.12-MariaDB-ubu2404", 11, 4, "unused"));
      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void detection_runs_once_not_per_operation() throws SQLException {
      var dataSource =
          database("PostgreSQL", "17.10", 17, 10, "PostgreSQL 17.10 on aarch64-unknown-linux-gnu");
      var repository = new JdbcContinuumRepository(dataSource);

      anyOperation(repository);
      anyOperation(repository);
      anyOperation(repository);

      verify(dataSource.getConnection().getMetaData(), times(1)).getDatabaseProductName();
    }
  }

  @Nested
  class Refusing {
    @Test
    void cockroachdb_is_refused_naming_both_identities() throws SQLException {
      // Measured: CockroachDB v24.1 through pgjdbc reports PostgreSQL 13.0.0 with no marker
      // anywhere in the metadata; only SELECT version() names the real engine.
      var repository =
          new JdbcContinuumRepository(
              database(
                  "PostgreSQL",
                  "13.0.0",
                  13,
                  0,
                  "CockroachDB CCL v24.1.0 (aarch64-unknown-linux-gnu)"));

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("CockroachDB")
          .withMessageContaining("v24.1.0")
          .withMessageContaining("reports as PostgreSQL")
          .withMessageContaining("withDialect");
    }

    @Test
    void postgresql_before_9_5_is_refused_for_lacking_skip_locked() throws SQLException {
      var repository =
          new JdbcContinuumRepository(
              database("PostgreSQL", "9.4.26", 9, 4, "PostgreSQL 9.4.26 on x86_64"));

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("PostgreSQL 9.4")
          .withMessageContaining("SKIP LOCKED");
    }

    @Test
    void mysql_5_7_is_refused_for_lacking_skip_locked() throws SQLException {
      var repository = new JdbcContinuumRepository(database("MySQL", "5.7.44", 5, 7, "unused"));

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("MySQL 5.7")
          .withMessageContaining("SKIP LOCKED");
    }

    @Test
    void an_unrelated_database_is_refused_by_name() throws SQLException {
      var repository =
          new JdbcContinuumRepository(database("H2", "2.3.232 (2024-08-11)", 2, 3, "unused"));

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("H2")
          .withMessageContaining("certified platforms");
    }

    @Test
    void refusal_repeats_on_every_attempt_rather_than_latching_open() throws SQLException {
      var repository = new JdbcContinuumRepository(database("H2", "2.3.232", 2, 3, "unused"));

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository));
      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository));
    }
  }

  @Nested
  class The_escape_hatch {
    @Test
    void assume_postgresql_never_touches_metadata() throws SQLException {
      var dataSource = database("H2", "2.3.232", 2, 3, "unused");
      var repository = JdbcContinuumRepository.assumePostgreSql(dataSource);

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
      verify(dataSource.getConnection(), never()).getMetaData();
    }
  }

  @Nested
  class Construction {
    @Test
    void constructing_opens_no_connection() throws SQLException {
      var dataSource = database("PostgreSQL", "17.10", 17, 10, "PostgreSQL 17.10");

      new JdbcContinuumRepository(dataSource);

      verify(dataSource, never()).getConnection();
    }
  }

  @Test
  void guard_is_wired_through_the_single_transaction_funnel() throws SQLException {
    // Belt and braces for the design assumption the guard rests on: a claim, not just a find,
    // hits detection. If a future operation bypasses inTransaction, this cannot catch it — but
    // the two most different operation shapes both passing through is strong evidence.
    var repository = new JdbcContinuumRepository(database("H2", "2.3.232", 2, 3, "unused"));

    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(
            () ->
                repository.claimDeliveries(
                    "worker", new ComputationKind("k"), 10, Duration.ofSeconds(30), Instant.now()));
  }
}
