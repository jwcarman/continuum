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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.jwcarman.accent.Accent;
import org.jwcarman.accent.AccentException;
import org.jwcarman.accent.Platform;
import org.jwcarman.continuum.api.CompletionDelivery;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

/**
 * JDBC persistence over a plain {@link DataSource}, certified on PostgreSQL 9.5+, MySQL 8+, MariaDB
 * 10.6+, Oracle 23ai+ and SQL Server 2012+ for production, and on H2 2.3+ for test/embedded use —
 * each passes the full TCK battery, concurrency suites included, on every build. Completion is a
 * single-transaction ownership transfer; outbox claiming uses {@code FOR UPDATE SKIP LOCKED} so
 * competing consumers never block. Schema is application-owned — see the classpath resources {@code
 * continuum-postgresql.sql} and {@code continuum-mysql.sql}.
 */
public final class JdbcContinuumRepository implements ContinuumRepository {

  private static final String ID_COLUMN = "id";
  private static final String KIND_COLUMN = "kind";
  private static final String COMPUTATION_ID_COLUMN = "computation_id";
  private static final String CONTINUATION_ID_COLUMN = "continuation_id";
  private static final String CONTINUATION_PAYLOAD_COLUMN = "continuation_payload";
  private static final String PAYLOAD_COLUMN = "payload";
  private static final String DISPATCH_PAYLOAD_COLUMN = "dispatch_payload";
  private static final String OUTCOME_TYPE_COLUMN = "outcome_type";
  private static final String OUTCOME_PAYLOAD_COLUMN = "outcome_payload";
  private static final String EXPIRY_KIND_COLUMN = "expiry_kind";
  private static final String MESSAGE_COLUMN = "message";
  private static final String DEADLINE_AT_COLUMN = "deadline_at";
  private static final String SUBMITTED_AT_COLUMN = "submitted_at";
  private static final String COMPLETED_AT_COLUMN = "completed_at";
  private static final String ATTEMPT_COUNT_COLUMN = "attempt_count";

  private final DataSource dataSource;
  // null means "detect on first use"; a preset dialect (escape hatches, withDialect) skips
  // detection entirely. Volatile with a benign race: concurrent first uses resolve identically.
  private volatile ContinuumDialect dialect;

  /**
   * Binds this repository to a data source. No schema is created or validated here — run {@code
   * continuum-postgresql.sql} before first use.
   *
   * <p>On first use — not at construction, so wiring a bean never opens a connection — the
   * repository verifies it is actually talking to PostgreSQL 9.5+ and refuses anything else,
   * loudly. This exists because wire-compatible databases lie: CockroachDB and YugabyteDB report
   * {@code PostgreSQL} through every metadata field a driver exposes and <em>accept</em> {@code FOR
   * UPDATE SKIP LOCKED} rather than rejecting it, so without detection the claim query parses,
   * runs, and warns nobody while the lock semantics the outbox's competing-consumer guarantee rests
   * on go unverified. Detection is one {@code SELECT version()} round trip, once per repository
   * instance.
   *
   * @param dataSource a data source for a certified platform; the application owns pooling and
   *     schema
   */
  public JdbcContinuumRepository(DataSource dataSource) {
    this(dataSource, null);
  }

  private JdbcContinuumRepository(DataSource dataSource, ContinuumDialect preset) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    this.dialect = preset;
  }

  /**
   * Binds this repository to a data source, skipping platform detection entirely — the escape hatch
   * for an operator who knows better than the driver's answer, at the cost of the
   * wire-compatible-impostor protection the detecting constructor provides.
   *
   * @param dataSource a data source the caller asserts is PostgreSQL 9.5+
   * @return a repository that will never run detection
   */
  public static JdbcContinuumRepository assumePostgreSql(DataSource dataSource) {
    return new JdbcContinuumRepository(dataSource, ContinuumDialect.POSTGRESQL);
  }

  /**
   * Binds this repository to a data source with an explicit dialect, skipping detection — the
   * extension point for a platform certified outside this project. Run the TCK against it first; a
   * dialect that binds correctly on a platform whose locking semantics break the contract is
   * exactly the silent failure the certified list exists to prevent.
   *
   * @param dataSource the data source
   * @param dialect the dialect to use, unconditionally
   * @return a repository that will never run detection
   */
  public static JdbcContinuumRepository withDialect(
      DataSource dataSource, ContinuumDialect dialect) {
    return new JdbcContinuumRepository(
        dataSource, Objects.requireNonNull(dialect, "dialect must not be null"));
  }

  private ContinuumDialect dialect() {
    return dialect;
  }

  @FunctionalInterface
  private interface SqlWork<T> {
    T perform(Connection connection) throws SQLException;
  }

  private <T> T inTransaction(SqlWork<T> work) {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      if (dialect == null) {
        resolveDialect(connection);
      }
      return commitOrRollback(work, connection);
    } catch (SQLException e) {
      throw new ContinuumPersistenceException("database operation failed", e);
    }
  }

  private void resolveDialect(Connection connection) {
    Platform platform;
    try {
      platform = Accent.of(connection);
    } catch (AccentException e) {
      throw new ContinuumPersistenceException("database platform detection failed", e);
    }
    // Identity first, capability second — the conjunction matters. CockroachDB and YugabyteDB
    // report supportsSkipLocked() = true and still failed TCK certification (see
    // persistence.md), so a capability-only gate would admit exactly the platforms the
    // certified list exists to refuse.
    ContinuumDialect resolved =
        switch (platform) {
          case Platform.PostgreSQL postgres when postgres.supportsSkipLocked() ->
              ContinuumDialect.POSTGRESQL;
          case Platform.MySQL mysql when mysql.supportsSkipLocked() -> ContinuumDialect.MYSQL;
          case Platform.MariaDB mariadb when mariadb.supportsSkipLocked() -> ContinuumDialect.MYSQL;
          // Certified for test/embedded use: passes the full TCK in both default and
          // PostgreSQL-compatibility modes, and speaks the PostgreSQL DDL verbatim.
          case Platform.H2 h2 when h2.supportsSkipLocked() -> ContinuumDialect.POSTGRESQL;
          case Platform.Oracle oracle when oracle.supportsSkipLocked() -> ContinuumDialect.ORACLE;
          // Deliberately NOT gated on supportsSkipLocked(): accent answers false for SQL Server
          // because that predicate covers the FOR UPDATE SKIP LOCKED clause specifically, and
          // this dialect never uses it — it locks with UPDLOCK/READPAST table hints instead.
          // The floor is 2012 (major 11), which introduced OFFSET/FETCH row limiting.
          case Platform.SqlServer sqlServer when sqlServer.majorVersion() >= 11 ->
              ContinuumDialect.SQLSERVER;
          default -> throw new ContinuumPersistenceException(refusal(platform));
        };
    dialect = resolved;
  }

  // Not an exhaustive switch on purpose: accent's Platform is sealed, and a new arm added there
  // must not break this compile — anything unrecognised falls through to the generic description.
  private static String refusal(Platform platform) {
    String detected =
        switch (platform) {
          case Platform.PostgreSQL postgres ->
              "PostgreSQL "
                  + postgres.majorVersion()
                  + "."
                  + postgres.minorVersion()
                  + ", which lacks FOR UPDATE SKIP LOCKED";
          case Platform.MySQL mysql ->
              "MySQL " + mysql.productVersion() + ", which lacks FOR UPDATE SKIP LOCKED";
          case Platform.MariaDB mariadb ->
              "MariaDB " + mariadb.productVersion() + ", which lacks FOR UPDATE SKIP LOCKED";
          case Platform.SqlServer sqlServer ->
              "SQL Server " + sqlServer.productVersion() + ", which predates OFFSET/FETCH (2012)";
          case Platform.CockroachDB cockroach ->
              "CockroachDB "
                  + cockroach.engine().raw()
                  + " (reports as PostgreSQL "
                  + cockroach.productVersion()
                  + ")";
          case Platform.YugabyteDB yugabyte ->
              "YugabyteDB "
                  + yugabyte.engine().raw()
                  + " (reports as PostgreSQL "
                  + yugabyte.productVersion()
                  + ")";
          default -> platform.productName() + " " + platform.productVersion();
        };
    return "unsupported database platform: "
        + detected
        + "; certified platforms: PostgreSQL 9.5+, MySQL 8+, MariaDB 10.6+, Oracle 23ai+,"
        + " SQL Server 2012+, H2 2.3+ (test/embedded only)."
        + " To use an uncertified platform anyway, construct via"
        + " JdbcContinuumRepository.withDialect(dataSource, dialect) — after running the TCK"
        + " against it.";
  }

  private static <T> T commitOrRollback(SqlWork<T> work, Connection connection)
      throws SQLException {
    try {
      T result = work.perform(connection);
      connection.commit();
      return result;
    } catch (SQLException | RuntimeException e) {
      connection.rollback();
      throw e;
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    inTransaction(
        connection -> {
          insertComputationRow(connection, computation);
          insertContinuation(connection, computation.id(), initial, computation.submittedAt());
          return null;
        });
  }

  private void insertComputationRow(Connection connection, Computation computation)
      throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO continuum_computation "
                + "(id, kind, deadline_at, dispatch_payload, attempt_count, submitted_at, last_updated_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
      dialect().setUuid(insert, 1, computation.id().value());
      insert.setString(2, computation.kind().value());
      insert.setTimestamp(3, Timestamp.from(computation.deadline()));
      insert.setBytes(4, computation.dispatchPayload());
      insert.setInt(5, computation.attemptCount());
      insert.setTimestamp(6, Timestamp.from(computation.submittedAt()));
      insert.setTimestamp(7, Timestamp.from(computation.submittedAt()));
      insert.executeUpdate();
    }
  }

  private void insertContinuation(
      Connection connection,
      ComputationId computationId,
      StoredContinuation continuation,
      Instant submittedAt)
      throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO continuum_continuation (id, computation_id, payload, created_at) "
                + "VALUES (?, ?, ?, ?)")) {
      dialect().setUuid(insert, 1, continuation.id().value());
      dialect().setUuid(insert, 2, computationId.value());
      insert.setBytes(3, continuation.payload());
      insert.setTimestamp(4, Timestamp.from(submittedAt));
      insert.executeUpdate();
    }
  }

  @Override
  public RegistrationOutcome registerContinuation(
      ComputationId id, StoredContinuation continuation) {
    return inTransaction(
        connection -> {
          Computation pending = lockAndReadPending(connection, id);
          if (pending != null) {
            insertContinuation(connection, id, continuation, pending.submittedAt());
            return new RegistrationOutcome.Registered();
          }
          return readResultOutcome(connection, id)
              .<RegistrationOutcome>map(RegistrationOutcome.Resolved::new)
              .orElseGet(RegistrationOutcome.NotFound::new);
        });
  }

  @Override
  public CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt) {
    return inTransaction(
        connection -> {
          Computation pending = lockAndReadPending(connection, id);
          if (pending == null) {
            return readResultOutcome(connection, id).isPresent()
                ? CompletionOutcome.ALREADY_RESOLVED
                : CompletionOutcome.NOT_FOUND;
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO continuum_result "
                      + "(computation_id, kind, outcome_type, outcome_payload, expiry_kind, message, "
                      + " deadline_at, attempt_count, submitted_at, completed_at) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            dialect().setUuid(insert, 1, id.value());
            insert.setString(2, pending.kind().value());
            setOutcomeColumns(insert, 3, outcome);
            insert.setTimestamp(7, Timestamp.from(pending.deadline()));
            insert.setInt(8, pending.attemptCount());
            insert.setTimestamp(9, Timestamp.from(pending.submittedAt()));
            insert.setTimestamp(10, Timestamp.from(completedAt));
            insert.executeUpdate();
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO continuum_outbox "
                      + "(id, computation_id, continuation_id, kind, continuation_payload, "
                      + " outcome_type, outcome_payload, expiry_kind, message, available_at, "
                      + " attempt_count, created_at, submitted_at, "
                      + " completed_at) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)")) {
            dialect().setUuid(insert, 2, id.value());
            insert.setString(4, pending.kind().value());
            setOutcomeColumns(insert, 6, outcome);
            insert.setTimestamp(10, Timestamp.from(completedAt));
            insert.setTimestamp(11, Timestamp.from(completedAt));
            insert.setTimestamp(12, Timestamp.from(pending.submittedAt()));
            insert.setTimestamp(13, Timestamp.from(completedAt));
            for (StoredContinuation continuation : readContinuations(connection, id)) {
              dialect().setUuid(insert, 1, DeliveryId.random().value());
              dialect().setUuid(insert, 3, continuation.id().value());
              insert.setBytes(5, continuation.payload());
              insert.addBatch();
            }
            insert.executeBatch();
          }
          executeUpdate(
              connection, "DELETE FROM continuum_continuation WHERE computation_id = ?", id);
          executeUpdate(connection, "DELETE FROM continuum_computation WHERE id = ?", id);
          return CompletionOutcome.COMPLETED;
        });
  }

  private void executeUpdate(Connection connection, String sql, ComputationId id)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      dialect().setUuid(statement, 1, id.value());
      statement.executeUpdate();
    }
  }

  private Computation lockAndReadPending(Connection connection, ComputationId id)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT kind, deadline_at, dispatch_payload, attempt_count, submitted_at "
                + "FROM continuum_computation"
                + dialect().pendingRowLock().tableHint()
                + " WHERE id = ?"
                + dialect().pendingRowLock().suffix())) {
      dialect().setUuid(select, 1, id.value());
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) {
          return null;
        }
        return pendingComputation(id, row);
      }
    }
  }

  private static Computation pendingComputation(ComputationId id, ResultSet row)
      throws SQLException {
    return new Computation(
        id,
        new ComputationKind(row.getString(KIND_COLUMN)),
        ComputationStatus.PENDING,
        row.getTimestamp(SUBMITTED_AT_COLUMN).toInstant(),
        row.getTimestamp(DEADLINE_AT_COLUMN).toInstant(),
        row.getBytes(DISPATCH_PAYLOAD_COLUMN),
        row.getInt(ATTEMPT_COUNT_COLUMN),
        null);
  }

  private List<StoredContinuation> readContinuations(Connection connection, ComputationId id)
      throws SQLException {
    List<StoredContinuation> continuations = new ArrayList<>();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT id, payload FROM continuum_continuation WHERE computation_id = ?")) {
      dialect().setUuid(select, 1, id.value());
      try (ResultSet row = select.executeQuery()) {
        while (row.next()) {
          continuations.add(
              new StoredContinuation(
                  new ContinuationId(dialect().getUuid(row, ID_COLUMN)),
                  row.getBytes(PAYLOAD_COLUMN)));
        }
      }
    }
    return continuations;
  }

  private Optional<Outcome> readResultOutcome(Connection connection, ComputationId id)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT outcome_type, outcome_payload, expiry_kind, message "
                + "FROM continuum_result WHERE computation_id = ?")) {
      dialect().setUuid(select, 1, id.value());
      try (ResultSet row = select.executeQuery()) {
        if (!row.next()) {
          return Optional.empty();
        }
        return Optional.of(readOutcome(row));
      }
    }
  }

  private static void setOutcomeColumns(
      PreparedStatement statement, int firstIndex, Outcome outcome) throws SQLException {
    switch (outcome) {
      case Outcome.Success(byte[] payload) -> {
        statement.setString(firstIndex, "SUCCESS");
        statement.setBytes(firstIndex + 1, payload);
        statement.setString(firstIndex + 2, null);
        statement.setString(firstIndex + 3, null);
      }
      case Outcome.Failure(String message) -> {
        statement.setString(firstIndex, "FAILURE");
        statement.setBytes(firstIndex + 1, null);
        statement.setString(firstIndex + 2, null);
        statement.setString(firstIndex + 3, message);
      }
      case Outcome.Expired(ExpiryKind expiryKind, String message) -> {
        statement.setString(firstIndex, "EXPIRED");
        statement.setBytes(firstIndex + 1, null);
        statement.setString(firstIndex + 2, expiryKind.name());
        statement.setString(firstIndex + 3, message);
      }
    }
  }

  private static Outcome readOutcome(ResultSet row) throws SQLException {
    return switch (row.getString(OUTCOME_TYPE_COLUMN)) {
      case "SUCCESS" -> Outcome.success(row.getBytes(OUTCOME_PAYLOAD_COLUMN));
      case "FAILURE" -> Outcome.failure(row.getString(MESSAGE_COLUMN));
      case "EXPIRED" ->
          Outcome.expired(
              ExpiryKind.valueOf(row.getString(EXPIRY_KIND_COLUMN)), row.getString(MESSAGE_COLUMN));
      default -> throw new ContinuumPersistenceException("unknown outcome_type");
    };
  }

  @Override
  public Optional<Computation> findComputation(ComputationId id) {
    return inTransaction(
        connection -> {
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT kind, deadline_at, dispatch_payload, attempt_count, submitted_at "
                      + "FROM continuum_computation WHERE id = ?")) {
            dialect().setUuid(select, 1, id.value());
            try (ResultSet row = select.executeQuery()) {
              if (row.next()) {
                return Optional.of(pendingComputation(id, row));
              }
            }
          }
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT kind, outcome_type, outcome_payload, expiry_kind, message, "
                      + " deadline_at, attempt_count, submitted_at "
                      + "FROM continuum_result WHERE computation_id = ?")) {
            dialect().setUuid(select, 1, id.value());
            try (ResultSet row = select.executeQuery()) {
              if (!row.next()) {
                return Optional.empty();
              }
              Outcome outcome = readOutcome(row);
              return Optional.of(
                  new Computation(
                      id,
                      new ComputationKind(row.getString(KIND_COLUMN)),
                      Outcome.statusOf(outcome),
                      row.getTimestamp(SUBMITTED_AT_COLUMN).toInstant(),
                      row.getTimestamp(DEADLINE_AT_COLUMN).toInstant(),
                      null,
                      row.getInt(ATTEMPT_COUNT_COLUMN),
                      outcome));
            }
          }
        });
  }

  @Override
  public List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now) {
    return inTransaction(
        connection -> {
          List<ClaimedDelivery> claimed = new ArrayList<>();
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT id, computation_id, continuation_id, continuation_payload, "
                      + " outcome_type, outcome_payload, expiry_kind, message, attempt_count, "
                      + " submitted_at, completed_at "
                      + "FROM continuum_outbox"
                      + dialect().claimLock().tableHint()
                      + " WHERE kind = ? AND available_at <= ? "
                      + " AND (claimed_until IS NULL OR claimed_until <= ?) "
                      + "ORDER BY available_at"
                      + (dialect().limitsLockingReads() ? dialect().limitClause() : "")
                      + dialect().claimLock().suffix())) {
            select.setString(1, kind.value());
            select.setTimestamp(2, Timestamp.from(now));
            select.setTimestamp(3, Timestamp.from(now));
            if (dialect().limitsLockingReads()) {
              select.setInt(4, limit);
            }
            try (ResultSet row = select.executeQuery()) {
              // The size check is the whole limit on dialects whose locking reads cannot carry
              // one; elsewhere it is redundant with the SQL and harmless.
              while (claimed.size() < limit && row.next()) {
                claimed.add(
                    new ClaimedDelivery(
                        new DeliveryId(dialect().getUuid(row, ID_COLUMN)),
                        new CompletionDelivery(
                            new ComputationId(dialect().getUuid(row, COMPUTATION_ID_COLUMN)),
                            kind,
                            new ContinuationId(dialect().getUuid(row, CONTINUATION_ID_COLUMN)),
                            row.getBytes(CONTINUATION_PAYLOAD_COLUMN),
                            readOutcome(row),
                            row.getTimestamp(SUBMITTED_AT_COLUMN).toInstant(),
                            row.getTimestamp(COMPLETED_AT_COLUMN).toInstant()),
                        row.getInt(ATTEMPT_COUNT_COLUMN)));
              }
            }
          }
          try (PreparedStatement update =
              connection.prepareStatement(
                  "UPDATE continuum_outbox SET claimed_by = ?, claimed_until = ? WHERE id = ?")) {
            update.setString(1, workerId);
            update.setTimestamp(2, Timestamp.from(now.plus(lease)));
            for (ClaimedDelivery delivery : claimed) {
              dialect().setUuid(update, 3, delivery.id().value());
              update.addBatch();
            }
            update.executeBatch();
          }
          return claimed;
        });
  }

  @Override
  public void acknowledgeDelivery(DeliveryId id) {
    inTransaction(
        connection -> {
          try (PreparedStatement delete =
              connection.prepareStatement("DELETE FROM continuum_outbox WHERE id = ?")) {
            dialect().setUuid(delete, 1, id.value());
            delete.executeUpdate();
          }
          return null;
        });
  }

  @Override
  public void releaseDelivery(DeliveryId id, Instant retryAt) {
    inTransaction(
        connection -> {
          try (PreparedStatement update =
              connection.prepareStatement(
                  "UPDATE continuum_outbox SET claimed_by = NULL, claimed_until = NULL, "
                      + " available_at = ?, attempt_count = attempt_count + 1 WHERE id = ?")) {
            update.setTimestamp(1, Timestamp.from(retryAt));
            dialect().setUuid(update, 2, id.value());
            update.executeUpdate();
          }
          return null;
        });
  }

  @Override
  public List<Computation> findExpired(ComputationKind kind, Instant now, int limit) {
    return inTransaction(
        connection -> {
          List<Computation> expired = new ArrayList<>();
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT id, kind, deadline_at, dispatch_payload, attempt_count, submitted_at "
                      + "FROM continuum_computation WHERE kind = ? AND deadline_at <= ? "
                      + "ORDER BY deadline_at"
                      + dialect().limitClause())) {
            select.setString(1, kind.value());
            select.setTimestamp(2, Timestamp.from(now));
            select.setInt(3, limit);
            try (ResultSet row = select.executeQuery()) {
              while (row.next()) {
                expired.add(
                    pendingComputation(new ComputationId(dialect().getUuid(row, ID_COLUMN)), row));
              }
            }
          }
          return expired;
        });
  }

  @Override
  public void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount) {
    inTransaction(
        connection -> {
          try (PreparedStatement update =
              connection.prepareStatement(
                  "UPDATE continuum_computation SET deadline_at = ?, attempt_count = ?, "
                      + " last_updated_at = CURRENT_TIMESTAMP WHERE id = ?")) {
            update.setTimestamp(1, Timestamp.from(newDeadline));
            update.setInt(2, attemptCount);
            dialect().setUuid(update, 3, id.value());
            update.executeUpdate();
          }
          return null;
        });
  }

  @Override
  public int purgeResults(ComputationKind kind, Instant olderThan, int limit) {
    return inTransaction(
        connection -> {
          try (PreparedStatement delete =
              connection.prepareStatement(
                  // The derived table is not decoration: MySQL and MariaDB reject LIMIT
                  // directly inside an IN subquery, and separately refuse to delete from a
                  // table being selected in the same statement — wrapping the subquery in a
                  // derived table sidesteps both, and PostgreSQL accepts the same text.
                  "DELETE FROM continuum_result WHERE computation_id IN ("
                      + "SELECT computation_id FROM ("
                      + "SELECT computation_id FROM continuum_result "
                      // Oldest first, on every dialect: SQL Server's OFFSET/FETCH requires an
                      // ORDER BY, and purging the oldest results first is the right order anyway.
                      + "WHERE kind = ? AND completed_at < ? ORDER BY completed_at"
                      + dialect().limitClause()
                      + ") purgeable)")) {
            delete.setString(1, kind.value());
            delete.setTimestamp(2, Timestamp.from(olderThan));
            delete.setInt(3, limit);
            return delete.executeUpdate();
          }
        });
  }
}
