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
import java.util.UUID;
import javax.sql.DataSource;
import org.jwcarman.continuum.CompletionDelivery;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.ContinuationId;
import org.jwcarman.continuum.ExpiryKind;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

public final class JdbcContinuumRepository implements ContinuumRepository {

  private static final String ATTEMPT_COUNT_COLUMN = "attempt_count";

  private final DataSource dataSource;

  public JdbcContinuumRepository(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
  }

  @FunctionalInterface
  private interface SqlWork<T> {
    T perform(Connection connection) throws SQLException;
  }

  private <T> T inTransaction(SqlWork<T> work) {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        T result = work.perform(connection);
        connection.commit();
        return result;
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      }
    } catch (SQLException e) {
      throw new ContinuumPersistenceException("database operation failed", e);
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    inTransaction(
        connection -> {
          insertComputationRow(connection, computation);
          insertContinuation(connection, computation.id(), initial, computation.createdAt());
          return null;
        });
  }

  private void insertComputationRow(Connection connection, Computation computation)
      throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO continuum_computation "
                + "(id, kind, deadline_at, dispatch_payload, attempt_count, created_at, last_updated_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
      insert.setObject(1, computation.id().value());
      insert.setString(2, computation.kind().value());
      insert.setTimestamp(3, Timestamp.from(computation.deadline()));
      insert.setBytes(4, computation.dispatchPayload());
      insert.setInt(5, computation.attemptCount());
      insert.setTimestamp(6, Timestamp.from(computation.createdAt()));
      insert.setTimestamp(7, Timestamp.from(computation.createdAt()));
      insert.executeUpdate();
    }
  }

  private void insertContinuation(
      Connection connection,
      ComputationId computationId,
      StoredContinuation continuation,
      Instant createdAt)
      throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO continuum_continuation (id, computation_id, payload, created_at) "
                + "VALUES (?, ?, ?, ?)")) {
      insert.setObject(1, continuation.id().value());
      insert.setObject(2, computationId.value());
      insert.setBytes(3, continuation.payload());
      insert.setTimestamp(4, Timestamp.from(createdAt));
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
            insertContinuation(connection, id, continuation, pending.createdAt());
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
                      + " deadline_at, attempt_count, created_at, completed_at) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            insert.setObject(1, id.value());
            insert.setString(2, pending.kind().value());
            setOutcomeColumns(insert, 3, outcome);
            insert.setTimestamp(7, Timestamp.from(pending.deadline()));
            insert.setInt(8, pending.attemptCount());
            insert.setTimestamp(9, Timestamp.from(pending.createdAt()));
            insert.setTimestamp(10, Timestamp.from(completedAt));
            insert.executeUpdate();
          }
          try (PreparedStatement insert =
              connection.prepareStatement(
                  "INSERT INTO continuum_outbox "
                      + "(id, computation_id, continuation_id, kind, continuation_payload, "
                      + " outcome_type, outcome_payload, expiry_kind, message, available_at, "
                      + " attempt_count, created_at) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)")) {
            insert.setObject(2, id.value());
            insert.setString(4, pending.kind().value());
            setOutcomeColumns(insert, 6, outcome);
            insert.setTimestamp(10, Timestamp.from(completedAt));
            insert.setTimestamp(11, Timestamp.from(completedAt));
            for (StoredContinuation continuation : readContinuations(connection, id)) {
              insert.setObject(1, DeliveryId.random().value());
              insert.setObject(3, continuation.id().value());
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
      statement.setObject(1, id.value());
      statement.executeUpdate();
    }
  }

  private Computation lockAndReadPending(Connection connection, ComputationId id)
      throws SQLException {
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT kind, deadline_at, dispatch_payload, attempt_count, created_at "
                + "FROM continuum_computation WHERE id = ? FOR UPDATE")) {
      select.setObject(1, id.value());
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
        new ComputationKind(row.getString("kind")),
        ComputationStatus.PENDING,
        row.getTimestamp("created_at").toInstant(),
        row.getTimestamp("deadline_at").toInstant(),
        row.getBytes("dispatch_payload"),
        row.getInt(ATTEMPT_COUNT_COLUMN),
        null);
  }

  private List<StoredContinuation> readContinuations(Connection connection, ComputationId id)
      throws SQLException {
    List<StoredContinuation> continuations = new ArrayList<>();
    try (PreparedStatement select =
        connection.prepareStatement(
            "SELECT id, payload FROM continuum_continuation WHERE computation_id = ?")) {
      select.setObject(1, id.value());
      try (ResultSet row = select.executeQuery()) {
        while (row.next()) {
          continuations.add(
              new StoredContinuation(
                  new ContinuationId(row.getObject("id", UUID.class)), row.getBytes("payload")));
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
      select.setObject(1, id.value());
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
    return switch (row.getString("outcome_type")) {
      case "SUCCESS" -> Outcome.success(row.getBytes("outcome_payload"));
      case "FAILURE" -> Outcome.failure(row.getString("message"));
      case "EXPIRED" ->
          Outcome.expired(
              ExpiryKind.valueOf(row.getString("expiry_kind")), row.getString("message"));
      default -> throw new ContinuumPersistenceException("unknown outcome_type");
    };
  }

  @Override
  public Optional<Computation> findComputation(ComputationId id) {
    return inTransaction(
        connection -> {
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT kind, deadline_at, dispatch_payload, attempt_count, created_at "
                      + "FROM continuum_computation WHERE id = ?")) {
            select.setObject(1, id.value());
            try (ResultSet row = select.executeQuery()) {
              if (row.next()) {
                return Optional.of(pendingComputation(id, row));
              }
            }
          }
          try (PreparedStatement select =
              connection.prepareStatement(
                  "SELECT kind, outcome_type, outcome_payload, expiry_kind, message, "
                      + " deadline_at, attempt_count, created_at "
                      + "FROM continuum_result WHERE computation_id = ?")) {
            select.setObject(1, id.value());
            try (ResultSet row = select.executeQuery()) {
              if (!row.next()) {
                return Optional.empty();
              }
              Outcome outcome = readOutcome(row);
              return Optional.of(
                  new Computation(
                      id,
                      new ComputationKind(row.getString("kind")),
                      Outcome.statusOf(outcome),
                      row.getTimestamp("created_at").toInstant(),
                      row.getTimestamp("deadline_at").toInstant(),
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
                      + " outcome_type, outcome_payload, expiry_kind, message, attempt_count "
                      + "FROM continuum_outbox "
                      + "WHERE kind = ? AND available_at <= ? "
                      + " AND (claimed_until IS NULL OR claimed_until <= ?) "
                      + "ORDER BY available_at LIMIT ? FOR UPDATE SKIP LOCKED")) {
            select.setString(1, kind.value());
            select.setTimestamp(2, Timestamp.from(now));
            select.setTimestamp(3, Timestamp.from(now));
            select.setInt(4, limit);
            try (ResultSet row = select.executeQuery()) {
              while (row.next()) {
                claimed.add(
                    new ClaimedDelivery(
                        new DeliveryId(row.getObject("id", UUID.class)),
                        new CompletionDelivery(
                            new ComputationId(row.getObject("computation_id", UUID.class)),
                            kind,
                            new ContinuationId(row.getObject("continuation_id", UUID.class)),
                            row.getBytes("continuation_payload"),
                            readOutcome(row)),
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
              update.setObject(3, delivery.id().value());
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
            delete.setObject(1, id.value());
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
            update.setObject(2, id.value());
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
                  "SELECT id, kind, deadline_at, dispatch_payload, attempt_count, created_at "
                      + "FROM continuum_computation WHERE kind = ? AND deadline_at <= ? "
                      + "ORDER BY deadline_at LIMIT ?")) {
            select.setString(1, kind.value());
            select.setTimestamp(2, Timestamp.from(now));
            select.setInt(3, limit);
            try (ResultSet row = select.executeQuery()) {
              while (row.next()) {
                expired.add(
                    pendingComputation(new ComputationId(row.getObject("id", UUID.class)), row));
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
            update.setObject(3, id.value());
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
                  "DELETE FROM continuum_result WHERE computation_id IN ("
                      + "SELECT computation_id FROM continuum_result "
                      + "WHERE kind = ? AND completed_at < ? LIMIT ?)")) {
            delete.setString(1, kind.value());
            delete.setTimestamp(2, Timestamp.from(olderThan));
            delete.setInt(3, limit);
            return delete.executeUpdate();
          }
        });
  }
}
