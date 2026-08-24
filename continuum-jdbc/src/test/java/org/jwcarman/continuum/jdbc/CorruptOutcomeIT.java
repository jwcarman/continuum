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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CorruptOutcomeIT {

  @Test
  void an_unknown_outcome_type_row_is_rejected_loudly() throws SQLException {
    DataSource dataSource = PostgresSupport.dataSource();
    PostgresSupport.applySchemaAndTruncate(dataSource);
    var id = ComputationId.random();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement insert =
            connection.prepareStatement(
                "INSERT INTO continuum_result "
                    + "(computation_id, kind, outcome_type, deadline_at, attempt_count, submitted_at, completed_at) "
                    + "VALUES (?, 'k', 'GARBAGE', ?, 1, ?, ?)")) {
      insert.setObject(1, UUID.fromString(id.value().toString()));
      Timestamp now = Timestamp.from(Instant.now());
      insert.setTimestamp(2, now);
      insert.setTimestamp(3, now);
      insert.setTimestamp(4, now);
      insert.executeUpdate();
    }

    var repository = new JdbcContinuumRepository(dataSource);
    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(() -> repository.findComputation(id));
  }
}
