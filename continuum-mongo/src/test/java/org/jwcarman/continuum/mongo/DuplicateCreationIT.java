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
package org.jwcarman.continuum.mongo;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.StoredContinuation;
import org.testcontainers.containers.MongoDBContainer;

/**
 * Pins the duplicate-creation refusal on a real server: the pending-collection pre-check inside the
 * transaction (a repeated {@code createComputation} while still pending) and the results-collection
 * pre-check (a repeated {@code createComputation} after the computation has completed).
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DuplicateCreationIT {

  private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.2");
  private static final MongoClient CLIENT;
  private static final String DATABASE = "continuum_duplicates";

  static {
    MONGO.start();
    CLIENT = MongoClients.create(MONGO.getConnectionString());
  }

  private MongoDatabase database;
  private MongoContinuumRepository repository;

  @BeforeEach
  void setUp() {
    database = CLIENT.getDatabase(DATABASE);
    database.drop();
    repository = new MongoContinuumRepository(CLIENT, DATABASE);
  }

  private static Computation computation(ComputationId id, Instant submittedAt, Instant deadline) {
    return new Computation(
        id,
        new ComputationKind("k"),
        ComputationStatus.PENDING,
        submittedAt,
        deadline,
        null,
        1,
        null);
  }

  private static StoredContinuation continuation() {
    return new StoredContinuation(ContinuationId.random(), new byte[0]);
  }

  @Test
  void creating_the_same_id_twice_while_pending_is_refused() {
    ComputationId id = ComputationId.random();
    Instant submittedAt = Instant.now();
    Instant deadline = submittedAt.plus(1, ChronoUnit.HOURS);
    repository.createComputation(computation(id, submittedAt, deadline), continuation());

    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(
            () ->
                repository.createComputation(
                    computation(id, submittedAt, deadline), continuation()))
        .withMessageContaining("duplicate computation id");
  }

  @Test
  void creating_the_same_id_again_after_completion_is_refused() {
    ComputationId id = ComputationId.random();
    Instant submittedAt = Instant.now();
    Instant deadline = submittedAt.plus(1, ChronoUnit.HOURS);
    repository.createComputation(computation(id, submittedAt, deadline), continuation());
    repository.complete(id, new Outcome.Success(new byte[0]), Instant.now());

    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(
            () ->
                repository.createComputation(
                    computation(id, submittedAt, deadline), continuation()))
        .withMessageContaining("duplicate computation id");
  }
}
