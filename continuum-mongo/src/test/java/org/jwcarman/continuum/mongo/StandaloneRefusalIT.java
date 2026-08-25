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

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.StoredContinuation;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * The standalone refusal against a real server, not a scripted {@code hello}. Testcontainers 2
 * starts {@code mongod} standalone unless asked for a replica set, which makes the topology the
 * guard exists to refuse trivially available. Two facts are pinned: the guard refuses it by name
 * with the fix, and what it protects against is real — bypass the guard and the first ownership
 * transfer fails inside the driver, because a standalone server has no multi-document transactions.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StandaloneRefusalIT {

  private static final MongoDBContainer STANDALONE = new MongoDBContainer("mongo:8.2");
  private static final MongoClient CLIENT;
  private static final String DATABASE = "continuum_standalone";

  static {
    STANDALONE.start();
    CLIENT = MongoClients.create(STANDALONE.getConnectionString());
  }

  private static Computation computation(ComputationId id) {
    Instant submittedAt = Instant.now();
    return new Computation(
        id,
        new ComputationKind("k"),
        ComputationStatus.PENDING,
        submittedAt,
        submittedAt.plus(1, ChronoUnit.HOURS),
        null,
        1,
        null);
  }

  @Test
  void a_real_standalone_server_is_refused_on_first_use_with_the_fix() {
    var repository = new MongoContinuumRepository(CLIENT, DATABASE);

    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(() -> repository.findComputation(ComputationId.random()))
        .withMessageContaining("MongoDB 8.2")
        .withMessageContaining("standalone")
        .withMessageContaining("--replSet")
        .withMessageContaining("assumeMongoDb");
  }

  @Test
  void bypassing_the_guard_fails_at_the_first_ownership_transfer() {
    var repository = MongoContinuumRepository.assumeMongoDb(CLIENT, DATABASE);
    ComputationId id = ComputationId.random();
    StoredContinuation continuation = new StoredContinuation(ContinuationId.random(), new byte[0]);

    // The refusal is not a preference: the driver itself rejects the transaction the ownership
    // transfer needs, and the message is the server's — the guard merely says it earlier and
    // better.
    assertThatExceptionOfType(MongoException.class)
        .isThrownBy(() -> repository.createComputation(computation(id), continuation))
        .withMessageContaining("replica set");
  }
}
