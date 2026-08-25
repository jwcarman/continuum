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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EnsureIndexesIT {

  private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.2");
  private static final MongoClient CLIENT;
  private static final String DATABASE = "continuum_indexes";

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

  private List<String> indexNames(String collection) {
    List<String> names = new ArrayList<>();
    for (Document index : database.getCollection(collection).listIndexes()) {
      names.add(index.getString("name"));
    }
    return names;
  }

  @Test
  void creates_the_four_query_indexes() {
    repository.ensureIndexes();

    assertThat(indexNames(Documents.COMPUTATIONS)).contains("kind_1_deadlineAt_1");
    assertThat(indexNames(Documents.CONTINUATIONS)).contains("computationId_1");
    assertThat(indexNames(Documents.RESULTS)).contains("kind_1_completedAt_1");
    assertThat(indexNames(Documents.OUTBOX)).contains("kind_1_availableAt_1");
  }

  @Test
  void is_idempotent() {
    repository.ensureIndexes();

    assertThatCode(repository::ensureIndexes).doesNotThrowAnyException();
    // _id plus exactly one query index per collection, however many times it runs
    assertThat(indexNames(Documents.OUTBOX)).hasSize(2);
  }

  @Test
  void construction_creates_nothing() {
    assertThat(database.listCollectionNames()).isEmpty();
  }
}
