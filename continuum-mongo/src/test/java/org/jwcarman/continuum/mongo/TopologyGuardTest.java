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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * The guard turns "the first complete() fails with an opaque driver error" into a refusal on first
 * use that names what was found and how to fix it. These tests script the {@code buildInfo} and
 * {@code hello} replies the way real servers were observed to answer.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TopologyGuardTest {

  private static final Document EMPTY_HELLO = new Document("ok", 1.0);

  /** A client whose database answers buildInfo/hello as scripted and returns no documents. */
  private static MongoClient server(Document buildInfo, Document hello) {
    MongoClient client = mock();
    MongoDatabase database = mock();
    MongoCollection<Document> collection = mock();
    FindIterable<Document> nothing = mock();
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.withCodecRegistry(any())).thenReturn(database);
    when(database.getCollection(anyString())).thenReturn(collection);
    when(database.runCommand(new Document("buildInfo", 1))).thenReturn(buildInfo);
    when(database.runCommand(new Document("hello", 1))).thenReturn(hello);
    when(collection.find(any(Bson.class))).thenReturn(nothing);
    when(nothing.first()).thenReturn(null);
    return client;
  }

  private static Document buildInfo(String version) {
    return new Document("version", version).append("ok", 1.0);
  }

  private static Document replicaSet(String... hosts) {
    return new Document("setName", "rs0")
        .append("hosts", List.of(hosts))
        .append("me", hosts[0])
        .append("ok", 1.0);
  }

  private static void anyOperation(MongoContinuumRepository repository) {
    repository.findComputation(ComputationId.random());
  }

  @Nested
  class Permitting {
    @Test
    void a_replica_set_on_mongodb_8_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("8.2.12"), replicaSet("db1:27017", "db2:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void a_single_node_replica_set_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("7.0.40"), replicaSet("localhost:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mongos_passes() {
      var hello = new Document("msg", "isdbgrid").append("ok", 1.0);
      var repository = new MongoContinuumRepository(server(buildInfo("8.2.12"), hello), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mongodb_5_0_is_the_floor_and_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("5.0.31"), replicaSet("localhost:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void a_host_that_merely_contains_the_docdb_suffix_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("8.2.12"), replicaSet("foo.docdb.amazonaws.com.example.net:27017")),
              "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void detection_runs_once_per_repository() {
      MongoClient client = server(buildInfo("8.2.12"), replicaSet("localhost:27017"));
      var repository = new MongoContinuumRepository(client, "app");

      anyOperation(repository);
      anyOperation(repository);

      verify(client.getDatabase("app")).runCommand(new Document("hello", 1));
    }
  }

  @Nested
  class Refusing {
    @Test
    void a_standalone_server_is_refused_with_the_fix() {
      var repository =
          new MongoContinuumRepository(server(buildInfo("8.2.12"), EMPTY_HELLO), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("MongoDB 8.2.12 standalone")
          .withMessageContaining("--replSet")
          .withMessageContaining("assumeMongoDb");
    }

    @Test
    void mongodb_before_5_0_is_refused() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("4.4.29"), replicaSet("localhost:27017")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("MongoDB 4.4.29")
          .withMessageContaining("5.0+");
    }

    @Test
    void amazon_documentdb_is_refused_by_name() {
      var repository =
          new MongoContinuumRepository(
              server(
                  buildInfo("5.0.0"),
                  replicaSet("cluster.cluster-abc.us-east-1.docdb.amazonaws.com:27017")),
              "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("Amazon DocumentDB")
          .withMessageContaining("reports as MongoDB 5.0.0")
          .withMessageContaining("not certified");
    }

    @Test
    void azure_cosmos_db_is_refused_by_name() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("7.0.0"), replicaSet("acct.mongo.cosmos.azure.com:10255")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("Azure Cosmos DB")
          .withMessageContaining("reports as MongoDB 7.0.0");
    }

    @Test
    void ferretdb_is_refused_by_name() {
      var buildInfo = buildInfo("7.0.42").append("ferretdb", new Document("version", "v2.1.0"));
      var repository =
          new MongoContinuumRepository(server(buildInfo, replicaSet("localhost:27017")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("FerretDB v2.1.0")
          .withMessageContaining("reports as MongoDB 7.0.42");
    }
  }

  @Nested
  class The_escape_hatch {
    @Test
    void assume_mongodb_never_runs_a_command() {
      MongoClient client = server(buildInfo("8.2.12"), EMPTY_HELLO);
      var repository = MongoContinuumRepository.assumeMongoDb(client, "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
      verify(client.getDatabase("app"), never()).runCommand(any(Bson.class));
    }
  }

  @Nested
  class Construction {
    @Test
    void opens_no_connection_and_runs_no_command() {
      MongoClient client = server(buildInfo("8.2.12"), EMPTY_HELLO);

      new MongoContinuumRepository(client, "app");

      verify(client.getDatabase("app"), never()).runCommand(any(Bson.class));
      verify(client, never()).startSession();
    }
  }
}
