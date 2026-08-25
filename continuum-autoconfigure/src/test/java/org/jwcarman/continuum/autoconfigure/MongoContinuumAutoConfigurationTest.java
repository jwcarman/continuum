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
package org.jwcarman.continuum.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javax.sql.DataSource;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.jdbc.JdbcContinuumRepository;
import org.jwcarman.continuum.memory.InMemoryContinuumRepository;
import org.jwcarman.continuum.mongo.MongoContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class MongoContinuumAutoConfigurationTest {

  // Selection tests are about selection only: ensure-indexes is off so no test here needs a
  // scripted topology; Mongo_properties covers ensure-indexes on its own.
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  JdbcContinuumAutoConfiguration.class,
                  MongoContinuumAutoConfiguration.class,
                  ContinuumAutoConfiguration.class))
          .withPropertyValues("continuum.mongo.ensure-indexes=false");

  /** A client whose database and collections are plain mocks, so calls can be verified. */
  private static MongoClient client(MongoDatabase database) {
    MongoClient client = mock();
    MongoCollection<Document> collection = mock();
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.withCodecRegistry(any())).thenReturn(database);
    when(database.getCollection(anyString())).thenReturn(collection);
    return client;
  }

  @Configuration(proxyBeanMethods = false)
  static class MongoClientConfiguration {
    @Bean
    MongoClient mongoClient() {
      return client(mock());
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DataSourceConfiguration {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class UserRepositoryConfiguration {
    static final InMemoryContinuumRepository INSTANCE = new InMemoryContinuumRepository();

    @Bean
    ContinuumRepository continuumRepository() {
      return INSTANCE;
    }
  }

  @Nested
  class Auto_detection {
    @Test
    void a_user_defined_repository_bean_always_wins() {
      runner
          .withUserConfiguration(
              DataSourceConfiguration.class,
              MongoClientConfiguration.class,
              UserRepositoryConfiguration.class)
          .run(
              context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(ContinuumRepository.class))
                    .isSameAs(UserRepositoryConfiguration.INSTANCE);
              });
    }

    @Test
    void uses_mongo_when_continuum_mongo_and_a_mongo_client_are_present() {
      runner
          .withUserConfiguration(MongoClientConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(MongoContinuumRepository.class));
    }

    @Test
    void falls_back_to_memory_when_continuum_mongo_is_not_on_the_classpath() {
      runner
          .withClassLoader(new FilteredClassLoader(MongoContinuumRepository.class))
          .withUserConfiguration(MongoClientConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(InMemoryContinuumRepository.class));
    }

    @Test
    void jdbc_alone_still_selects_jdbc() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(JdbcContinuumRepository.class));
    }

    @Test
    void both_candidates_without_the_property_fail_startup_naming_the_property() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .run(
              context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .rootCause()
                    .hasMessageContaining("continuum.persistence.type")
                    .hasMessageContaining("jdbc")
                    .hasMessageContaining("mongo");
              });
    }
  }

  @Nested
  class Explicit_type {
    @Test
    void mongo_wins_over_jdbc_when_selected() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=mongo")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(MongoContinuumRepository.class));
    }

    @Test
    void jdbc_wins_over_mongo_when_selected() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=jdbc")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(JdbcContinuumRepository.class));
    }

    @Test
    void memory_disables_both_durable_providers() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=memory")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(InMemoryContinuumRepository.class));
    }

    @Test
    void an_unknown_type_fails_startup_naming_the_property_and_the_choices() {
      runner
          .withUserConfiguration(MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=mongodb")
          .run(
              context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .rootCause()
                    .hasMessageContaining("continuum.persistence.type")
                    .hasMessageContaining("mongodb")
                    .hasMessageContaining("jdbc, mongo, memory");
              });
    }
  }

  @Nested
  class Mongo_properties {
    private ApplicationContextRunner mongoOnly(MongoClient client) {
      return new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  MongoContinuumAutoConfiguration.class, ContinuumAutoConfiguration.class))
          .withBean(MongoClient.class, () -> client);
    }

    @Test
    void database_name_defaults_to_boots_property() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues(
              "spring.mongodb.database=orders", "continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("orders"));
    }

    @Test
    void continuum_mongo_database_overrides_boots_property() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues(
              "spring.mongodb.database=orders",
              "continuum.mongo.database=continuum",
              "continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("continuum"));
    }

    @Test
    void the_driver_default_database_is_the_last_resort() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues("continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("test"));
    }

    @Test
    void indexes_are_ensured_at_startup_by_default() {
      MongoDatabase database = mock();
      MongoClient client = client(database);
      // The guard runs before ensureIndexes(); script a replica set so it passes.
      when(database.runCommand(new Document("buildInfo", 1)))
          .thenReturn(new Document("version", "8.2.12"));
      when(database.runCommand(new Document("hello", 1)))
          .thenReturn(new Document("setName", "rs0"));
      // The stub above routes every collection name to the same mock, so all four
      // ensureIndexes() calls (computations, continuations, results, outbox) land on it;
      // atLeastOnce() confirms indexing ran without over-specifying the exact count.
      mongoOnly(client)
          .run(
              context ->
                  verify(database.getCollection("continuum_outbox"), atLeastOnce())
                      .createIndex(any(Bson.class)));
    }

    @Test
    void ensure_indexes_false_touches_nothing_at_startup() {
      MongoDatabase database = mock();
      MongoClient client = client(database);
      mongoOnly(client)
          .withPropertyValues("continuum.mongo.ensure-indexes=false")
          .run(
              context -> {
                assertThat(context.getBean(ContinuumRepository.class))
                    .isInstanceOf(MongoContinuumRepository.class);
                verify(database, never()).runCommand(any(Bson.class));
                verify(database.getCollection("continuum_outbox"), never())
                    .createIndex(any(Bson.class));
              });
    }
  }
}
