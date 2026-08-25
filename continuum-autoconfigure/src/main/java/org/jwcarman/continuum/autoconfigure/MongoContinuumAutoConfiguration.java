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

import com.mongodb.client.MongoClient;
import org.jwcarman.continuum.mongo.MongoContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Configures durable MongoDB persistence (certified: MongoDB 5.0+ replica sets) when {@code
 * continuum-mongo} is on the classpath, the application defines a {@link MongoClient}, and {@code
 * continuum.persistence.type} either selects {@code mongo} or is absent with no competing JDBC
 * candidate.
 */
@AutoConfiguration(
    before = ContinuumAutoConfiguration.class,
    // String names: Boot's Mongo auto-configuration lives in an optional module. First name is
    // Spring Boot 4.x; second is the Boot 3.x package. Unknown names are ignored.
    afterName = {
      "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration",
      "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration"
    })
@ConditionalOnClass(MongoContinuumRepository.class)
@ConditionalOnBean(MongoClient.class)
@ConditionalOnPersistenceType(PersistenceType.MONGO)
@EnableConfigurationProperties(MongoContinuumProperties.class)
public class MongoContinuumAutoConfiguration {

  private static final String DEFAULT_DATABASE = "test";

  /** Instantiated by Spring Boot's auto-configuration machinery, not by application code. */
  public MongoContinuumAutoConfiguration() {
    // Spring instantiates this class reflectively; nothing to initialize.
  }

  /**
   * Contributes MongoDB persistence over the application's {@link MongoClient}, unless a {@link
   * ContinuumRepository} is already defined, and ensures the query indexes unless {@code
   * continuum.mongo.ensure-indexes=false}.
   *
   * @param client the application's client; it owns pooling and credentials
   * @param properties {@code continuum.mongo.*}
   * @param environment for Boot's own Mongo database property as the default name
   * @return a {@link MongoContinuumRepository} bound to that client
   */
  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository mongoContinuumRepository(
      MongoClient client, MongoContinuumProperties properties, Environment environment) {
    MongoContinuumRepository repository =
        new MongoContinuumRepository(client, databaseName(properties, environment));
    if (properties.isEnsureIndexes()) {
      repository.ensureIndexes();
    }
    return repository;
  }

  private static String databaseName(MongoContinuumProperties properties, Environment environment) {
    if (properties.getDatabase() != null) {
      return properties.getDatabase();
    }
    String boot4 = environment.getProperty("spring.mongodb.database");
    if (boot4 != null) {
      return boot4;
    }
    String boot3 = environment.getProperty("spring.data.mongodb.database");
    return boot3 != null ? boot3 : DEFAULT_DATABASE;
  }
}
