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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Certifies MongoDB through the ordinary detecting constructor, so the guard's admission of a
 * genuine replica set is itself under test on every build. {@code withReplicaSet()} asks for a
 * single-node replica set, which is all transactions need — Testcontainers 2 starts a standalone by
 * default, and the guard refuses that. The image is 8.2, not 8.0: 8.0 refuses to start on Linux
 * kernels 6.19+ (SERVER-121912), which Docker Desktop currently ships.
 */
class MongoContinuumTckIT extends ContinuumTck {

  static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.2").withReplicaSet();
  static final MongoClient CLIENT;
  static final String DATABASE = "continuum";

  static {
    MONGO.start();
    CLIENT = MongoClients.create(MONGO.getConnectionString());
  }

  @Override
  protected ContinuumRepository createRepository() {
    CLIENT.getDatabase(DATABASE).drop();
    return new MongoContinuumRepository(CLIENT, DATABASE);
  }
}
