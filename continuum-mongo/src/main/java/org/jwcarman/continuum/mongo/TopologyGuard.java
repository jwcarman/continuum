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

import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.bson.Document;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * Refuses, on first use, any server that cannot meet the TCK: standalone (no multi-document
 * transactions), MongoDB before 5.0, and the wire-compatible impostors — Amazon DocumentDB, Azure
 * Cosmos DB, FerretDB — which report genuine-looking versions and are refused by name until
 * certified. The message names what was found and how to fix it.
 */
final class TopologyGuard {

  private static final int MINIMUM_MAJOR = 5;
  private static final String ESCAPE_HATCH =
      " An operator who knows better can bypass detection with"
          + " MongoContinuumRepository.assumeMongoDb(client, databaseName).";

  private TopologyGuard() {}

  static void verify(MongoDatabase database) {
    Document buildInfo = database.runCommand(new Document("buildInfo", 1));
    String version = buildInfo.getString("version");
    Document ferret = buildInfo.get("ferretdb", Document.class);
    if (ferret != null) {
      throw refuse(
          "FerretDB " + ferret.getString("version") + " (reports as MongoDB " + version + ")");
    }
    Document hello = database.runCommand(new Document("hello", 1));
    for (String host : hosts(hello)) {
      String lower = host.toLowerCase(Locale.ROOT);
      if (lower.contains(".docdb.amazonaws.com")
          || lower.contains(".docdb-elastic.amazonaws.com")) {
        throw refuse("Amazon DocumentDB (reports as MongoDB " + version + ")");
      }
      if (lower.contains(".cosmos.azure.com")) {
        throw refuse("Azure Cosmos DB (reports as MongoDB " + version + ")");
      }
    }
    if (major(version) < MINIMUM_MAJOR) {
      throw new ContinuumPersistenceException(
          "unsupported database platform: MongoDB "
              + version
              + "; continuum needs MongoDB 5.0+."
              + ESCAPE_HATCH);
    }
    boolean mongos = "isdbgrid".equals(hello.getString("msg"));
    boolean replicaSet = hello.containsKey("setName");
    if (!mongos && !replicaSet) {
      throw new ContinuumPersistenceException(
          "unsupported database topology: MongoDB "
              + version
              + " standalone; continuum needs a replica set for multi-document transactions,"
              + " and a single node started with --replSet (then rs.initiate()) is enough."
              + ESCAPE_HATCH);
    }
  }

  private static ContinuumPersistenceException refuse(String detected) {
    return new ContinuumPersistenceException(
        "unsupported database platform: "
            + detected
            + "; not certified — see the persistence guide."
            + ESCAPE_HATCH);
  }

  private static List<String> hosts(Document hello) {
    List<String> hosts = new ArrayList<>();
    List<String> listed = hello.getList("hosts", String.class);
    if (listed != null) {
      hosts.addAll(listed);
    }
    String me = hello.getString("me");
    if (me != null) {
      hosts.add(me);
    }
    return hosts;
  }

  private static int major(String version) {
    int dot = version.indexOf('.');
    return Integer.parseInt(dot < 0 ? version : version.substring(0, dot));
  }
}
