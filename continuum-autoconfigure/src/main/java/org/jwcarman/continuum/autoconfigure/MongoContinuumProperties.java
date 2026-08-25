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

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code continuum.mongo.*}. */
@ConfigurationProperties("continuum.mongo")
public class MongoContinuumProperties {

  /**
   * The database holding the continuum collections. Defaults to Boot's {@code
   * spring.mongodb.database} (or the older {@code spring.data.mongodb.database}), then {@code test}
   * — the driver's own default.
   */
  private String database;

  /** Whether to call {@code ensureIndexes()} at startup. */
  private boolean ensureIndexes = true;

  /** Instantiated by Spring's binder. */
  public MongoContinuumProperties() {
    // bound reflectively
  }

  /**
   * The configured database name.
   *
   * @return the database name, or null to derive it from Boot's Mongo properties
   */
  public String getDatabase() {
    return database;
  }

  /**
   * Sets the database name.
   *
   * @param database the database name, or null to derive it from Boot's Mongo properties
   */
  public void setDatabase(String database) {
    this.database = database;
  }

  /**
   * Whether {@code ensureIndexes()} runs at startup.
   *
   * @return true unless {@code continuum.mongo.ensure-indexes=false}
   */
  public boolean isEnsureIndexes() {
    return ensureIndexes;
  }

  /**
   * Sets whether {@code ensureIndexes()} runs at startup.
   *
   * @param ensureIndexes true to ensure indexes at startup
   */
  public void setEnsureIndexes(boolean ensureIndexes) {
    this.ensureIndexes = ensureIndexes;
  }
}
