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

import javax.sql.DataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;

/**
 * MariaDB 11.4 certification through its native driver: the full TCK battery over the MySQL dialect
 * and its reference DDL. Named {@code *IT} because this platform is expected to pass and stay
 * certified.
 */
class MariaDbContinuumTckIT extends ContinuumTck {

  static final MariaDBContainer<?> MARIADB = new MariaDBContainer<>("mariadb:11.4");
  private static final DataSource DATA_SOURCE;

  static {
    MARIADB.start();
    try {
      MariaDbDataSource dataSource = new MariaDbDataSource(MARIADB.getJdbcUrl());
      dataSource.setUser(MARIADB.getUsername());
      dataSource.setPassword(MARIADB.getPassword());
      DATA_SOURCE = dataSource;
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException("could not configure MariaDB data source", e);
    }
    TckSchema.applySchema(DATA_SOURCE, ContinuumDialect.MYSQL);
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
