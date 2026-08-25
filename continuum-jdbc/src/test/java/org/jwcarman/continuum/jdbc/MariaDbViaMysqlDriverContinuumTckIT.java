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

import com.mysql.cj.jdbc.MysqlDataSource;
import javax.sql.DataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

/**
 * The impostor pairing: a MariaDB server reached through mysql-connector-j, which reports {@code
 * productName = "MySQL"} and betrays the real engine only by {@code MariaDB} appearing in the
 * version string. accent exists because this pairing lies about identity; this suite proves the
 * whole path — detection disambiguates to the {@code MariaDB} arm, the guard admits it (11.4 is
 * past the 10.6 skip-locked floor), the MySQL dialect binds correctly — against the same container
 * {@link MariaDbContinuumTckIT} certifies natively.
 */
class MariaDbViaMysqlDriverContinuumTckIT extends ContinuumTck {

  private static final DataSource DATA_SOURCE;

  static {
    // Reuses MariaDbContinuumTckIT's container (starting it if needed); the URL is rewritten to
    // the mysql scheme and handed to a driver-specific DataSource — DriverManager never chooses.
    var mariadb = MariaDbContinuumTckIT.MARIADB;
    if (!mariadb.isRunning()) {
      mariadb.start();
    }
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(mariadb.getJdbcUrl().replace("jdbc:mariadb:", "jdbc:mysql:"));
    dataSource.setUser(mariadb.getUsername());
    dataSource.setPassword(mariadb.getPassword());
    DATA_SOURCE = dataSource;
    // No applySchema here: reading MariaDbContinuumTckIT.MARIADB above forces that class's
    // initializer, which applies the schema — and MySQL's CREATE INDEX is not idempotent, so
    // applying twice against the shared container would fail on duplicate index names.
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
