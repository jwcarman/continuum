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
import org.testcontainers.mysql.MySQLContainer;

/**
 * MySQL 8.4 certification: the full TCK battery over the MySQL dialect and its reference DDL,
 * through mysql-connector-j. Named {@code *IT} because this platform is expected to pass and stay
 * certified — a failure here is a regression, not an experiment outcome.
 */
class MySqlContinuumTckIT extends ContinuumTck {

  private static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4");
  private static final DataSource DATA_SOURCE;

  static {
    MYSQL.start();
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(MYSQL.getJdbcUrl());
    dataSource.setUser(MYSQL.getUsername());
    dataSource.setPassword(MYSQL.getPassword());
    DATA_SOURCE = dataSource;
    TckSchema.applySchema(DATA_SOURCE, ContinuumDialect.MYSQL);
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
