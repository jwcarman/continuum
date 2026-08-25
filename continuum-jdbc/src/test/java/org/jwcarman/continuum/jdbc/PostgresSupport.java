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
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

final class PostgresSupport {

  private PostgresSupport() {}

  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  static {
    POSTGRES.start();
  }

  static DataSource dataSource() {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    return dataSource;
  }

  static void applySchemaAndTruncate(DataSource dataSource) {
    TckSchema.applyAndTruncate(dataSource);
  }
}
