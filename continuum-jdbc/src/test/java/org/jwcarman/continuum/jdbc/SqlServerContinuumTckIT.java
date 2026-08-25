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

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import javax.sql.DataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.testcontainers.containers.MSSQLServerContainer;

/**
 * SQL Server 2022 certification: the full TCK over the only dialect whose locking statements differ
 * in kind — no {@code FOR UPDATE} at all; {@code UPDLOCK, ROWLOCK} table hints to lock and {@code
 * READPAST} to skip rows another claimer holds. accent's {@code supportsSkipLocked()} is
 * deliberately {@code false} here because that predicate covers the {@code FOR UPDATE SKIP LOCKED}
 * clause specifically; this dialect never uses it, so the guard's eventual admission gates on
 * version, not that capability. Goes through the ordinary detecting constructor, so the guard's
 * admission of SQL Server is itself under test on every build.
 */
class SqlServerContinuumTckIT extends ContinuumTck {

  private static final MSSQLServerContainer<?> SQLSERVER =
      new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2022-latest").acceptLicense();
  private static final DataSource DATA_SOURCE;

  static {
    SQLSERVER.start();
    SQLServerDataSource dataSource = new SQLServerDataSource();
    dataSource.setURL(SQLSERVER.getJdbcUrl());
    dataSource.setUser(SQLSERVER.getUsername());
    dataSource.setPassword(SQLSERVER.getPassword());
    DATA_SOURCE = dataSource;
    TckSchema.applySchema(DATA_SOURCE, ContinuumDialect.SQLSERVER);
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
