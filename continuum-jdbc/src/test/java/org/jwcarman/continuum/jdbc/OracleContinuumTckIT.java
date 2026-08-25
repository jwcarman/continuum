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
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.testcontainers.oracle.OracleContainer;

/**
 * Oracle 23ai certification: the full TCK over the Oracle dialect, whose one genuinely different
 * behavior is the row-limited locking read — Oracle cannot lock through the inline view its {@code
 * FETCH FIRST} creates, so the claim query carries no limit and the provider stops fetching after
 * {@code limit} rows, locking each as it is read (the Oracle AQ dequeue idiom). Constructed via
 * {@link JdbcContinuumRepository#withDialect} because the guard does not yet admit Oracle; promoted
 * to {@code *IT} and admitted once this passes.
 */
class OracleContinuumTckIT extends ContinuumTck {

  private static final OracleContainer ORACLE =
      new OracleContainer("gvenzl/oracle-free:23-slim-faststart");
  private static final DataSource DATA_SOURCE;

  static {
    ORACLE.start();
    try {
      // Pooled on purpose: see the ucp11 dependency comment. Eight connections bounds the
      // race suites' concurrency well under Oracle Free's session cap while still exercising
      // real contention.
      PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();
      dataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
      dataSource.setURL(ORACLE.getJdbcUrl());
      dataSource.setUser(ORACLE.getUsername());
      dataSource.setPassword(ORACLE.getPassword());
      dataSource.setMaxPoolSize(8);
      DATA_SOURCE = dataSource;
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException("could not configure Oracle data source", e);
    }
    TckSchema.applySchema(DATA_SOURCE, ContinuumDialect.ORACLE);
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
