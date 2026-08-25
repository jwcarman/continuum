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

import javax.sql.DataSource;
import org.jwcarman.continuum.jdbc.JdbcContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Configures durable JDBC persistence (certified: PostgreSQL, MySQL, MariaDB, Oracle, SQL Server)
 * when {@code continuum-jdbc} is on the classpath and the application defines a {@link DataSource}.
 */
@AutoConfiguration(
    before = ContinuumAutoConfiguration.class,
    // String names, not class references: DataSourceAutoConfiguration lives in an
    // optional module. The first name covers Spring Boot 4.x (verified identical in
    // 4.0 and 4.1); the second covers the Boot 3.x package. Unknown names are ignored.
    afterName = {
      "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
      "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
    })
@ConditionalOnClass(JdbcContinuumRepository.class)
@ConditionalOnBean(DataSource.class)
public class JdbcContinuumAutoConfiguration {

  /** Instantiated by Spring Boot's auto-configuration machinery, not by application code. */
  public JdbcContinuumAutoConfiguration() {
    // Spring instantiates this class reflectively; nothing to initialize.
  }

  /**
   * Contributes durable JDBC persistence over the application's {@link DataSource}, unless a {@link
   * ContinuumRepository} is already defined. Because this class is ordered before {@link
   * ContinuumAutoConfiguration}, winning here is what suppresses the in-memory fallback.
   *
   * @param dataSource the application's data source; it owns pooling and schema
   * @return a {@link JdbcContinuumRepository} bound to that data source
   */
  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository jdbcContinuumRepository(DataSource dataSource) {
    return new JdbcContinuumRepository(dataSource);
  }
}
