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
 * Configures durable PostgreSQL persistence when {@code continuum-jdbc} is on the classpath and the
 * application defines a {@link DataSource}.
 */
@AutoConfiguration(before = ContinuumAutoConfiguration.class)
@ConditionalOnClass(JdbcContinuumRepository.class)
@ConditionalOnBean(DataSource.class)
public class JdbcContinuumAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository jdbcContinuumRepository(DataSource dataSource) {
    return new JdbcContinuumRepository(dataSource);
  }
}
