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

import org.h2.jdbcx.JdbcDataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

/**
 * H2 2.3 in its default mode — the URL a Boot test context gets without asking — over the
 * unmodified PostgreSQL dialect and reference DDL.
 *
 * <p>Certified for test/embedded use only — H2 is what a Spring Boot test context typically wires,
 * and this suite is what lets such a context exercise the real JDBC provider (SQL, schema, dialect)
 * without a container. Goes through the ordinary detecting constructor, so the guard's admission of
 * H2 is itself under test.
 */
class H2ContinuumTckIT extends ContinuumTck {

  private static final JdbcDataSource DATA_SOURCE = new JdbcDataSource();

  static {
    DATA_SOURCE.setURL("jdbc:h2:mem:continuum-tck-default;DB_CLOSE_DELAY=-1");
    TckSchema.applySchema(DATA_SOURCE, ContinuumDialect.POSTGRESQL);
  }

  @Override
  protected ContinuumRepository createRepository() {
    TckSchema.truncate(DATA_SOURCE);
    return new JdbcContinuumRepository(DATA_SOURCE);
  }
}
