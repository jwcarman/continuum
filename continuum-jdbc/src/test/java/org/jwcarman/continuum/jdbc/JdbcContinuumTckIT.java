package org.jwcarman.continuum.jdbc;

import javax.sql.DataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

class JdbcContinuumTckIT extends ContinuumTck {

  @Override
  protected ContinuumRepository createRepository() {
    DataSource dataSource = PostgresSupport.dataSource();
    PostgresSupport.applySchemaAndTruncate(dataSource);
    return new JdbcContinuumRepository(dataSource);
  }
}
