package org.jwcarman.continuum.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        InputStream schema =
            PostgresSupport.class.getResourceAsStream(
                "/org/jwcarman/continuum/jdbc/continuum-postgresql.sql")) {
      statement.execute(new String(schema.readAllBytes(), StandardCharsets.UTF_8));
      statement.execute(
          "TRUNCATE continuum_outbox, continuum_result, continuum_continuation, continuum_computation");
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("failed to prepare postgres schema", e);
    }
  }
}
