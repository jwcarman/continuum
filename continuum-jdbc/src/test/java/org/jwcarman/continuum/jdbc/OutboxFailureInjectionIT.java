package org.jwcarman.continuum.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.ContinuationId;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.StoredContinuation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OutboxFailureInjectionIT {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("inject");

  private final AtomicBoolean failOutboxInsert = new AtomicBoolean(false);

  private DataSource failingDataSource(DataSource delegate) {
    InvocationHandler dataSourceHandler =
        (proxy, method, args) -> {
          Object result = invoke(delegate, method, args);
          if ("getConnection".equals(method.getName())) {
            Connection connection = (Connection) result;
            return Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Connection.class},
                (connProxy, connMethod, connArgs) -> {
                  if ("prepareStatement".equals(connMethod.getName())
                      && failOutboxInsert.get()
                      && ((String) connArgs[0]).startsWith("INSERT INTO continuum_outbox")) {
                    throw new SQLException("injected outbox failure");
                  }
                  return invoke(connection, connMethod, connArgs);
                });
          }
          return result;
        };
    return (DataSource)
        Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] {DataSource.class}, dataSourceHandler);
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test
  void failed_outbox_insert_rolls_back_the_entire_completion() {
    DataSource real = PostgresSupport.dataSource();
    PostgresSupport.applySchemaAndTruncate(real);
    var repository = new JdbcContinuumRepository(failingDataSource(real));

    var id = ComputationId.random();
    repository.createComputation(
        new Computation(
            id, KIND, ComputationStatus.PENDING, NOW, NOW.plusSeconds(300), null, 1, null),
        new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));

    failOutboxInsert.set(true);
    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(
            () ->
                repository.complete(id, Outcome.success("r".getBytes(UTF_8)), NOW.plusSeconds(1)));
    failOutboxInsert.set(false);

    // nothing committed: still pending, continuation intact, no result, no outbox rows
    var found = repository.findComputation(id).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
    assertThat(
            repository.claimDeliveries("w", KIND, 10, Duration.ofSeconds(30), NOW.plusSeconds(2)))
        .isEmpty();

    // and the computation is still completable afterwards
    assertThat(repository.complete(id, Outcome.success("r".getBytes(UTF_8)), NOW.plusSeconds(3)))
        .isEqualTo(CompletionOutcome.COMPLETED);
  }
}
