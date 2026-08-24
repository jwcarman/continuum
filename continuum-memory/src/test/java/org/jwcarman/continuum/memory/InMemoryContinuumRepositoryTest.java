package org.jwcarman.continuum.memory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
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
import org.jwcarman.continuum.spi.StoredContinuation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class InMemoryContinuumRepositoryTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("k");

  private InMemoryContinuumRepository repository;

  @BeforeEach
  void set_up() {
    repository = new InMemoryContinuumRepository();
  }

  private Computation pending(ComputationId id) {
    return new Computation(
        id, KIND, ComputationStatus.PENDING, NOW, NOW.plusSeconds(300), null, 1, null);
  }

  @Test
  void complete_transfers_pending_row_to_result_and_creates_deliveries() {
    var id = ComputationId.random();
    repository.createComputation(
        pending(id), new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));

    var outcome = Outcome.success("r".getBytes(UTF_8));
    assertThat(repository.complete(id, outcome, NOW.plusSeconds(1)))
        .isEqualTo(CompletionOutcome.COMPLETED);

    var found = repository.findComputation(id).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.COMPLETED);
    assertThat(found.outcome()).isEqualTo(outcome);
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(600), 10)).isEmpty();

    var claimed =
        repository.claimDeliveries("w", KIND, 10, Duration.ofSeconds(30), NOW.plusSeconds(1));
    assertThat(claimed).hasSize(1);
    assertThat(claimed.getFirst().delivery().outcome()).isEqualTo(outcome);
  }

  @Test
  void deadline_at_now_counts_as_expired() {
    var id = ComputationId.random();
    repository.createComputation(
        pending(id), new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(300), 10)).hasSize(1);
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(299), 10)).isEmpty();
  }
}
