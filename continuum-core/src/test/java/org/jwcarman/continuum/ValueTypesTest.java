package org.jwcarman.continuum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ValueTypesTest {

  @Nested
  class Identifiers {
    @Test
    void random_computation_ids_are_unique() {
      assertThat(ComputationId.random()).isNotEqualTo(ComputationId.random());
    }

    @Test
    void computation_kind_rejects_blank() {
      assertThatIllegalArgumentException().isThrownBy(() -> new ComputationKind(" "));
    }

    @Test
    void computation_kind_rejects_null() {
      assertThatNullPointerException().isThrownBy(() -> new ComputationKind(null));
    }
  }

  @Nested
  class Outcomes {
    @Test
    void success_status_is_completed() {
      assertThat(Outcome.statusOf(Outcome.success(new byte[] {1})))
          .isEqualTo(ComputationStatus.COMPLETED);
    }

    @Test
    void failure_status_is_failed() {
      assertThat(Outcome.statusOf(Outcome.failure("boom"))).isEqualTo(ComputationStatus.FAILED);
    }

    @Test
    void expired_status_is_expired() {
      assertThat(
              Outcome.statusOf(
                  Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (3 of 3)")))
          .isEqualTo(ComputationStatus.EXPIRED);
    }

    @Test
    void success_equality_compares_payload_contents() {
      assertThat(Outcome.success(new byte[] {1, 2})).isEqualTo(Outcome.success(new byte[] {1, 2}));
    }
  }

  @Nested
  class Requests {
    @Test
    void request_requires_continuation_payload() {
      assertThatNullPointerException()
          .isThrownBy(
              () -> new ComputationRequest(new ComputationKind("k"), null, Instant.EPOCH, null));
    }

    @Test
    void request_allows_null_dispatch_payload() {
      var request =
          new ComputationRequest(new ComputationKind("k"), new byte[] {1}, Instant.EPOCH, null);
      assertThat(request.dispatchPayload()).isNull();
    }
  }

  @Nested
  class Computations {
    @Test
    void retryable_means_dispatch_payload_present() {
      var pending =
          new Computation(
              ComputationId.random(),
              new ComputationKind("k"),
              ComputationStatus.PENDING,
              Instant.EPOCH,
              Instant.EPOCH.plusSeconds(60),
              new byte[] {1},
              1,
              null);
      assertThat(pending.retryable()).isTrue();
      var bare =
          new Computation(
              ComputationId.random(),
              new ComputationKind("k"),
              ComputationStatus.PENDING,
              Instant.EPOCH,
              Instant.EPOCH.plusSeconds(60),
              null,
              1,
              null);
      assertThat(bare.retryable()).isFalse();
    }

    @Test
    void attempt_count_must_be_at_least_one() {
      assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  new Computation(
                      ComputationId.random(),
                      new ComputationKind("k"),
                      ComputationStatus.PENDING,
                      Instant.EPOCH,
                      Instant.EPOCH.plusSeconds(60),
                      null,
                      0,
                      null));
    }
  }
}
