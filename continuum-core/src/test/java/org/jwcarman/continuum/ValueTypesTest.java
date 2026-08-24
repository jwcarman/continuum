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
  class Value_semantics {
    private Computation computation(byte[] dispatchPayload, Outcome outcome) {
      return new Computation(
          new ComputationId(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")),
          new ComputationKind("k"),
          outcome == null ? ComputationStatus.PENDING : Outcome.statusOf(outcome),
          Instant.EPOCH,
          Instant.EPOCH.plusSeconds(60),
          dispatchPayload,
          1,
          outcome);
    }

    @Test
    void computations_compare_dispatch_payloads_by_content() {
      var a = computation(new byte[] {1, 2}, null);
      var b = computation(new byte[] {1, 2}, null);
      assertThat(a)
          .isEqualTo(b)
          .hasSameHashCodeAs(b)
          .isNotEqualTo(computation(new byte[] {9}, null));
      assertThat(a.toString()).contains("PENDING");
    }

    @Test
    void computations_with_different_outcomes_differ() {
      assertThat(computation(null, Outcome.failure("f")))
          .isNotEqualTo(computation(null, Outcome.success(new byte[] {1})));
    }

    @Test
    void requests_compare_payloads_by_content() {
      var a =
          new ComputationRequest(
              new ComputationKind("k"), new byte[] {1}, Instant.EPOCH, new byte[] {2});
      var b =
          new ComputationRequest(
              new ComputationKind("k"), new byte[] {1}, Instant.EPOCH, new byte[] {2});
      assertThat(a)
          .isEqualTo(b)
          .hasSameHashCodeAs(b)
          .isNotEqualTo(
              new ComputationRequest(
                  new ComputationKind("k"), new byte[] {1}, Instant.EPOCH, null));
      assertThat(a.toString()).contains("retryable=true");
    }

    @Test
    void deliveries_compare_payloads_by_content() {
      var computationId = ComputationId.random();
      var continuationId = ContinuationId.random();
      var a =
          new CompletionDelivery(
              computationId,
              new ComputationKind("k"),
              continuationId,
              new byte[] {1},
              Outcome.failure("f"));
      var b =
          new CompletionDelivery(
              computationId,
              new ComputationKind("k"),
              continuationId,
              new byte[] {1},
              Outcome.failure("f"));
      assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
      assertThat(a.toString()).contains(computationId.value().toString());
    }

    @Test
    void success_is_never_equal_to_another_arm_or_type() {
      var success = Outcome.success(new byte[] {1});
      assertThat(success)
          .isNotEqualTo(Outcome.failure("f"))
          .isNotEqualTo("not an outcome")
          .hasSameHashCodeAs(Outcome.success(new byte[] {1}));
    }

    @Test
    void outcome_arms_have_value_semantics() {
      assertThat(Outcome.failure("f")).isEqualTo(Outcome.failure("f"));
      assertThat(Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "m"))
          .isEqualTo(Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "m"))
          .isNotEqualTo(Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "m"));
      assertThat(Outcome.success(new byte[] {1})).isNotEqualTo(Outcome.success(new byte[] {2}));
      assertThat(Outcome.success(new byte[] {1}).toString()).contains("1 bytes");
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
