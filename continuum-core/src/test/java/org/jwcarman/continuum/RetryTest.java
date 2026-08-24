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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Retry.RetryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RetryTest {

  private RetryContext contextWithAttempts(int attemptCount) {
    return new RetryContext(
        ComputationId.random(),
        new ComputationKind("k"),
        attemptCount,
        Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Nested
  class Declarative_retries {
    @Test
    void handler_is_invoked_and_default_timeout_reported_below_the_limit() {
      var dispatched = new ArrayList<String>();
      Retry<String> retry =
          Retry.of(r -> r.atMost(3).handler((dispatch, ctx) -> dispatched.add(dispatch)));

      var result = retry.onTimeout("work", contextWithAttempts(2));

      assertThat(dispatched).containsExactly("work");
      assertThat(result).isEqualTo(RetryResult.retried());
    }

    @Test
    void configured_timeout_overrides_the_default() {
      Retry<String> retry =
          Retry.of(r -> r.atMost(3).timeout(Duration.ofSeconds(7)).handler((dispatch, ctx) -> {}));
      assertThat(retry.onTimeout("work", contextWithAttempts(1)))
          .isEqualTo(RetryResult.retried(Duration.ofSeconds(7)));
    }

    @Test
    void exhausted_attempts_do_not_invoke_the_handler() {
      var dispatched = new ArrayList<String>();
      Retry<String> retry =
          Retry.of(r -> r.atMost(3).handler((dispatch, ctx) -> dispatched.add(dispatch)));

      var result = retry.onTimeout("work", contextWithAttempts(3));

      assertThat(dispatched).isEmpty();
      assertThat(result).isEqualTo(RetryResult.notRetried("attempts exhausted (3 of 3)"));
    }

    @Test
    void handler_is_required() {
      assertThatNullPointerException().isThrownBy(() -> Retry.of(r -> r.atMost(3)));
    }

    @Test
    void without_at_most_the_retry_never_exhausts() {
      Retry<String> retry = Retry.of(r -> r.handler((dispatch, ctx) -> {}));
      assertThat(retry.onTimeout("work", contextWithAttempts(1_000)))
          .isEqualTo(RetryResult.retried());
    }
  }

  @Nested
  class Custom_retries {
    @Test
    void a_direct_implementation_controls_the_result_entirely() {
      Retry<String> retry = (dispatch, ctx) -> RetryResult.notRetried("circuit open");
      assertThat(retry.onTimeout("work", contextWithAttempts(1)))
          .isEqualTo(RetryResult.notRetried("circuit open"));
    }
  }
}
