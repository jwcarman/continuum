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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ExpiryContext;
import org.jwcarman.continuum.retry.Retry;
import org.jwcarman.continuum.retry.Retry.RetryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RetryTest {

  private static final Instant SUBMITTED_AT = Instant.parse("2026-01-01T00:00:00Z");

  private ExpiryContext contextWithAttempts(int attemptCount) {
    return contextElapsed(attemptCount, Duration.ofMinutes(6));
  }

  private ExpiryContext contextElapsed(int attemptCount, Duration elapsed) {
    return new ExpiryContext(
        ComputationId.random(),
        new ComputationKind("k"),
        attemptCount,
        SUBMITTED_AT,
        SUBMITTED_AT.plus(Duration.ofMinutes(5)),
        SUBMITTED_AT.plus(elapsed));
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
    void at_most_requires_a_positive_count() {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> Retry.of(r -> r.atMost(0).handler((d, c) -> {})));
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

  @Nested
  class Elapsed_time {
    @Test
    void measures_from_submission_to_the_observed_lapse() {
      assertThat(contextElapsed(3, Duration.ofDays(2)).elapsedTime()).isEqualTo(Duration.ofDays(2));
    }

    @Test
    void supports_a_wall_clock_give_up_rule_that_ignores_attempt_count() {
      Retry<String> giveUpAfterAWeek =
          (dispatch, ctx) ->
              ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
                  ? RetryResult.notRetried("no response within 7 days")
                  : RetryResult.retried();

      assertThat(giveUpAfterAWeek.onTimeout("work", contextElapsed(500, Duration.ofDays(6))))
          .isEqualTo(RetryResult.retried());
      assertThat(giveUpAfterAWeek.onTimeout("work", contextElapsed(2, Duration.ofDays(8))))
          .isEqualTo(RetryResult.notRetried("no response within 7 days"));
    }
  }
}
