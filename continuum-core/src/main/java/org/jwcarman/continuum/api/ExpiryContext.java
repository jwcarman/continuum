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
package org.jwcarman.continuum.api;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Continuum's durable facts about a computation whose deadline has lapsed — the same facts whether
 * the kind is retryable or not, so both {@code Retry} and {@link Expiry} receive this.
 *
 * <p>Prefer {@link #elapsedTime()} to hand-rolled arithmetic: it measures from submission on
 * Continuum's own clock, so a give-up rule reads as "stop waiting a week after submission" and
 * stays correct when attempts carry differing timeouts — which attempt counts cannot express.
 *
 * <pre>{@code
 * Expiry approvals = ctx ->
 *     ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
 *         ? ExpiryResult.expired("no response within 7 days")
 *         : ExpiryResult.extended();
 * }</pre>
 *
 * @param computationId the lapsed computation — where a redispatched worker would report
 * @param kind the computation's kind
 * @param attemptCount how many attempts have already run, counting the original dispatch as 1;
 *     always 1 for a non-retryable kind, which is never redispatched
 * @param submittedAt when the computation was originally submitted, unchanged by any redispatch or
 *     extension
 * @param deadline the deadline that lapsed
 * @param observedAt when Continuum observed the lapse, read from its {@code InstantSource} — one
 *     value shared by every context in a pump run, so a batch cannot disagree with itself
 */
public record ExpiryContext(
    ComputationId computationId,
    ComputationKind kind,
    int attemptCount,
    Instant submittedAt,
    Instant deadline,
    Instant observedAt) {

  /**
   * Requires the durable facts a policy needs to decide.
   *
   * @throws NullPointerException if {@code computationId}, {@code kind}, {@code submittedAt},
   *     {@code deadline}, or {@code observedAt} is null
   */
  public ExpiryContext {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(submittedAt, "submittedAt must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
    Objects.requireNonNull(observedAt, "observedAt must not be null");
  }

  /**
   * How long the computation has been outstanding: from {@code submittedAt} to {@code observedAt}.
   *
   * <p>Measured on Continuum's {@code InstantSource} rather than the wall clock, so it agrees with
   * every other timestamp here and stays correct under a test clock — {@code Instant.now()} in a
   * policy would not.
   *
   * @return the time elapsed since submission
   */
  public Duration elapsedTime() {
    return Duration.between(submittedAt, observedAt);
  }
}
