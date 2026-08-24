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
 * One delivery, decoded — everything {@code deliverResults} knows about it, not just the two values
 * a consumer usually acts on.
 *
 * <p>The two-argument consumer gives you the continuation and the outcome, which is all most
 * consumers want. This is the same delivery with the facts that consumer cannot see: identities to
 * correlate a log line against, the timestamps to record end-to-end duration at the moment the
 * outcome arrives, and the delivery attempt count a give-up or dead-letter policy needs.
 *
 * <pre>{@code
 * toolCalls.deliverResults(BatchSize.of(25), delivery -> {
 *     if (delivery.deliveryAttempt() >= 10) {
 *         deadLetter.record(delivery.continuationId(), delivery.outcome());
 *         return;                                  // returning acknowledges: stop redelivering
 *     }
 *     metrics.timer("tool-call").record(delivery.elapsedTime());
 *     backlog.record(delivery.continuation(), delivery.outcome());
 * });
 * }</pre>
 *
 * @param computationId the computation this outcome belongs to
 * @param continuationId the stable deduplication key for at-least-once delivery
 * @param continuation the decoded continuation — what should receive the outcome
 * @param outcome the decoded terminal outcome
 * @param submittedAt when the computation was originally submitted
 * @param completedAt when the computation reached its terminal outcome
 * @param deliveryAttempt how many delivery attempts have already been made, 0 on first delivery.
 *     Distinct from {@code Computation.attemptCount}, which counts <em>dispatch</em> attempts:
 *     redelivering an outcome is not re-running the work.
 * @param <C> the continuation type
 * @param <R> the decoded result type
 */
public record TypedDelivery<C, R>(
    ComputationId computationId,
    ContinuationId continuationId,
    C continuation,
    TypedOutcome<R> outcome,
    Instant submittedAt,
    Instant completedAt,
    int deliveryAttempt) {

  /**
   * Requires everything but the decoded continuation, which a codec may legitimately decode to
   * null.
   *
   * @throws NullPointerException if any component other than {@code continuation} is null
   */
  public TypedDelivery {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(continuationId, "continuationId must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
    Objects.requireNonNull(submittedAt, "submittedAt must not be null");
    Objects.requireNonNull(completedAt, "completedAt must not be null");
  }

  /**
   * How long the computation took end to end: from {@code submittedAt} to {@code completedAt}.
   *
   * <p>This is the computation's own duration, not how long the delivery waited. It is available
   * here so a metrics fold can record it as the outcome arrives, without a second lookup against a
   * result row that may already have been purged.
   *
   * @return the time elapsed from submission to completion
   */
  public Duration elapsedTime() {
    return Duration.between(submittedAt, completedAt);
  }
}
