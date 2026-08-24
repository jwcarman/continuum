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
import java.util.Arrays;
import java.util.Objects;

/**
 * One self-contained delivery obligation: everything a consumer needs to act on a terminal outcome
 * without reconstructing transient state. {@code continuationId} is the stable deduplication key
 * for at-least-once delivery.
 *
 * <p>{@code submittedAt} and {@code completedAt} are the computation's own timestamps, carried onto
 * the delivery so a consumer can record end-to-end latency at the moment the outcome reaches it,
 * without a second lookup. They are denormalized for the same reason the outcome is: a delivery
 * must stay actionable after its memoized result has been purged.
 *
 * @param computationId the computation the delivery belongs to
 * @param kind the computation kind
 * @param continuationId the stable deduplication key for this delivery
 * @param continuationPayload opaque bytes describing what should receive the outcome
 * @param outcome the terminal outcome being delivered
 * @param submittedAt when the computation was originally submitted — its {@code createdAt}
 * @param completedAt when the computation reached its terminal outcome
 */
public record CompletionDelivery(
    ComputationId computationId,
    ComputationKind kind,
    ContinuationId continuationId,
    byte[] continuationPayload,
    Outcome outcome,
    Instant submittedAt,
    Instant completedAt) {

  /**
   * Requires every component — a delivery must be actionable without further lookups.
   *
   * @throws NullPointerException if any component is null
   */
  public CompletionDelivery {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationId, "continuationId must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
    Objects.requireNonNull(submittedAt, "submittedAt must not be null");
    Objects.requireNonNull(completedAt, "completedAt must not be null");
  }

  /**
   * How long the computation took end to end: from {@code submittedAt} to {@code completedAt}.
   *
   * <p>The same measurement {@code ExpiryContext.elapsedTime()} reports for a still-pending
   * computation — time since submission — here fixed at the terminal outcome.
   *
   * @return the time elapsed from submission to completion
   */
  public Duration elapsedTime() {
    return Duration.between(submittedAt, completedAt);
  }

  @Override
  public boolean equals(Object o) {
    return o
            instanceof
            CompletionDelivery(
                ComputationId otherComputationId,
                ComputationKind otherKind,
                ContinuationId otherContinuationId,
                byte[] otherContinuationPayload,
                Outcome otherOutcome,
                Instant otherSubmittedAt,
                Instant otherCompletedAt)
        && computationId.equals(otherComputationId)
        && kind.equals(otherKind)
        && continuationId.equals(otherContinuationId)
        && Arrays.equals(continuationPayload, otherContinuationPayload)
        && outcome.equals(otherOutcome)
        && submittedAt.equals(otherSubmittedAt)
        && completedAt.equals(otherCompletedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        computationId,
        kind,
        continuationId,
        Arrays.hashCode(continuationPayload),
        outcome,
        submittedAt,
        completedAt);
  }

  @Override
  public String toString() {
    return "CompletionDelivery[computationId=%s, kind=%s, continuationId=%s, outcome=%s]"
        .formatted(computationId.value(), kind.value(), continuationId.value(), outcome);
  }
}
