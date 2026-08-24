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

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * The durable view of a computation. Status is always derived, never stored: {@code PENDING} while
 * a pending record exists, otherwise a 1:1 reading of the memoized outcome. {@code dispatchPayload}
 * presence is what retryable means; {@code attemptCount} starts at 1 (the original dispatch is
 * attempt one) and is the only retry state Continuum persists.
 *
 * @param id the globally unique identity
 * @param kind the computation kind
 * @param status the derived status, never stored
 * @param submittedAt when the computation was created
 * @param deadline the current attempt's absolute deadline
 * @param dispatchPayload opaque bytes replayed on every redispatch, or null if not retryable
 * @param attemptCount how many attempts have run, counting the original dispatch as 1
 * @param outcome the memoized terminal outcome, or null while pending
 */
public record Computation(
    ComputationId id,
    ComputationKind kind,
    ComputationStatus status,
    Instant submittedAt,
    Instant deadline,
    byte[] dispatchPayload,
    int attemptCount,
    Outcome outcome) {

  /**
   * Requires the durable facts; {@code dispatchPayload} and {@code outcome} stay nullable because
   * their absence is meaningful — not retryable, and still pending, respectively.
   *
   * @throws IllegalArgumentException if {@code attemptCount} is less than 1
   * @throws NullPointerException if {@code id}, {@code kind}, {@code status}, {@code submittedAt},
   *     or {@code deadline} is null
   */
  public Computation {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(submittedAt, "submittedAt must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be at least 1");
    }
  }

  /**
   * Whether a dispatch payload exists — the definition of retryable.
   *
   * @return true if this computation can be redispatched
   */
  public boolean retryable() {
    return dispatchPayload != null;
  }

  @Override
  public boolean equals(Object o) {
    return o
            instanceof
            Computation(
                ComputationId otherId,
                ComputationKind otherKind,
                ComputationStatus otherStatus,
                Instant otherSubmittedAt,
                Instant otherDeadline,
                byte[] otherDispatchPayload,
                int otherAttemptCount,
                Outcome otherOutcome)
        && attemptCount == otherAttemptCount
        && id.equals(otherId)
        && kind.equals(otherKind)
        && status == otherStatus
        && submittedAt.equals(otherSubmittedAt)
        && deadline.equals(otherDeadline)
        && Arrays.equals(dispatchPayload, otherDispatchPayload)
        && Objects.equals(outcome, otherOutcome);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        kind,
        status,
        submittedAt,
        deadline,
        Arrays.hashCode(dispatchPayload),
        attemptCount,
        outcome);
  }

  @Override
  public String toString() {
    return "Computation[id=%s, kind=%s, status=%s, attemptCount=%d, outcome=%s]"
        .formatted(id.value(), kind.value(), status, attemptCount, outcome);
  }
}
