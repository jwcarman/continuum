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

public record Computation(
    ComputationId id,
    ComputationKind kind,
    ComputationStatus status,
    Instant createdAt,
    Instant deadline,
    byte[] dispatchPayload,
    int attemptCount,
    Outcome outcome) {

  public Computation {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be at least 1");
    }
  }

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
                Instant otherCreatedAt,
                Instant otherDeadline,
                byte[] otherDispatchPayload,
                int otherAttemptCount,
                Outcome otherOutcome)
        && attemptCount == otherAttemptCount
        && id.equals(otherId)
        && kind.equals(otherKind)
        && status == otherStatus
        && createdAt.equals(otherCreatedAt)
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
        createdAt,
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
