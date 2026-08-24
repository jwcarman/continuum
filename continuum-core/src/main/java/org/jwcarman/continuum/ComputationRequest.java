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

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

public record ComputationRequest(
    ComputationKind kind, byte[] continuationPayload, Instant deadline, byte[] dispatchPayload) {

  public ComputationRequest {
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }

  @Override
  public boolean equals(Object o) {
    return o
            instanceof
            ComputationRequest(
                ComputationKind otherKind,
                byte[] otherContinuationPayload,
                Instant otherDeadline,
                byte[] otherDispatchPayload)
        && kind.equals(otherKind)
        && Arrays.equals(continuationPayload, otherContinuationPayload)
        && deadline.equals(otherDeadline)
        && Arrays.equals(dispatchPayload, otherDispatchPayload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kind, Arrays.hashCode(continuationPayload), deadline, Arrays.hashCode(dispatchPayload));
  }

  @Override
  public String toString() {
    return "ComputationRequest[kind=%s, deadline=%s, retryable=%b]"
        .formatted(kind.value(), deadline, dispatchPayload != null);
  }
}
