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

import java.util.Arrays;
import java.util.Objects;

public record CompletionDelivery(
    ComputationId computationId,
    ComputationKind kind,
    ContinuationId continuationId,
    byte[] continuationPayload,
    Outcome outcome) {

  public CompletionDelivery {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationId, "continuationId must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CompletionDelivery other
        && computationId.equals(other.computationId)
        && kind.equals(other.kind)
        && continuationId.equals(other.continuationId)
        && Arrays.equals(continuationPayload, other.continuationPayload)
        && outcome.equals(other.outcome);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        computationId, kind, continuationId, Arrays.hashCode(continuationPayload), outcome);
  }

  @Override
  public String toString() {
    return "CompletionDelivery[computationId=%s, kind=%s, continuationId=%s, outcome=%s]"
        .formatted(computationId.value(), kind.value(), continuationId.value(), outcome);
  }
}
