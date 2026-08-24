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
package org.jwcarman.continuum.retry;

import java.time.Instant;
import java.util.Objects;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;

/**
 * Continuum's durable facts about an expired computation, handed to a {@link Retry}: where the
 * redispatched worker must report ({@code computationId}), the kind, how many attempts have run
 * ({@code attemptCount} — the original dispatch was attempt 1), and the deadline that expired.
 */
public record RetryContext(
    ComputationId computationId, ComputationKind kind, int attemptCount, Instant deadline) {

  public RetryContext {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }
}
