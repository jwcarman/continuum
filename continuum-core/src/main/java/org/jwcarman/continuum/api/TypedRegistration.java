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

import java.util.Objects;

/**
 * The typed mirror of {@link RegistrationResult}: durably {@link Registered}, or {@link Resolved}
 * with the decoded outcome.
 *
 * @param <R> the decoded result type
 */
public sealed interface TypedRegistration<R> {

  /**
   * The computation was still pending, so the continuation is now durable and a delivery is
   * guaranteed to follow.
   *
   * @param continuationId the Continuum-assigned identity of the registered continuation
   * @param <R> the decoded result type
   */
  record Registered<R>(ContinuationId continuationId) implements TypedRegistration<R> {

    /**
     * Requires the assigned identity — it is the caller's deduplication key.
     *
     * @throws NullPointerException if {@code continuationId} is null
     */
    public Registered {
      Objects.requireNonNull(continuationId, "continuationId must not be null");
    }
  }

  /**
   * The computation had already resolved, so the memoized outcome comes back decoded and nothing
   * was persisted — no continuation exists and no delivery will follow.
   *
   * @param outcome the memoized terminal outcome, decoded
   * @param <R> the decoded result type
   */
  record Resolved<R>(TypedOutcome<R> outcome) implements TypedRegistration<R> {

    /**
     * Requires the memoized outcome — this arm exists to carry it.
     *
     * @throws NullPointerException if {@code outcome} is null
     */
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }
}
