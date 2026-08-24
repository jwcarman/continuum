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
package org.jwcarman.continuum.spi;

import java.util.Objects;
import org.jwcarman.continuum.api.Outcome;

/**
 * The repository-level answer to a registration: registered, resolved with the memoized outcome, or
 * unknown.
 */
public sealed interface RegistrationOutcome {

  /**
   * The computation was still pending, so the continuation was persisted and a delivery is
   * guaranteed. Carries no identity: the caller already knows the id it asked the repository to
   * store.
   */
  record Registered() implements RegistrationOutcome {}

  /**
   * The computation had already resolved, so nothing was persisted and the memoized outcome comes
   * back instead.
   *
   * @param outcome the memoized terminal outcome
   */
  record Resolved(Outcome outcome) implements RegistrationOutcome {

    /**
     * Requires the memoized outcome — this arm exists to carry it.
     *
     * @throws NullPointerException if {@code outcome} is null
     */
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }

  /**
   * No such computation — it never existed, or its result was purged. This arm is what the API
   * layer turns into a {@code ComputationNotFoundException}; the SPI reports it as data so
   * providers need not throw.
   */
  record NotFound() implements RegistrationOutcome {}
}
