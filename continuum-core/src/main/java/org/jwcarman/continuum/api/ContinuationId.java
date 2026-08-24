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

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import java.util.Objects;
import java.util.UUID;

/**
 * The identity of one registered continuation — assigned by Continuum, and the stable deduplication
 * key for at-least-once delivery.
 *
 * @param value the underlying identity
 */
public record ContinuationId(UUID value) {

  private static final NoArgGenerator IDS = Generators.timeBasedEpochGenerator();

  /**
   * Requires an identity.
   *
   * @throws NullPointerException if {@code value} is null
   */
  public ContinuationId {
    Objects.requireNonNull(value, "value must not be null");
  }

  /**
   * A new identity — a time-ordered UUIDv7, not a random UUIDv4.
   *
   * <p>Identities are primary keys, and v7's leading millisecond timestamp makes successive inserts
   * land at the right-hand edge of the index instead of scattering across it. Within a millisecond
   * the generator is monotonic, so ordering holds under load rather than only between ticks.
   *
   * @return a fresh, globally unique, time-ordered id
   */
  public static ContinuationId random() {
    return new ContinuationId(IDS.generate());
  }
}
