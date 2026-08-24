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
import java.util.UUID;

/**
 * The identity of one registered continuation — assigned by Continuum, and the stable deduplication
 * key for at-least-once delivery.
 */
public record ContinuationId(UUID value) {
  public ContinuationId {
    Objects.requireNonNull(value, "value must not be null");
  }

  public static ContinuationId random() {
    return new ContinuationId(UUID.randomUUID());
  }
}
