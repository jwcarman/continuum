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
import java.util.UUID;

/**
 * The identity of one outbox row.
 *
 * @param value the underlying identity
 */
public record DeliveryId(UUID value) {

  /**
   * Requires an identity.
   *
   * @throws NullPointerException if {@code value} is null
   */
  public DeliveryId {
    Objects.requireNonNull(value, "value must not be null");
  }

  /**
   * A new random identity.
   *
   * @return a fresh, globally unique id
   */
  public static DeliveryId random() {
    return new DeliveryId(UUID.randomUUID());
  }
}
