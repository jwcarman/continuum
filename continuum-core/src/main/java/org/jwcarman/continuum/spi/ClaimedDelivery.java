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
import org.jwcarman.continuum.api.CompletionDelivery;

/**
 * One leased outbox item: the delivery, its outbox identity, and how many attempts preceded.
 *
 * @param id the outbox row's identity, used to acknowledge or fail the claim
 * @param delivery the delivery obligation to act on
 * @param attemptCount how many delivery attempts have already been made
 */
public record ClaimedDelivery(DeliveryId id, CompletionDelivery delivery, int attemptCount) {

  /**
   * Requires the outbox identity and the delivery it leases.
   *
   * @throws NullPointerException if {@code id} or {@code delivery} is null
   */
  public ClaimedDelivery {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(delivery, "delivery must not be null");
  }
}
