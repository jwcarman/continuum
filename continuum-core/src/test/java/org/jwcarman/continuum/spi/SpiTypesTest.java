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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.ContinuationId;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SpiTypesTest {

  @Test
  void random_delivery_ids_are_unique() {
    assertThat(DeliveryId.random()).isNotEqualTo(DeliveryId.random());
  }

  @Test
  void persistence_exception_carries_message_and_cause() {
    var cause = new IllegalStateException("root");
    var wrapped = new ContinuumPersistenceException("failed", cause);
    assertThat(wrapped).hasMessage("failed").hasCause(cause);
    assertThat(new ContinuumPersistenceException("bare")).hasMessage("bare").hasNoCause();
  }

  @Test
  void stored_continuation_requires_payload() {
    assertThatNullPointerException()
        .isThrownBy(() -> new StoredContinuation(ContinuationId.random(), null));
  }
}
