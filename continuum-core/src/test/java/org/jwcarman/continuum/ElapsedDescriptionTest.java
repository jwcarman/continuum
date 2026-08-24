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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

/**
 * The elapsed-time prose on an expired outcome is user-visible — it lands in logs and in {@code
 * continuum_result.message} — so its shape is pinned here rather than left to chance.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ElapsedDescriptionTest {

  @Test
  void every_unit_is_named_and_singular_forms_drop_the_s() {
    assertThat(
            ClientSupport.describeElapsed(
                Duration.ofDays(1).plusHours(1).plusMinutes(1).plusSeconds(1)))
        .isEqualTo("1 day, 1 hour, 1 min, 1 sec");
  }

  @Test
  void plural_forms_take_the_s() {
    assertThat(
            ClientSupport.describeElapsed(
                Duration.ofDays(2).plusHours(3).plusMinutes(4).plusSeconds(5)))
        .isEqualTo("2 days, 3 hours, 4 mins, 5 secs");
  }

  @Test
  void zero_valued_parts_are_omitted_rather_than_padded() {
    assertThat(ClientSupport.describeElapsed(Duration.ofSeconds(90))).isEqualTo("1 min, 30 secs");
    assertThat(ClientSupport.describeElapsed(Duration.ofSeconds(45))).isEqualTo("45 secs");
    assertThat(ClientSupport.describeElapsed(Duration.ofDays(3))).isEqualTo("3 days");
  }

  @Test
  void a_sub_second_wait_says_so_rather_than_rendering_empty() {
    assertThat(ClientSupport.describeElapsed(Duration.ofMillis(500)))
        .isEqualTo("less than a second");
  }

  @Test
  void zero_and_negative_elapsed_times_read_as_zero() {
    assertThat(ClientSupport.describeElapsed(Duration.ZERO)).isEqualTo("0 secs");
    assertThat(ClientSupport.describeElapsed(Duration.ofSeconds(-30))).isEqualTo("0 secs");
  }
}
