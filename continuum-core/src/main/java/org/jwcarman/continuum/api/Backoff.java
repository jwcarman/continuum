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

import java.time.Duration;

/**
 * How long a failed delivery waits before becoming claimable again.
 *
 * @param value the wait before a failed delivery is retried, positive
 */
public record Backoff(Duration value) implements TimeSpan {

  /**
   * Requires a positive duration — a zero or negative backoff would make a failing delivery
   * immediately reclaimable, spinning the pump.
   *
   * @throws IllegalArgumentException if {@code value} is not positive
   * @throws NullPointerException if {@code value} is null
   */
  public Backoff {
    TimeSpan.requirePositive(value);
  }

  /**
   * A backoff of the given duration.
   *
   * @param value the duration, positive
   * @return the backoff
   */
  public static Backoff of(Duration value) {
    return new Backoff(value);
  }

  /**
   * A backoff of the given seconds.
   *
   * @param seconds the duration in seconds
   * @return the backoff
   */
  public static Backoff ofSeconds(long seconds) {
    return of(Duration.ofSeconds(seconds));
  }

  /**
   * A backoff of the given minutes.
   *
   * @param minutes the duration in minutes
   * @return the backoff
   */
  public static Backoff ofMinutes(long minutes) {
    return of(Duration.ofMinutes(minutes));
  }

  /**
   * A backoff of the given hours.
   *
   * @param hours the duration in hours
   * @return the backoff
   */
  public static Backoff ofHours(long hours) {
    return of(Duration.ofHours(hours));
  }
}
