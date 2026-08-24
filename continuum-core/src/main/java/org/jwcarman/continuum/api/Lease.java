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
 * How long a claimed delivery stays invisible to other claimers. Must exceed the worst-case
 * consumer time, or another node may reclaim mid-processing.
 *
 * @param value the invisibility window, positive
 */
public record Lease(Duration value) implements TimeSpan {

  /**
   * Requires a positive duration — a non-positive lease would leave a claimed delivery immediately
   * visible to other claimers.
   *
   * @throws IllegalArgumentException if {@code value} is not positive
   * @throws NullPointerException if {@code value} is null
   */
  public Lease {
    TimeSpan.requirePositive(value);
  }

  /**
   * A lease of the given duration.
   *
   * @param value the duration, positive
   * @return the lease
   */
  public static Lease of(Duration value) {
    return new Lease(value);
  }

  /**
   * A lease of the given seconds.
   *
   * @param seconds the duration in seconds
   * @return the lease
   */
  public static Lease ofSeconds(long seconds) {
    return of(Duration.ofSeconds(seconds));
  }

  /**
   * A lease of the given minutes.
   *
   * @param minutes the duration in minutes
   * @return the lease
   */
  public static Lease ofMinutes(long minutes) {
    return of(Duration.ofMinutes(minutes));
  }

  /**
   * A lease of the given hours.
   *
   * @param hours the duration in hours
   * @return the lease
   */
  public static Lease ofHours(long hours) {
    return of(Duration.ofHours(hours));
  }
}
