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
package org.jwcarman.continuum.testing;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;

/** A settable, advanceable {@link InstantSource} so tests own time. */
public final class MutableInstantSource implements InstantSource {

  private volatile Instant current;

  /**
   * Starts the clock at a fixed instant. Time moves only when a test calls {@link
   * #advance(Duration)} or {@link #set(Instant)}.
   *
   * @param start the initial instant
   */
  public MutableInstantSource(Instant start) {
    this.current = Objects.requireNonNull(start, "start must not be null");
  }

  @Override
  public Instant instant() {
    return current;
  }

  /**
   * Moves time forward.
   *
   * @param duration how far to advance
   */
  public void advance(Duration duration) {
    current = current.plus(duration);
  }

  /**
   * Sets the current instant.
   *
   * @param instant the new now
   */
  public void set(Instant instant) {
    current = Objects.requireNonNull(instant, "instant must not be null");
  }
}
