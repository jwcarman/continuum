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
 * How long memoized results outlive completion before purging. Results are a coordination memo, not
 * an audit log — they only need to outlive the last plausible late registrant.
 */
public record ResultTtl(Duration value) implements TimeSpan {

  public ResultTtl {
    TimeSpan.requirePositive(value);
  }

  public static ResultTtl of(Duration value) {
    return new ResultTtl(value);
  }

  public static ResultTtl ofSeconds(long seconds) {
    return of(Duration.ofSeconds(seconds));
  }

  public static ResultTtl ofMinutes(long minutes) {
    return of(Duration.ofMinutes(minutes));
  }

  public static ResultTtl ofHours(long hours) {
    return of(Duration.ofHours(hours));
  }
}
