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

import java.time.Duration;
import java.util.Objects;

/**
 * Common shape of the duration-valued pump parameters. Java statics are not inherited, so each
 * implementation declares its own {@code of}/{@code ofSeconds}/{@code ofMinutes}/{@code ofHours}
 * factories as one-liners; this interface carries the shared value shape and validation.
 */
public sealed interface TimeSpan permits Lease, Backoff, ResultTtl {

  Duration value();

  static Duration requirePositive(Duration value) {
    Objects.requireNonNull(value, "value must not be null");
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException("value must be positive");
    }
    return value;
  }
}
