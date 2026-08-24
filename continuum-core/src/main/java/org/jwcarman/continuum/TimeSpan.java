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
