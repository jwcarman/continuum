package org.jwcarman.continuum;

import java.time.Duration;

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
