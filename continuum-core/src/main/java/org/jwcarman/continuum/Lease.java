package org.jwcarman.continuum;

import java.time.Duration;

public record Lease(Duration value) implements TimeSpan {

  public Lease {
    TimeSpan.requirePositive(value);
  }

  public static Lease of(Duration value) {
    return new Lease(value);
  }

  public static Lease ofSeconds(long seconds) {
    return of(Duration.ofSeconds(seconds));
  }

  public static Lease ofMinutes(long minutes) {
    return of(Duration.ofMinutes(minutes));
  }

  public static Lease ofHours(long hours) {
    return of(Duration.ofHours(hours));
  }
}
