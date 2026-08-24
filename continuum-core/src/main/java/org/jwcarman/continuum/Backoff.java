package org.jwcarman.continuum;

import java.time.Duration;

public record Backoff(Duration value) implements TimeSpan {

  public Backoff {
    TimeSpan.requirePositive(value);
  }

  public static Backoff of(Duration value) {
    return new Backoff(value);
  }

  public static Backoff ofSeconds(long seconds) {
    return of(Duration.ofSeconds(seconds));
  }

  public static Backoff ofMinutes(long minutes) {
    return of(Duration.ofMinutes(minutes));
  }

  public static Backoff ofHours(long hours) {
    return of(Duration.ofHours(hours));
  }
}
