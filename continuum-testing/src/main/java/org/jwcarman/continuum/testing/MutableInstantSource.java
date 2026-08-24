package org.jwcarman.continuum.testing;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;

public final class MutableInstantSource implements InstantSource {

  private volatile Instant current;

  public MutableInstantSource(Instant start) {
    this.current = Objects.requireNonNull(start, "start must not be null");
  }

  @Override
  public Instant instant() {
    return current;
  }

  public void advance(Duration duration) {
    current = current.plus(duration);
  }

  public void set(Instant instant) {
    current = Objects.requireNonNull(instant, "instant must not be null");
  }
}
