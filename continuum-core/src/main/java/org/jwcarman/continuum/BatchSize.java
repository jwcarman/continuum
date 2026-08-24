package org.jwcarman.continuum;

public record BatchSize(int value) {

  public BatchSize {
    if (value < 1) {
      throw new IllegalArgumentException("value must be at least 1");
    }
  }

  public static BatchSize of(int value) {
    return new BatchSize(value);
  }
}
