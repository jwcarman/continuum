package org.jwcarman.continuum;

import java.util.Objects;

public record ComputationKind(String value) {
  public ComputationKind {
    Objects.requireNonNull(value, "value must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
