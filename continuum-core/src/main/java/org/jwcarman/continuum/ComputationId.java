package org.jwcarman.continuum;

import java.util.Objects;
import java.util.UUID;

public record ComputationId(UUID value) {
  public ComputationId {
    Objects.requireNonNull(value, "value must not be null");
  }

  public static ComputationId random() {
    return new ComputationId(UUID.randomUUID());
  }
}
