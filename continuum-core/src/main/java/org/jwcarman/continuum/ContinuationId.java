package org.jwcarman.continuum;

import java.util.Objects;
import java.util.UUID;

public record ContinuationId(UUID value) {
  public ContinuationId {
    Objects.requireNonNull(value, "value must not be null");
  }

  public static ContinuationId random() {
    return new ContinuationId(UUID.randomUUID());
  }
}
