package org.jwcarman.continuum.spi;

import java.util.Objects;
import org.jwcarman.continuum.ContinuationId;

public record StoredContinuation(ContinuationId id, byte[] payload) {

  public StoredContinuation {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(payload, "payload must not be null");
  }
}
