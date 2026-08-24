package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record ComputationRequest(
    ComputationKind kind, byte[] continuationPayload, Instant deadline, byte[] dispatchPayload) {

  public ComputationRequest {
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }
}
