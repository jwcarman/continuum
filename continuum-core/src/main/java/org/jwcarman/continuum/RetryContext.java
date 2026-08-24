package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record RetryContext(
    ComputationId computationId, ComputationKind kind, int attemptCount, Instant deadline) {

  public RetryContext {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }
}
