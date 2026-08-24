package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record Computation(
    ComputationId id,
    ComputationKind kind,
    ComputationStatus status,
    Instant createdAt,
    Instant deadline,
    byte[] dispatchPayload,
    int attemptCount,
    Outcome outcome) {

  public Computation {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be at least 1");
    }
  }

  public boolean retryable() {
    return dispatchPayload != null;
  }
}
