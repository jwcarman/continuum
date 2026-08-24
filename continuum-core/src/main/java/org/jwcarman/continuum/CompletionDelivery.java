package org.jwcarman.continuum;

import java.util.Objects;

public record CompletionDelivery(
    ComputationId computationId,
    ComputationKind kind,
    ContinuationId continuationId,
    byte[] continuationPayload,
    Outcome outcome) {

  public CompletionDelivery {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationId, "continuationId must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
  }
}
