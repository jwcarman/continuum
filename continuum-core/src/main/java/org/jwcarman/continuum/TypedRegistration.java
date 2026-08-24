package org.jwcarman.continuum;

import java.util.Objects;

public sealed interface TypedRegistration<R> {

  record Registered<R>(ContinuationId continuationId) implements TypedRegistration<R> {
    public Registered {
      Objects.requireNonNull(continuationId, "continuationId must not be null");
    }
  }

  record Resolved<R>(TypedOutcome<R> outcome) implements TypedRegistration<R> {
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }
}
