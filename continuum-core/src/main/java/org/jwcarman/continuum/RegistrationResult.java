package org.jwcarman.continuum;

import java.util.Objects;

public sealed interface RegistrationResult {

  record Registered(ContinuationId continuationId) implements RegistrationResult {
    public Registered {
      Objects.requireNonNull(continuationId, "continuationId must not be null");
    }
  }

  record Resolved(Outcome outcome) implements RegistrationResult {
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }
}
