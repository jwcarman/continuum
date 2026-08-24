package org.jwcarman.continuum.spi;

import java.util.Objects;
import org.jwcarman.continuum.Outcome;

public sealed interface RegistrationOutcome {

  record Registered() implements RegistrationOutcome {}

  record Resolved(Outcome outcome) implements RegistrationOutcome {
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }

  record NotFound() implements RegistrationOutcome {}
}
