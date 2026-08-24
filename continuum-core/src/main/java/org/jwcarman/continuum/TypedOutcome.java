package org.jwcarman.continuum;

import java.util.Objects;

public sealed interface TypedOutcome<R> {

  record Success<R>(R value) implements TypedOutcome<R> {
    public Success {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  record Failure<R>(String message) implements TypedOutcome<R> {
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  record Expired<R>(ExpiryKind kind, String message) implements TypedOutcome<R> {
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }
}
