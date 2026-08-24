package org.jwcarman.continuum;

import java.util.Arrays;
import java.util.Objects;

public sealed interface Outcome {

  record Success(byte[] payload) implements Outcome {
    public Success {
      Objects.requireNonNull(payload, "payload must not be null");
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Success other && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
      return "Success[" + payload.length + " bytes]";
    }
  }

  record Failure(String message) implements Outcome {
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  record Expired(ExpiryKind kind, String message) implements Outcome {
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  static Outcome success(byte[] payload) {
    return new Success(payload);
  }

  static Outcome failure(String message) {
    return new Failure(message);
  }

  static Outcome expired(ExpiryKind kind, String message) {
    return new Expired(kind, message);
  }

  static ComputationStatus statusOf(Outcome outcome) {
    return switch (outcome) {
      case Success _ -> ComputationStatus.COMPLETED;
      case Failure _ -> ComputationStatus.FAILED;
      case Expired _ -> ComputationStatus.EXPIRED;
    };
  }
}
