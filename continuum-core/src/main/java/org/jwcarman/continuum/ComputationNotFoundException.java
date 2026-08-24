package org.jwcarman.continuum;

public class ComputationNotFoundException extends RuntimeException {

  public ComputationNotFoundException(ComputationId id) {
    super("computation not found: " + id.value());
  }
}
