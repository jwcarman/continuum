package org.jwcarman.continuum.spi;

public class ContinuumPersistenceException extends RuntimeException {

  public ContinuumPersistenceException(String message) {
    super(message);
  }

  public ContinuumPersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
