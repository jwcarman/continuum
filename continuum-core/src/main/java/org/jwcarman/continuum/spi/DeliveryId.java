package org.jwcarman.continuum.spi;

import java.util.Objects;
import java.util.UUID;

public record DeliveryId(UUID value) {

  public DeliveryId {
    Objects.requireNonNull(value, "value must not be null");
  }

  public static DeliveryId random() {
    return new DeliveryId(UUID.randomUUID());
  }
}
