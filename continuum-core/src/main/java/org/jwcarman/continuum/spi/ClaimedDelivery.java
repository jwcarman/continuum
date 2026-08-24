package org.jwcarman.continuum.spi;

import java.util.Objects;
import org.jwcarman.continuum.CompletionDelivery;

public record ClaimedDelivery(DeliveryId id, CompletionDelivery delivery, int attemptCount) {

  public ClaimedDelivery {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(delivery, "delivery must not be null");
  }
}
