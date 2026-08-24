package org.jwcarman.continuum.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.ContinuationId;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SpiTypesTest {

  @Test
  void random_delivery_ids_are_unique() {
    assertThat(DeliveryId.random()).isNotEqualTo(DeliveryId.random());
  }

  @Test
  void stored_continuation_requires_payload() {
    assertThatNullPointerException()
        .isThrownBy(() -> new StoredContinuation(ContinuationId.random(), null));
  }
}
