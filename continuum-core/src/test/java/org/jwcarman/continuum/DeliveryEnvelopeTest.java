/*
 * Copyright © 2026 James Carman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jwcarman.continuum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionDelivery;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.api.TypedDelivery;
import org.jwcarman.continuum.api.TypedOutcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;

/**
 * The delivery envelope exists so a consumer can see what {@code deliverResults} already knows —
 * identities, timestamps, and the delivery attempt count. Before this, those were reachable only by
 * dropping to the raw SPI, which made the timestamps on {@code CompletionDelivery} ornamental.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DeliveryEnvelopeTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("tool");

  private Continuum continuum;
  private ContinuumRepository repository;

  @BeforeEach
  void set_up() {
    continuum = mock(Continuum.class, CALLS_REAL_METHODS);
    repository = mock(ContinuumRepository.class);
    when(continuum.instants()).thenReturn(InstantSource.fixed(NOW));
    when(continuum.repository()).thenReturn(repository);
  }

  private ContinuumClient<String, String> client() {
    return continuum.client(
        "tool",
        String.class,
        String.class,
        cfg ->
            cfg.resultCodec(ClientMintingTest.STRINGS)
                .continuationCodec(ClientMintingTest.STRINGS)
                .deadline(Duration.ofMinutes(5)));
  }

  private ClaimedDelivery claim(int deliveryAttempt) {
    return new ClaimedDelivery(
        DeliveryId.random(),
        new CompletionDelivery(
            ComputationId.random(),
            KIND,
            ContinuationId.random(),
            "cont".getBytes(UTF_8),
            Outcome.success("r".getBytes(UTF_8)),
            NOW.minus(Duration.ofMinutes(90)),
            NOW.minus(Duration.ofMinutes(30))),
        deliveryAttempt);
  }

  @Nested
  class The_envelope {
    @Test
    void carries_every_fact_the_two_argument_consumer_cannot_see() {
      var claimed = claim(3);
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any()))
          .thenReturn(List.of(claimed));

      var seen = new ArrayList<TypedDelivery<String, String>>();
      // NOTE: `seen::add` will NOT compile here — an overloaded method reference cannot
      // disambiguate the two deliverResults overloads. An explicit lambda can.
      int delivered =
          client()
              .deliverResults(BatchSize.of(10), (TypedDelivery<String, String> d) -> seen.add(d));

      assertThat(delivered).isEqualTo(1);
      assertThat(seen)
          .singleElement()
          .satisfies(
              d -> {
                assertThat(d.computationId()).isEqualTo(claimed.delivery().computationId());
                assertThat(d.continuationId()).isEqualTo(claimed.delivery().continuationId());
                assertThat(d.continuation()).isEqualTo("cont");
                assertThat(d.outcome()).isEqualTo(new TypedOutcome.Success<>("r"));
                assertThat(d.submittedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(90)));
                assertThat(d.completedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(30)));
                assertThat(d.deliveryAttempt()).isEqualTo(3);
                assertThat(d.elapsedTime()).isEqualTo(Duration.ofMinutes(60));
              });
    }

    @Test
    void a_throwing_consumer_still_releases_for_retry() {
      var claimed = claim(0);
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any()))
          .thenReturn(List.of(claimed));

      int delivered =
          client()
              .deliverResults(
                  BatchSize.of(10),
                  (TypedDelivery<String, String> d) -> {
                    throw new IllegalStateException("boom");
                  });

      assertThat(delivered).isZero();
      verify(repository).releaseDelivery(any(), any());
      verify(repository, never()).acknowledgeDelivery(any());
    }
  }
}
