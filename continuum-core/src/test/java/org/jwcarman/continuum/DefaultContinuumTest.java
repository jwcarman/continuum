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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.InstantSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;
import org.mockito.ArgumentCaptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultContinuumTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("k");

  private ContinuumRepository repository;
  private DefaultContinuum continuum;

  @BeforeEach
  void set_up() {
    repository = mock(ContinuumRepository.class);
    continuum = new DefaultContinuum(repository, InstantSource.fixed(NOW));
  }

  private ComputationRequest request() {
    return new ComputationRequest(
        KIND, "c".getBytes(UTF_8), NOW.plusSeconds(300), "d".getBytes(UTF_8));
  }

  @Nested
  class Creating {
    @Test
    void persists_pending_computation_with_initial_continuation() {
      var computation = continuum.create(request());

      var computationCaptor = ArgumentCaptor.forClass(Computation.class);
      var continuationCaptor = ArgumentCaptor.forClass(StoredContinuation.class);
      verify(repository)
          .createComputation(computationCaptor.capture(), continuationCaptor.capture());

      assertThat(computationCaptor.getValue()).isEqualTo(computation);
      assertThat(computation.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(computation.createdAt()).isEqualTo(NOW);
      assertThat(computation.attemptCount()).isEqualTo(1);
      assertThat(computation.outcome()).isNull();
      assertThat(continuationCaptor.getValue().payload()).isEqualTo("c".getBytes(UTF_8));
    }
  }

  @Nested
  class Registering {
    @Test
    void registered_result_carries_the_generated_continuation_id() {
      when(repository.registerContinuation(any(), any()))
          .thenReturn(new RegistrationOutcome.Registered());
      var result = continuum.registerContinuation(ComputationId.random(), "x".getBytes(UTF_8));
      assertThat(result).isInstanceOf(RegistrationResult.Registered.class);
    }

    @Test
    void resolved_result_carries_the_memoized_outcome() {
      var outcome = Outcome.success("r".getBytes(UTF_8));
      when(repository.registerContinuation(any(), any()))
          .thenReturn(new RegistrationOutcome.Resolved(outcome));
      var result = continuum.registerContinuation(ComputationId.random(), "x".getBytes(UTF_8));
      assertThat(result).isEqualTo(new RegistrationResult.Resolved(outcome));
    }

    @Test
    void unknown_computation_throws() {
      when(repository.registerContinuation(any(), any()))
          .thenReturn(new RegistrationOutcome.NotFound());
      var id = ComputationId.random();
      byte[] payload = "x".getBytes(UTF_8);
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(id, payload));
    }
  }

  @Nested
  class Completing {
    @Test
    void rejects_expired_outcomes() {
      var id = ComputationId.random();
      var expired = Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (3 of 3)");
      assertThatIllegalArgumentException().isThrownBy(() -> continuum.complete(id, expired));
    }

    @Test
    void maps_repository_outcomes() {
      var id = ComputationId.random();
      var outcome = Outcome.success("r".getBytes(UTF_8));
      when(repository.complete(id, outcome, NOW)).thenReturn(CompletionOutcome.ALREADY_RESOLVED);
      assertThat(continuum.complete(id, outcome)).isEqualTo(CompletionResult.ALREADY_RESOLVED);
    }
  }
}
