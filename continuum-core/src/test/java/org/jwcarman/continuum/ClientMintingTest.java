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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.codec.spi.Codec;
import org.mockito.ArgumentCaptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ClientMintingTest {

  static final Codec<String> STRINGS =
      new Codec<>() {
        @Override
        public byte[] encode(String value) {
          return value.getBytes(UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
          return new String(bytes, UTF_8);
        }
      };

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  private Continuum continuum;

  @BeforeEach
  void set_up() {
    continuum = mock(Continuum.class, CALLS_REAL_METHODS);
    when(continuum.instants()).thenReturn(InstantSource.fixed(NOW));
  }

  private RetryableContinuumClient<String, String, String> retryableClient() {
    return continuum.client(
        "tool",
        String.class,
        String.class,
        String.class,
        cfg ->
            cfg.resultCodec(STRINGS)
                .continuationCodec(STRINGS)
                .dispatchCodec(STRINGS)
                .deadline(Duration.ofMinutes(5)));
  }

  @Nested
  class Minting {
    @Test
    void deadline_is_required() {
      assertThatIllegalStateException()
          .isThrownBy(
              () ->
                  continuum.client(
                      "k",
                      String.class,
                      String.class,
                      cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS)));
    }

    @Test
    void unresolvable_codec_fails_at_mint_time() {
      assertThatIllegalStateException()
          .isThrownBy(
              () ->
                  continuum.client(
                      "k",
                      String.class,
                      String.class,
                      cfg -> cfg.continuationCodec(STRINGS).deadline(Duration.ofMinutes(1))));
    }
  }

  @Nested
  class Creating {
    @Test
    void retryable_create_encodes_both_payloads_and_computes_the_deadline() {
      when(continuum.create(any()))
          .thenAnswer(
              invocation -> {
                ComputationRequest request = invocation.getArgument(0);
                return new Computation(
                    ComputationId.random(),
                    request.kind(),
                    ComputationStatus.PENDING,
                    NOW,
                    request.deadline(),
                    request.dispatchPayload(),
                    1,
                    null);
              });

      retryableClient().create("my-continuation", "my-dispatch");

      var captor = ArgumentCaptor.forClass(ComputationRequest.class);
      verify(continuum).create(captor.capture());
      var request = captor.getValue();
      assertThat(request.kind()).isEqualTo(new ComputationKind("tool"));
      assertThat(request.continuationPayload()).isEqualTo("my-continuation".getBytes(UTF_8));
      assertThat(request.dispatchPayload()).isEqualTo("my-dispatch".getBytes(UTF_8));
      assertThat(request.deadline()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void one_shot_create_never_carries_a_dispatch_payload() {
      when(continuum.create(any()))
          .thenAnswer(
              invocation -> {
                ComputationRequest request = invocation.getArgument(0);
                return new Computation(
                    ComputationId.random(),
                    request.kind(),
                    ComputationStatus.PENDING,
                    NOW,
                    request.deadline(),
                    request.dispatchPayload(),
                    1,
                    null);
              });
      var client =
          continuum.client(
              "approval",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(STRINGS).continuationCodec(STRINGS).deadline(Duration.ofDays(3)));

      var computation = client.create("who-to-tell");
      assertThat(computation.dispatchPayload()).isNull();
      assertThat(computation.retryable()).isFalse();
    }
  }

  @Nested
  class Completing_and_registering {
    @Test
    void complete_encodes_the_result_as_success() {
      var id = ComputationId.random();
      when(continuum.complete(eq(id), any())).thenReturn(CompletionResult.COMPLETED);
      retryableClient().complete(id, "the-result");
      verify(continuum).complete(id, Outcome.success("the-result".getBytes(UTF_8)));
    }

    @Test
    void fail_reports_a_producer_failure() {
      var id = ComputationId.random();
      when(continuum.complete(eq(id), any())).thenReturn(CompletionResult.COMPLETED);
      retryableClient().fail(id, "tool blew up");
      verify(continuum).complete(id, Outcome.failure("tool blew up"));
    }

    @Test
    void register_decodes_a_resolved_outcome() {
      var id = ComputationId.random();
      when(continuum.registerContinuation(eq(id), any()))
          .thenReturn(new RegistrationResult.Resolved(Outcome.success("r".getBytes(UTF_8))));
      var registration = retryableClient().register(id, "late-party");
      assertThat(registration)
          .isEqualTo(new TypedRegistration.Resolved<>(new TypedOutcome.Success<>("r")));
    }
  }
}
