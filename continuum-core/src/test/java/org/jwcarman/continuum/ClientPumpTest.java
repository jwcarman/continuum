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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;
import org.jwcarman.codec.spi.TypeRef;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.StoredContinuation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ClientPumpTest {

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

  private RetryableContinuumClient<String, String, String> retryable() {
    return continuum.client(
        "tool",
        String.class,
        String.class,
        String.class,
        cfg ->
            cfg.resultCodec(ClientMintingTest.STRINGS)
                .continuationCodec(ClientMintingTest.STRINGS)
                .dispatchCodec(ClientMintingTest.STRINGS)
                .deadline(Duration.ofMinutes(5)));
  }

  private Computation expired(byte[] dispatchPayload, int attemptCount) {
    return new Computation(
        ComputationId.random(),
        KIND,
        ComputationStatus.PENDING,
        NOW.minusSeconds(600),
        NOW.minusSeconds(10),
        dispatchPayload,
        attemptCount,
        null);
  }

  @Nested
  class Delivering {
    @Test
    void call_site_lease_is_used_for_claims() {
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any())).thenReturn(List.of());
      retryable()
          .deliverResults(
              BatchSize.of(7), Lease.ofSeconds(45), Backoff.ofSeconds(20), (c, o) -> {});
      verify(repository)
          .claimDeliveries(any(), eq(KIND), eq(7), eq(Duration.ofSeconds(45)), eq(NOW));
    }

    @Test
    void default_lease_is_used_when_not_supplied() {
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any())).thenReturn(List.of());
      retryable().deliverResults(BatchSize.of(7), (c, o) -> {});
      verify(repository)
          .claimDeliveries(any(), eq(KIND), eq(7), eq(Duration.ofSeconds(30)), eq(NOW));
    }

    @Test
    void consumer_failure_releases_with_the_configured_backoff() {
      var delivery =
          new ClaimedDelivery(
              DeliveryId.random(),
              new CompletionDelivery(
                  ComputationId.random(),
                  KIND,
                  ContinuationId.random(),
                  "cont".getBytes(UTF_8),
                  Outcome.success("r".getBytes(UTF_8))),
              0);
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any()))
          .thenReturn(List.of(delivery));

      int delivered =
          retryable()
              .deliverResults(
                  BatchSize.of(10),
                  Lease.ofSeconds(30),
                  Backoff.ofSeconds(20),
                  (c, o) -> {
                    throw new IllegalStateException("boom");
                  });

      assertThat(delivered).isZero();
      verify(repository).releaseDelivery(delivery.id(), NOW.plus(Duration.ofSeconds(20)));
      verify(repository, never()).acknowledgeDelivery(any());
    }

    @Test
    void failure_and_expired_outcomes_decode_to_their_typed_arms() {
      var failure =
          new ClaimedDelivery(
              DeliveryId.random(),
              new CompletionDelivery(
                  ComputationId.random(),
                  KIND,
                  ContinuationId.random(),
                  "cont".getBytes(UTF_8),
                  Outcome.failure("producer said no")),
              0);
      var expired =
          new ClaimedDelivery(
              DeliveryId.random(),
              new CompletionDelivery(
                  ComputationId.random(),
                  KIND,
                  ContinuationId.random(),
                  "cont".getBytes(UTF_8),
                  Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "deadline passed")),
              0);
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any()))
          .thenReturn(List.of(failure, expired));

      var outcomes = new java.util.ArrayList<TypedOutcome<String>>();
      int delivered = retryable().deliverResults(BatchSize.of(10), (c, o) -> outcomes.add(o));

      assertThat(delivered).isEqualTo(2);
      assertThat(outcomes)
          .containsExactly(
              new TypedOutcome.Failure<>("producer said no"),
              new TypedOutcome.Expired<>(ExpiryKind.RETRY_DISALLOWED, "deadline passed"));
    }
  }

  @Nested
  class Reaping {
    @Test
    void retried_with_explicit_timeout_extends_by_that_timeout() {
      var computation = expired("d".getBytes(UTF_8), 1);
      when(repository.findExpired(KIND, NOW, 10)).thenReturn(List.of(computation));

      Retry<String> retry =
          Retry.of(r -> r.timeout(Duration.ofSeconds(90)).handler((dispatch, ctx) -> {}));
      assertThat(retryable().reapExpiredComputations(BatchSize.of(10), retry)).isEqualTo(1);

      verify(repository).extendDeadline(computation.id(), NOW.plus(Duration.ofSeconds(90)), 2);
    }

    @Test
    void retried_default_extends_by_the_client_deadline() {
      var computation = expired("d".getBytes(UTF_8), 2);
      when(repository.findExpired(KIND, NOW, 10)).thenReturn(List.of(computation));

      Retry<String> retry = Retry.of(r -> r.handler((dispatch, ctx) -> {}));
      retryable().reapExpiredComputations(BatchSize.of(10), retry);

      verify(repository).extendDeadline(computation.id(), NOW.plus(Duration.ofMinutes(5)), 3);
    }

    @Test
    void not_retried_expires_with_retry_exhausted_and_the_reason() {
      var computation = expired("d".getBytes(UTF_8), 3);
      when(repository.findExpired(KIND, NOW, 10)).thenReturn(List.of(computation));

      retryable()
          .reapExpiredComputations(
              BatchSize.of(10), (dispatch, ctx) -> Retry.RetryResult.notRetried("circuit open"));

      verify(repository)
          .complete(
              computation.id(), Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "circuit open"), NOW);
    }

    @Test
    void payload_less_computation_reached_by_retryable_reap_expires_as_disallowed() {
      var computation = expired(null, 1);
      when(repository.findExpired(KIND, NOW, 10)).thenReturn(List.of(computation));

      Retry<String> retry = Retry.of(r -> r.handler((dispatch, ctx) -> {}));
      assertThat(retryable().reapExpiredComputations(BatchSize.of(10), retry)).isEqualTo(1);

      verify(repository)
          .complete(
              computation.id(),
              Outcome.expired(
                  ExpiryKind.RETRY_DISALLOWED, "deadline " + computation.deadline() + " passed"),
              NOW);
    }

    @Test
    void a_throwing_retry_leaves_the_computation_untouched() {
      var computation = expired("d".getBytes(UTF_8), 1);
      when(repository.findExpired(KIND, NOW, 10)).thenReturn(List.of(computation));

      int reaped =
          retryable()
              .reapExpiredComputations(
                  BatchSize.of(10),
                  (dispatch, ctx) -> {
                    throw new IllegalStateException("dispatch transport down");
                  });

      assertThat(reaped).isZero();
      verify(repository, never()).extendDeadline(any(), any(), anyInt());
      verify(repository, never()).complete(any(), any(), any());
    }

    @Test
    void reap_requires_a_retry() {
      assertThatNullPointerException()
          .isThrownBy(() -> retryable().reapExpiredComputations(BatchSize.of(10), null));
    }
  }

  @Nested
  class Purging {
    @Test
    void purge_translates_ttl_to_an_absolute_cutoff() {
      when(repository.purgeResults(any(), any(), anyInt())).thenReturn(4);
      assertThat(retryable().purgeExpiredResults(BatchSize.of(50), ResultTtl.ofHours(2)))
          .isEqualTo(4);
      verify(repository).purgeResults(KIND, NOW.minus(Duration.ofHours(2)), 50);
    }
  }

  @Nested
  class Creating_with_overrides {
    @Test
    void per_call_deadline_override_wins_over_the_client_default() {
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
      var computation = retryable().create("c", "d", Duration.ofSeconds(30));
      assertThat(computation.deadline()).isEqualTo(NOW.plus(Duration.ofSeconds(30)));
    }

    @Test
    void retryable_create_requires_a_dispatch() {
      assertThatNullPointerException().isThrownBy(() -> retryable().create("c", null));
    }
  }

  @Nested
  class Retryable_config_validation {
    @Test
    void retryable_kind_accessor_reports_the_kind() {
      assertThat(retryable().kind()).isEqualTo(KIND);
    }

    @Test
    void register_requires_a_continuation() {
      assertThatNullPointerException()
          .isThrownBy(() -> retryable().register(ComputationId.random(), null));
    }

    @Test
    void resolved_registration_decodes_expired_outcomes() {
      var id = ComputationId.random();
      when(continuum.registerContinuation(any(), any()))
          .thenReturn(
              new RegistrationResult.Resolved(
                  Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "deadline passed")));
      assertThat(retryable().register(id, "late"))
          .isEqualTo(
              new TypedRegistration.Resolved<>(
                  new TypedOutcome.Expired<>(ExpiryKind.RETRY_DISALLOWED, "deadline passed")));
    }

    @Test
    void retryable_deadline_is_required() {
      assertThatIllegalStateException()
          .isThrownBy(
              () ->
                  continuum.client(
                      "k",
                      String.class,
                      String.class,
                      String.class,
                      cfg ->
                          cfg.resultCodec(ClientMintingTest.STRINGS)
                              .continuationCodec(ClientMintingTest.STRINGS)
                              .dispatchCodec(ClientMintingTest.STRINGS)));
    }

    @Test
    void retryable_unresolvable_dispatch_codec_fails_at_mint_time() {
      assertThatIllegalStateException()
          .isThrownBy(
              () ->
                  continuum.client(
                      "k",
                      String.class,
                      String.class,
                      String.class,
                      cfg ->
                          cfg.resultCodec(ClientMintingTest.STRINGS)
                              .continuationCodec(ClientMintingTest.STRINGS)
                              .deadline(Duration.ofMinutes(1))));
    }

    @Test
    void retryable_codecs_resolve_through_the_factory() {
      CodecFactory factory =
          new CodecFactory() {
            @Override
            public <T> Codec<T> create(TypeRef<T> typeRef) {
              return new Codec<>() {
                @Override
                public byte[] encode(T value) {
                  return value.toString().getBytes(UTF_8);
                }

                @Override
                public T decode(byte[] bytes) {
                  throw new UnsupportedOperationException("decode is not exercised by this test");
                }
              };
            }
          };
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
              "factory-kind",
              String.class,
              String.class,
              String.class,
              cfg -> cfg.codecs(factory).deadline(Duration.ofMinutes(1)));
      var computation = client.create("continuation", "dispatch");
      assertThat(computation.dispatchPayload()).isEqualTo("dispatch".getBytes(UTF_8));
    }
  }

  @Nested
  class Codec_factory_resolution {
    @Test
    void unset_codecs_resolve_through_the_factory() {
      CodecFactory factory =
          new CodecFactory() {
            @Override
            public <T> Codec<T> create(TypeRef<T> typeRef) {
              assertThat(typeRef.getType()).isEqualTo(String.class);
              return new Codec<>() {
                @Override
                public byte[] encode(T value) {
                  return value.toString().getBytes(UTF_8);
                }

                @Override
                public T decode(byte[] bytes) {
                  throw new UnsupportedOperationException("decode is not exercised by this test");
                }
              };
            }
          };
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
              "factory-kind",
              String.class,
              String.class,
              cfg -> cfg.codecs(factory).deadline(Duration.ofMinutes(1)));

      var computation = client.create("via-factory");
      verify(continuum).create(any());
      assertThat(computation.kind()).isEqualTo(new ComputationKind("factory-kind"));
    }
  }

  @Nested
  class One_shot_shape {
    @Test
    void one_shot_reap_expires_every_overdue_computation() {
      var first = expired(null, 1);
      var second = expired(null, 1);
      when(repository.findExpired(any(), any(), anyInt())).thenReturn(List.of(first, second));

      var client =
          continuum.client(
              "tool",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(ClientMintingTest.STRINGS)
                      .continuationCodec(ClientMintingTest.STRINGS)
                      .deadline(Duration.ofMinutes(5)));

      assertThat(client.reapExpiredComputations(BatchSize.of(10))).isEqualTo(2);
      verify(repository)
          .complete(
              first.id(),
              Outcome.expired(
                  ExpiryKind.RETRY_DISALLOWED, "deadline " + first.deadline() + " passed"),
              NOW);
      verify(repository)
          .complete(
              second.id(),
              Outcome.expired(
                  ExpiryKind.RETRY_DISALLOWED, "deadline " + second.deadline() + " passed"),
              NOW);
    }

    @Test
    void one_shot_register_surfaces_the_registered_arm() {
      var client =
          continuum.client(
              "tool",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(ClientMintingTest.STRINGS)
                      .continuationCodec(ClientMintingTest.STRINGS)
                      .deadline(Duration.ofMinutes(5)));
      var continuationId = ContinuationId.random();
      when(continuum.registerContinuation(any(), any()))
          .thenReturn(new RegistrationResult.Registered(continuationId));

      assertThat(client.register(ComputationId.random(), "late"))
          .isEqualTo(new TypedRegistration.Registered<>(continuationId));
    }

    @Test
    void one_shot_create_honors_a_deadline_override_and_complete_encodes_success() {
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
      when(continuum.complete(any(), any())).thenReturn(CompletionResult.COMPLETED);
      var client =
          continuum.client(
              "tool",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(ClientMintingTest.STRINGS)
                      .continuationCodec(ClientMintingTest.STRINGS)
                      .deadline(Duration.ofMinutes(5)));

      var computation = client.create("c", Duration.ofSeconds(15));
      assertThat(computation.deadline()).isEqualTo(NOW.plus(Duration.ofSeconds(15)));

      assertThat(client.complete(computation.id(), "done")).isEqualTo(CompletionResult.COMPLETED);
      verify(continuum).complete(computation.id(), Outcome.success("done".getBytes(UTF_8)));

      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any())).thenReturn(List.of());
      assertThat(client.deliverResults(BatchSize.of(5), (c, o) -> {})).isZero();
      assertThat(client.kind()).isEqualTo(KIND);
    }

    @Test
    void one_shot_explicit_lease_and_purge_overloads_work() {
      when(repository.claimDeliveries(any(), any(), anyInt(), any(), any())).thenReturn(List.of());
      when(repository.purgeResults(any(), any(), anyInt())).thenReturn(2);
      var client =
          continuum.client(
              "tool",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(ClientMintingTest.STRINGS)
                      .continuationCodec(ClientMintingTest.STRINGS)
                      .deadline(Duration.ofMinutes(5)));

      assertThat(
              client.deliverResults(
                  BatchSize.of(3), Lease.ofSeconds(45), Backoff.ofSeconds(5), (c, o) -> {}))
          .isZero();
      verify(repository)
          .claimDeliveries(any(), eq(KIND), eq(3), eq(Duration.ofSeconds(45)), eq(NOW));
      assertThat(client.purgeExpiredResults(BatchSize.of(9), ResultTtl.ofMinutes(30))).isEqualTo(2);
      verify(repository).purgeResults(KIND, NOW.minus(Duration.ofMinutes(30)), 9);
    }

    @Test
    void one_shot_fail_reports_a_producer_failure() {
      var client =
          continuum.client(
              "tool",
              String.class,
              String.class,
              cfg ->
                  cfg.resultCodec(ClientMintingTest.STRINGS)
                      .continuationCodec(ClientMintingTest.STRINGS)
                      .deadline(Duration.ofMinutes(5)));
      var id = ComputationId.random();
      when(continuum.complete(any(), any())).thenReturn(CompletionResult.COMPLETED);

      assertThat(client.fail(id, "nope")).isEqualTo(CompletionResult.COMPLETED);
      verify(continuum).complete(id, Outcome.failure("nope"));
    }
  }

  @Nested
  class Repository_types {
    @Test
    void claimed_delivery_requires_its_parts() {
      var delivery =
          new CompletionDelivery(
              ComputationId.random(),
              KIND,
              ContinuationId.random(),
              "c".getBytes(UTF_8),
              Outcome.failure("f"));
      assertThatNullPointerException().isThrownBy(() -> new ClaimedDelivery(null, delivery, 0));
      assertThatNullPointerException()
          .isThrownBy(() -> new ClaimedDelivery(DeliveryId.random(), null, 0));
    }

    @Test
    void stored_continuation_value_semantics() {
      var id = ContinuationId.random();
      var a = new StoredContinuation(id, "p".getBytes(UTF_8));
      var b = new StoredContinuation(id, "p".getBytes(UTF_8));
      assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
      assertThat(a.toString()).contains(id.value().toString());
    }
  }
}
