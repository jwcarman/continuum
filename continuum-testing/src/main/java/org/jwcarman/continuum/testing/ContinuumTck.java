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
package org.jwcarman.continuum.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.ContinuumClient;
import org.jwcarman.continuum.DefaultContinuum;
import org.jwcarman.continuum.RetryableContinuumClient;
import org.jwcarman.continuum.api.Backoff;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationNotFoundException;
import org.jwcarman.continuum.api.ComputationRequest;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.Expiry.ExpiryResult;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Lease;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.api.RegistrationResult;
import org.jwcarman.continuum.api.ResultTtl;
import org.jwcarman.continuum.api.TypedOutcome;
import org.jwcarman.continuum.retry.Retry;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumRepository;

/**
 * The provider certification battery. Extend, supply {@link #createRepository()}, and inherit
 * lifecycle semantics, the registration-vs-completion and complete-vs-complete races, competing
 * consumers, lease expiry, late registration, expiry outcomes, and purge behavior.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class ContinuumTck {

  /**
   * The kind every computation in the battery is filed under — providers that partition storage by
   * kind see all TCK traffic on this one.
   */
  protected static final ComputationKind KIND = new ComputationKind("tck");

  /** The lease the battery takes when claiming deliveries; expiry tests advance past it. */
  protected static final Duration LEASE = Duration.ofSeconds(30);

  /** The clock the battery drives; advance it rather than sleeping to reach a deadline. */
  protected MutableInstantSource instants;

  /** The provider's repository under certification, rebuilt before each test. */
  protected ContinuumRepository repository;

  /** A {@link DefaultContinuum} over {@link #repository} and {@link #instants}. */
  protected Continuum continuum;

  /**
   * Subclasses are instantiated by JUnit, once per test method; per-test state belongs in {@link
   * #createRepository()} or an {@code @BeforeEach}, not here.
   */
  protected ContinuumTck() {}

  /**
   * Builds the repository to certify. Called before every test, and must hand back storage with no
   * computations, continuations, or deliveries left over from a prior test.
   *
   * @return a fresh, empty repository under certification
   */
  protected abstract ContinuumRepository createRepository();

  /** Rebuilds the clock, the repository, and the {@link Continuum} facade before each test. */
  @BeforeEach
  protected void setUpTck() {
    instants = new MutableInstantSource(Instant.parse("2026-01-01T00:00:00Z"));
    repository = createRepository();
    continuum = new DefaultContinuum(repository, instants);
  }

  /**
   * A request of {@link #KIND} carrying {@code "cont"} as its initial continuation and a deadline
   * five minutes past the current instant — advance {@link #instants} past that to make it expire.
   *
   * @param dispatchPayload the payload a retry pump would re-dispatch, or {@code null} for none
   * @return the request
   */
  protected ComputationRequest request(byte[] dispatchPayload) {
    return new ComputationRequest(
        KIND,
        "cont".getBytes(UTF_8),
        instants.instant().plus(Duration.ofMinutes(5)),
        dispatchPayload);
  }

  /**
   * Claims every ready delivery of {@link #KIND} for one worker under {@link #LEASE}. The claim is
   * not acknowledged, so a second worker sees nothing until the lease lapses.
   *
   * @param workerId the claiming worker's identity
   * @return the deliveries now leased to that worker
   */
  protected List<ClaimedDelivery> claimAll(String workerId) {
    return repository.claimDeliveries(workerId, KIND, 100, LEASE, instants.instant());
  }

  /**
   * Runs both tasks as concurrently as a latch can make them; rethrows any task failure. Neither
   * argument is privileged — which one wins a race is exactly what the race tests probe.
   *
   * @param first one contender
   * @param second the other contender
   */
  protected static void concurrently(Runnable first, Runnable second) {
    CountDownLatch start = new CountDownLatch(1);
    Callable<Void> a =
        () -> {
          start.await();
          first.run();
          return null;
        };
    Callable<Void> b =
        () -> {
          start.await();
          second.run();
          return null;
        };
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      Future<Void> firstFuture = pool.submit(a);
      Future<Void> secondFuture = pool.submit(b);
      start.countDown();
      firstFuture.get();
      secondFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  @Nested
  class Lifecycle {
    @Test
    void create_then_find_reports_pending() {
      var computation = continuum.create(request(null));
      var found = continuum.find(computation.id()).orElseThrow();
      assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(found.attemptCount()).isEqualTo(1);
    }

    @Test
    void completion_delivers_to_the_initial_continuation() {
      var computation = continuum.create(request(null));
      var outcome = Outcome.success("r".getBytes(UTF_8));
      assertThat(continuum.complete(computation.id(), outcome))
          .isEqualTo(CompletionResult.COMPLETED);

      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(1);
      assertThat(claimed.getFirst().delivery().computationId()).isEqualTo(computation.id());
      assertThat(claimed.getFirst().delivery().continuationPayload())
          .isEqualTo("cont".getBytes(UTF_8));
      assertThat(claimed.getFirst().delivery().outcome()).isEqualTo(outcome);

      repository.acknowledgeDelivery(claimed.getFirst().id());
      assertThat(claimAll("w1")).isEmpty();
    }

    @Test
    void duplicate_completion_is_already_resolved_and_outcome_is_immutable() {
      var computation = continuum.create(request(null));
      var winner = Outcome.success("first".getBytes(UTF_8));
      continuum.complete(computation.id(), winner);
      assertThat(continuum.complete(computation.id(), Outcome.failure("late")))
          .isEqualTo(CompletionResult.ALREADY_RESOLVED);
      assertThat(continuum.find(computation.id()).orElseThrow().outcome()).isEqualTo(winner);
    }

    @Test
    void completing_an_unknown_computation_is_not_found() {
      assertThat(continuum.complete(ComputationId.random(), Outcome.failure("x")))
          .isEqualTo(CompletionResult.NOT_FOUND);
    }

    @Test
    void registering_against_an_unknown_computation_throws() {
      var id = ComputationId.random();
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(id, "x".getBytes(UTF_8)));
    }
  }

  @Nested
  class Registration {
    @Test
    void continuation_registered_before_completion_receives_its_own_delivery() {
      var computation = continuum.create(request(null));
      var registration = continuum.registerContinuation(computation.id(), "second".getBytes(UTF_8));
      assertThat(registration).isInstanceOf(RegistrationResult.Registered.class);

      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(2);
      assertThat(claimed.stream().map(c -> c.delivery().continuationId()).distinct()).hasSize(2);
    }

    @Test
    void late_registration_returns_the_memoized_outcome_and_persists_nothing() {
      var computation = continuum.create(request(null));
      var outcome = Outcome.success("r".getBytes(UTF_8));
      continuum.complete(computation.id(), outcome);

      var registration = continuum.registerContinuation(computation.id(), "late".getBytes(UTF_8));
      assertThat(registration).isEqualTo(new RegistrationResult.Resolved(outcome));
      assertThat(claimAll("w1")).hasSize(1); // only the initial continuation's delivery
    }

    @Test
    void register_vs_complete_race_yields_exactly_one_of_registered_or_resolved() {
      for (int i = 0; i < 50; i++) {
        var computation = continuum.create(request(null));
        var outcome = Outcome.success("r".getBytes(UTF_8));
        var registrations = new ArrayList<RegistrationResult>();

        concurrently(
            () ->
                registrations.add(
                    continuum.registerContinuation(computation.id(), "b".getBytes(UTF_8))),
            () -> continuum.complete(computation.id(), outcome));

        var claimed = claimAll("w1");
        switch (registrations.getFirst()) {
          case RegistrationResult.Registered(var continuationId) ->
              assertThat(claimed.stream().map(c -> c.delivery().continuationId()))
                  .contains(continuationId);
          case RegistrationResult.Resolved(var resolved) -> {
            assertThat(resolved).isEqualTo(outcome);
            assertThat(claimed).hasSize(1);
          }
        }
        claimed.forEach(c -> repository.acknowledgeDelivery(c.id()));
      }
    }

    @Test
    void concurrent_registrations_each_produce_exactly_one_delivery() {
      var computation = continuum.create(request(null));
      int extras = 8;
      CountDownLatch start = new CountDownLatch(1);
      try (ExecutorService pool = Executors.newFixedThreadPool(extras)) {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < extras; i++) {
          byte[] payload = ("extra-" + i).getBytes(UTF_8);
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    continuum.registerContinuation(computation.id(), payload);
                    return null;
                  }));
        }
        start.countDown();
        for (Future<?> future : futures) {
          future.get();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      }
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(extras + 1);
      assertThat(claimed.stream().map(c -> c.delivery().continuationId()).distinct())
          .hasSize(extras + 1);
    }
  }

  @Nested
  class Racing {
    @Test
    void complete_vs_complete_has_exactly_one_winner_whose_outcome_is_stored() {
      for (int i = 0; i < 50; i++) {
        var computation = continuum.create(request(null));
        var success = Outcome.success("s".getBytes(UTF_8));
        var failure = Outcome.failure("f");
        var resultA = new ArrayList<CompletionResult>();
        var resultB = new ArrayList<CompletionResult>();

        concurrently(
            () -> resultA.add(continuum.complete(computation.id(), success)),
            () -> resultB.add(continuum.complete(computation.id(), failure)));

        var results = List.of(resultA.getFirst(), resultB.getFirst());
        assertThat(results)
            .containsExactlyInAnyOrder(
                CompletionResult.COMPLETED, CompletionResult.ALREADY_RESOLVED);
        var stored = continuum.find(computation.id()).orElseThrow().outcome();
        if (resultA.getFirst() == CompletionResult.COMPLETED) {
          assertThat(stored).isEqualTo(success);
        } else {
          assertThat(stored).isEqualTo(failure);
        }
        claimAll("w1").forEach(c -> repository.acknowledgeDelivery(c.id()));
      }
    }

    @Test
    void expiry_vs_completion_has_exactly_one_winner() {
      var computation = continuum.create(request("d".getBytes(UTF_8)));
      instants.advance(Duration.ofMinutes(6));
      var success = Outcome.success("s".getBytes(UTF_8));
      var expired = Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (1 of 1)");

      concurrently(
          () -> repository.complete(computation.id(), success, instants.instant()),
          () -> repository.complete(computation.id(), expired, instants.instant()));

      var stored = continuum.find(computation.id()).orElseThrow();
      assertThat(stored.status()).isIn(ComputationStatus.COMPLETED, ComputationStatus.EXPIRED);
      assertThat(stored.outcome()).isIn(success, expired);
    }
  }

  @Nested
  class Claiming {
    @Test
    void deliveries_carry_the_computation_submission_and_completion_times() {
      var submittedAt = instants.instant();
      var computation = continuum.create(request(null));
      instants.advance(Duration.ofMinutes(3));
      var completedAt = instants.instant();
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));

      var delivery = claimAll("w1").getFirst().delivery();

      assertThat(delivery.submittedAt()).isEqualTo(submittedAt);
      assertThat(delivery.completedAt()).isEqualTo(completedAt);
      assertThat(delivery.elapsedTime()).isEqualTo(Duration.ofMinutes(3));
    }

    @Test
    void competing_consumers_claim_disjoint_deliveries() {
      continuum.complete(
          continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      var claimedByA = new ArrayList<ClaimedDelivery>();
      var claimedByB = new ArrayList<ClaimedDelivery>();

      concurrently(
          () -> claimedByA.addAll(claimAll("wA")), () -> claimedByB.addAll(claimAll("wB")));

      assertThat(claimedByA.size() + claimedByB.size()).isEqualTo(1);
    }

    @Test
    void leased_deliveries_are_reclaimable_after_lease_expiry() {
      continuum.complete(
          continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      assertThat(claimAll("wA")).hasSize(1);
      assertThat(claimAll("wB")).isEmpty(); // still leased
      instants.advance(LEASE.plusSeconds(1));
      assertThat(claimAll("wB")).hasSize(1); // lease lapsed, reclaimed
    }

    @Test
    void released_deliveries_return_after_the_backoff_with_incremented_attempts() {
      continuum.complete(
          continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("wA");
      repository.releaseDelivery(
          claimed.getFirst().id(), instants.instant().plus(Duration.ofSeconds(10)));

      assertThat(claimAll("wA")).isEmpty(); // still backing off
      instants.advance(Duration.ofSeconds(11));
      var reclaimed = claimAll("wA");
      assertThat(reclaimed).hasSize(1);
      assertThat(reclaimed.getFirst().deliveryAttempt()).isEqualTo(1);
    }
  }

  @Nested
  class Expiry {
    @Test
    void find_expired_excludes_future_deadlines_and_terminal_computations() {
      var expiring = continuum.create(request(null));
      instants.advance(Duration.ofMinutes(1));
      var young = continuum.create(request(null)); // deadline 5m from the LATER now
      instants.advance(
          Duration.ofMinutes(4)); // expiring's deadline (<= now) passed; young's has not

      var expired = repository.findExpired(KIND, instants.instant(), 10);
      assertThat(expired).extracting(Computation::id).containsExactly(expiring.id());

      continuum.complete(expiring.id(), Outcome.failure("f"));
      assertThat(repository.findExpired(KIND, instants.instant(), 10)).isEmpty();
      assertThat(young.id()).isNotNull();
    }

    @Test
    void extend_deadline_defers_expiry_and_records_the_attempt() {
      var computation = continuum.create(request("d".getBytes(UTF_8)));
      instants.advance(Duration.ofMinutes(6));
      repository.extendDeadline(
          computation.id(), instants.instant().plus(Duration.ofMinutes(5)), 2);

      assertThat(repository.findExpired(KIND, instants.instant(), 10)).isEmpty();
      assertThat(continuum.find(computation.id()).orElseThrow().attemptCount()).isEqualTo(2);
    }
  }

  @Nested
  class Purging {
    @Test
    void purge_removes_only_results_older_than_the_cutoff() {
      var old = continuum.create(request(null));
      continuum.complete(old.id(), Outcome.success("r".getBytes(UTF_8)));
      instants.advance(Duration.ofHours(2));
      var recent = continuum.create(request(null));
      continuum.complete(recent.id(), Outcome.success("r".getBytes(UTF_8)));

      int purged =
          repository.purgeResults(KIND, instants.instant().minus(Duration.ofHours(1)), 100);
      assertThat(purged).isEqualTo(1);
      assertThat(continuum.find(old.id())).isEmpty();
      assertThat(continuum.find(recent.id())).isPresent();
    }

    @Test
    void purged_computations_behave_as_never_known() {
      var computation = continuum.create(request(null));
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      instants.advance(Duration.ofHours(2));
      repository.purgeResults(KIND, instants.instant(), 100);

      assertThat(repository.complete(computation.id(), Outcome.failure("late"), instants.instant()))
          .isEqualTo(CompletionOutcome.NOT_FOUND);
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(computation.id(), "x".getBytes(UTF_8)));
    }
  }

  @Nested
  class Typed_clients {

    final Codec<String> strings =
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

    private RetryableContinuumClient<String, String, String> retryable() {
      return continuum.client(
          "typed-tool",
          String.class,
          String.class,
          String.class,
          cfg ->
              cfg.resultCodec(strings)
                  .continuationCodec(strings)
                  .dispatchCodec(strings)
                  .deadline(Duration.ofMinutes(5)));
    }

    private ContinuumClient<String, String> nonRetryable() {
      return continuum.client(
          "typed-approval",
          String.class,
          String.class,
          cfg ->
              cfg.resultCodec(strings).continuationCodec(strings).deadline(Duration.ofMinutes(5)));
    }

    @Test
    void create_complete_deliver_roundtrip_with_user_types() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      client.complete(computation.id(), "the-answer");

      var received = new CopyOnWriteArrayList<String>();
      int delivered =
          client.deliverResults(
              BatchSize.of(10),
              delivery -> {
                assertThat(delivery.continuation()).isEqualTo("route-me");
                assertThat(delivery.outcome()).isEqualTo(new TypedOutcome.Success<>("the-answer"));
                assertThat(delivery.deliveryAttempt()).isZero();
                received.add(delivery.continuation());
              });
      assertThat(delivered).isEqualTo(1);
      assertThat(received).hasSize(1);
      assertThat(client.deliverResults(BatchSize.of(10), d -> {})).isZero(); // acknowledged, gone
    }

    @Test
    void failing_consumer_releases_the_delivery_for_redelivery_after_backoff() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      client.complete(computation.id(), "the-answer");

      assertThat(
              client.deliverResults(
                  BatchSize.of(10),
                  Lease.ofSeconds(30),
                  Backoff.ofSeconds(10),
                  d -> {
                    throw new IllegalStateException("consumer crash");
                  }))
          .isZero();
      assertThat(client.deliverResults(BatchSize.of(10), d -> {})).isZero(); // backing off
      instants.advance(Duration.ofSeconds(11));
      assertThat(client.deliverResults(BatchSize.of(10), d -> {})).isEqualTo(1);
    }

    @Test
    void retry_pump_consults_the_retry_and_extends_the_deadline() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      instants.advance(Duration.ofMinutes(6));

      var redispatched = new AtomicReference<String>();
      int reaped =
          client.retryExpiredComputations(
              BatchSize.of(10),
              Retry.of(
                  r ->
                      r.atMost(3)
                          .handler(
                              (dispatch, ctx) -> {
                                assertThat(ctx.computationId()).isEqualTo(computation.id());
                                assertThat(ctx.attemptCount()).isEqualTo(1);
                                redispatched.set(dispatch);
                              })));

      assertThat(reaped).isEqualTo(1);
      assertThat(redispatched.get()).isEqualTo("dispatch-me");
      var found = continuum.find(computation.id()).orElseThrow();
      assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(found.attemptCount()).isEqualTo(2);
      assertThat(found.deadline()).isEqualTo(instants.instant().plus(Duration.ofMinutes(5)));
    }

    @Test
    void exhausted_retries_expire_the_computation_and_deliver_the_expiry() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      var retry = Retry.<String>of(r -> r.atMost(1).handler((dispatch, ctx) -> {}));

      instants.advance(Duration.ofMinutes(6));
      assertThat(client.retryExpiredComputations(BatchSize.of(10), retry)).isEqualTo(1);

      assertThat(continuum.find(computation.id()).orElseThrow().status())
          .isEqualTo(ComputationStatus.EXPIRED);
      var outcomes = new CopyOnWriteArrayList<TypedOutcome<String>>();
      client.deliverResults(BatchSize.of(10), d -> outcomes.add(d.outcome()));
      assertThat(outcomes)
          .containsExactly(
              new TypedOutcome.Expired<>(
                  ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (1 of 1)"));
    }

    @Test
    void fail_pump_always_expires_with_retry_disallowed() {
      var client = nonRetryable();
      var computation = client.create("route-me");
      instants.advance(Duration.ofMinutes(6));

      assertThat(client.failExpiredComputations(BatchSize.of(10))).isEqualTo(1);

      var found = continuum.find(computation.id()).orElseThrow();
      assertThat(found.status()).isEqualTo(ComputationStatus.EXPIRED);
      assertThat(found.outcome())
          .isEqualTo(Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "expired after 6 mins"));
    }

    @Test
    void fail_pump_policy_can_extend_the_wait_instead_of_ending_it() {
      var client = nonRetryable();
      var computation = client.create("route-me");
      instants.advance(Duration.ofMinutes(6));

      assertThat(
              client.failExpiredComputations(
                  BatchSize.of(10),
                  ctx ->
                      ctx.elapsedTime().compareTo(Duration.ofHours(1)) > 0
                          ? ExpiryResult.expired("waited long enough")
                          : ExpiryResult.extended(Duration.ofMinutes(30))))
          .isEqualTo(1);

      var stillPending = continuum.find(computation.id()).orElseThrow();
      assertThat(stillPending.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(stillPending.deadline())
          .isEqualTo(instants.instant().plus(Duration.ofMinutes(30)));
      assertThat(stillPending.attemptCount()).isEqualTo(1);

      instants.advance(Duration.ofHours(2));
      assertThat(
              client.failExpiredComputations(
                  BatchSize.of(10), ctx -> ExpiryResult.expired("waited long enough")))
          .isEqualTo(1);
      assertThat(continuum.find(computation.id()).orElseThrow().outcome())
          .isEqualTo(Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "waited long enough"));
    }

    @Test
    void purge_via_the_client_uses_call_site_ttl() {
      var client = nonRetryable();
      var computation = client.create("route-me");
      client.complete(computation.id(), "done");
      instants.advance(Duration.ofHours(2));

      assertThat(client.purgeExpiredResults(BatchSize.of(100), ResultTtl.ofHours(1))).isEqualTo(1);
      assertThat(continuum.find(computation.id())).isEmpty();
    }
  }
}
