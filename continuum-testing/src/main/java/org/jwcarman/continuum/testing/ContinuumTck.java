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
import org.jwcarman.continuum.CompletionResult;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationNotFoundException;
import org.jwcarman.continuum.ComputationRequest;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.ContinuumClient;
import org.jwcarman.continuum.DefaultContinuum;
import org.jwcarman.continuum.ExpiryKind;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.RegistrationResult;
import org.jwcarman.continuum.Retry;
import org.jwcarman.continuum.RetryableContinuumClient;
import org.jwcarman.continuum.TypedOutcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumRepository;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class ContinuumTck {

  protected static final ComputationKind KIND = new ComputationKind("tck");
  protected static final Duration LEASE = Duration.ofSeconds(30);

  protected MutableInstantSource instants;
  protected ContinuumRepository repository;
  protected Continuum continuum;

  protected abstract ContinuumRepository createRepository();

  @BeforeEach
  protected void setUpTck() {
    instants = new MutableInstantSource(Instant.parse("2026-01-01T00:00:00Z"));
    repository = createRepository();
    continuum = new DefaultContinuum(repository, instants);
  }

  protected ComputationRequest request(byte[] dispatchPayload) {
    return new ComputationRequest(
        KIND,
        "cont".getBytes(UTF_8),
        instants.instant().plus(Duration.ofMinutes(5)),
        dispatchPayload);
  }

  protected List<ClaimedDelivery> claimAll(String workerId) {
    return repository.claimDeliveries(workerId, KIND, 100, LEASE, instants.instant());
  }

  /** Runs both tasks as concurrently as a latch can make them; rethrows any task failure. */
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
      assertThat(reclaimed.getFirst().attemptCount()).isEqualTo(1);
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
                  .deadline(Duration.ofMinutes(5))
                  .backoff(Duration.ofSeconds(10)));
    }

    private ContinuumClient<String, String> oneShot() {
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
              10,
              (continuation, outcome) -> {
                assertThat(continuation).isEqualTo("route-me");
                assertThat(outcome).isEqualTo(new TypedOutcome.Success<>("the-answer"));
                received.add(continuation);
              });
      assertThat(delivered).isEqualTo(1);
      assertThat(received).hasSize(1);
      assertThat(client.deliverResults(10, (c, o) -> {})).isZero(); // acknowledged, gone
    }

    @Test
    void failing_consumer_releases_the_delivery_for_redelivery_after_backoff() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      client.complete(computation.id(), "the-answer");

      assertThat(
              client.deliverResults(
                  10,
                  (c, o) -> {
                    throw new IllegalStateException("consumer crash");
                  }))
          .isZero();
      assertThat(client.deliverResults(10, (c, o) -> {})).isZero(); // backing off
      instants.advance(Duration.ofSeconds(11));
      assertThat(client.deliverResults(10, (c, o) -> {})).isEqualTo(1);
    }

    @Test
    void reap_consults_the_retry_and_extends_the_deadline() {
      var client = retryable();
      var computation = client.create("route-me", "dispatch-me");
      instants.advance(Duration.ofMinutes(6));

      var redispatched = new AtomicReference<String>();
      int reaped =
          client.reapExpiredComputations(
              10,
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
      assertThat(client.reapExpiredComputations(10, retry)).isEqualTo(1);

      assertThat(continuum.find(computation.id()).orElseThrow().status())
          .isEqualTo(ComputationStatus.EXPIRED);
      var outcomes = new CopyOnWriteArrayList<TypedOutcome<String>>();
      client.deliverResults(10, (continuation, outcome) -> outcomes.add(outcome));
      assertThat(outcomes)
          .containsExactly(
              new TypedOutcome.Expired<>(
                  ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (1 of 1)"));
    }

    @Test
    void one_shot_reap_always_expires_with_retry_disallowed() {
      var client = oneShot();
      var computation = client.create("route-me");
      instants.advance(Duration.ofMinutes(6));

      assertThat(client.reapExpiredComputations(10)).isEqualTo(1);

      var found = continuum.find(computation.id()).orElseThrow();
      assertThat(found.status()).isEqualTo(ComputationStatus.EXPIRED);
      assertThat(found.outcome())
          .isEqualTo(
              Outcome.expired(
                  ExpiryKind.RETRY_DISALLOWED, "deadline " + found.deadline() + " passed"));
    }

    @Test
    void purge_via_the_client_uses_call_site_ttl() {
      var client = oneShot();
      var computation = client.create("route-me");
      client.complete(computation.id(), "done");
      instants.advance(Duration.ofHours(2));

      assertThat(client.purgeExpiredResults(100, Duration.ofHours(1))).isEqualTo(1);
      assertThat(continuum.find(computation.id())).isEmpty();
    }
  }
}
