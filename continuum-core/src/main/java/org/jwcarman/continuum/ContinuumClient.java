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

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;
import org.jwcarman.continuum.api.Backoff;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.Expiry;
import org.jwcarman.continuum.api.Expiry.ExpiryResult;
import org.jwcarman.continuum.api.ExpiryContext;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Lease;
import org.jwcarman.continuum.api.ResultTtl;
import org.jwcarman.continuum.api.TypedDelivery;
import org.jwcarman.continuum.api.TypedRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The typed client for a <em>non-retryable</em> kind: computations carry no dispatch payload, so
 * nothing can ever be redispatched and {@link #failExpiredComputations(BatchSize)} expires overdue
 * computations unconditionally as {@code Expired(RETRY_DISALLOWED, ...)}. Minted by {@code
 * continuum.client(kind, resultType, continuationType, customizer)}.
 *
 * @param <R> the result type
 * @param <C> the continuation type ("what receives the result")
 */
public final class ContinuumClient<R, C> {

  private static final Logger log = LoggerFactory.getLogger(ContinuumClient.class);

  private final ClientSupport<R, C> support;

  ContinuumClient(ClientSupport<R, C> support) {
    this.support = support;
  }

  /**
   * Atomically creates a pending computation (deadline = now + the configured deadline) with the
   * given continuation. Non-retryable: no dispatch payload exists to redispatch.
   *
   * @param continuation what should receive the outcome
   * @return the pending computation
   */
  public Computation create(C continuation) {
    return support.create(continuation, null, null);
  }

  /**
   * As {@link #create(Object)} with a per-call deadline.
   *
   * @param continuation what should receive the outcome
   * @param deadlineOverride the per-attempt timeout for this computation
   * @return the pending computation
   */
  public Computation create(C continuation, Duration deadlineOverride) {
    return support.create(continuation, null, deadlineOverride);
  }

  /**
   * Reports the computation's successful result; first terminalization wins.
   *
   * @param id the computation to resolve
   * @param result the result value, encoded with this client's result codec
   * @return whether this call won, lost, or found nothing
   */
  public CompletionResult complete(ComputationId id, R result) {
    return support.complete(id, result);
  }

  /**
   * Reports that the work itself failed; first terminalization wins.
   *
   * @param id the computation to resolve
   * @param message the producer's words — diagnostic prose, never parsed
   * @return whether this call won, lost, or found nothing
   */
  public CompletionResult fail(ComputationId id, String message) {
    return support.fail(id, message);
  }

  /**
   * Registers interest, atomically against completion: durably registered, or the already-memoized
   * outcome decoded — never neither.
   *
   * @param id the computation to watch
   * @param continuation what should receive the outcome
   * @return the registration or the decoded outcome
   */
  public TypedRegistration<R> register(ComputationId id, C continuation) {
    return support.register(id, continuation);
  }

  /**
   * Claims up to a batch of this kind's outbox deliveries under the default 30-second lease,
   * decodes each, and invokes the consumer. Success acknowledges (deletes) the delivery; a consumer
   * exception releases it with the default 30-second backoff and an incremented delivery attempt —
   * one delivery's failure never blocks the others. At-least-once: deduplicate on {@link
   * TypedDelivery#continuationId()}.
   *
   * <p>The consumer receives the whole {@link TypedDelivery}, not just the continuation and
   * outcome: identities to correlate a log line against, {@link TypedDelivery#elapsedTime()} to
   * record end-to-end duration as the outcome arrives, and {@link TypedDelivery#deliveryAttempt()}
   * for a give-up or dead-letter policy.
   *
   * @param batchSize the maximum deliveries to process
   * @param consumer receives the decoded delivery
   * @return the number successfully delivered — the drain signal
   */
  public int deliverResults(BatchSize batchSize, Consumer<TypedDelivery<C, R>> consumer) {
    return support.deliverResults(
        batchSize, ClientSupport.DEFAULT_LEASE, ClientSupport.DEFAULT_BACKOFF, consumer);
  }

  /**
   * As {@link #deliverResults(BatchSize, Consumer)} with an explicit lease (must exceed the
   * worst-case consumer time) and failure backoff.
   *
   * @param batchSize the maximum deliveries to process
   * @param lease how long claimed deliveries stay invisible to other claimers
   * @param backoff how long a failed delivery waits before redelivery
   * @param consumer receives the decoded delivery
   * @return the number successfully delivered
   */
  public int deliverResults(
      BatchSize batchSize, Lease lease, Backoff backoff, Consumer<TypedDelivery<C, R>> consumer) {
    return support.deliverResults(batchSize, lease, backoff, consumer);
  }

  /**
   * Expires up to a batch of this kind's overdue computations, unconditionally, as {@code
   * Expired(RETRY_DISALLOWED, "expired after ...")} — through the normal delivery path. Nothing is
   * ever redispatched for a non-retryable kind.
   *
   * <p>Use {@link #failExpiredComputations(BatchSize, Expiry)} when the wait should be re-decided
   * at each lapse rather than ended.
   *
   * @param batchSize the maximum expired computations to process
   * @return the number expired
   */
  public int failExpiredComputations(BatchSize batchSize) {
    return failExpiredComputations(
        batchSize,
        context ->
            ExpiryResult.expired(
                "expired after " + ClientSupport.describeElapsed(context.elapsedTime())));
  }

  /**
   * Sweeps up to a batch of this kind's overdue computations, consulting the policy for each:
   * {@code extended} pushes the deadline out and the computation keeps waiting; {@code expired}
   * terminalizes as {@code Expired(RETRY_DISALLOWED, reason)} through the normal delivery path; a
   * throwing policy leaves the computation untouched for the next pump.
   *
   * <p>This is how a non-retryable kind waits open-endedly — a human approval that may sit for days
   * — without inventing a dispatch payload it must never use. Extending never increments the
   * attempt count: nothing was dispatched.
   *
   * <pre>{@code
   * approvals.failExpiredComputations(BatchSize.of(50), ctx ->
   *     ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
   *         ? ExpiryResult.expired("no response within 7 days")
   *         : ExpiryResult.extended());
   * }</pre>
   *
   * @param batchSize the maximum expired computations to process
   * @param expiry decides whether each lapsed computation keeps waiting or ends
   * @return the number processed
   */
  public int failExpiredComputations(BatchSize batchSize, Expiry expiry) {
    Objects.requireNonNull(expiry, "expiry must not be null");
    int reaped = 0;
    Instant observedAt = support.now();
    for (Computation computation : support.findExpired(batchSize)) {
      try {
        applyExpiry(computation, expiry.onTimeout(contextFor(computation, observedAt)));
        reaped++;
      } catch (RuntimeException e) {
        log.warn(
            "expiry policy for computation {} failed; leaving pending for the next reap",
            computation.id().value(),
            e);
      }
    }
    return reaped;
  }

  private ExpiryContext contextFor(Computation computation, Instant observedAt) {
    return new ExpiryContext(
        computation.id(),
        computation.kind(),
        computation.attemptCount(),
        computation.submittedAt(),
        computation.deadline(),
        observedAt);
  }

  private void applyExpiry(Computation computation, ExpiryResult result) {
    switch (result) {
      case ExpiryResult.Extended(Duration timeout) -> extend(computation, timeout);
      case ExpiryResult.ExtendedDefault() -> extend(computation, support.deadline());
      case ExpiryResult.Expired(String reason) ->
          support.failExpired(computation, ExpiryKind.RETRY_DISALLOWED, reason);
    }
  }

  private void extend(Computation computation, Duration timeout) {
    support
        .continuum()
        .repository()
        .extendDeadline(computation.id(), support.now().plus(timeout), computation.attemptCount());
  }

  /**
   * Deletes up to a batch of this kind's memoized results older than the call-site TTL. After
   * purge, a computation behaves as never known.
   *
   * @param batchSize the maximum result records to delete
   * @param ttl how long results outlive completion
   * @return the number purged
   */
  public int purgeExpiredResults(BatchSize batchSize, ResultTtl ttl) {
    return support.purgeExpiredResults(batchSize, ttl);
  }

  /**
   * The computation kind this client is bound to.
   *
   * @return the kind
   */
  public ComputationKind kind() {
    return support.kind();
  }
}
