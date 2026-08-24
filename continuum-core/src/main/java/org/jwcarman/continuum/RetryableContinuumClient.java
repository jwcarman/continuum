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
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.api.Backoff;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ExpiryContext;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Lease;
import org.jwcarman.continuum.api.ResultTtl;
import org.jwcarman.continuum.api.TypedDelivery;
import org.jwcarman.continuum.api.TypedRegistration;
import org.jwcarman.continuum.retry.Retry;
import org.jwcarman.continuum.retry.Retry.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The typed client for a <em>retryable</em> kind: every computation carries a dispatch payload (the
 * write-once "how to restart this work" breadcrumb — embed your idempotency key in it), and {@link
 * #retryExpiredComputations(BatchSize, Retry)} consults a {@link Retry} for overdue computations.
 * Minted by {@code continuum.client(kind, resultType, continuationType, dispatchType, customizer)}.
 *
 * @param <R> the result type
 * @param <C> the continuation type ("what receives the result")
 * @param <D> the dispatch type ("how to restart the work")
 */
public final class RetryableContinuumClient<R, C, D> {

  private static final Logger log = LoggerFactory.getLogger(RetryableContinuumClient.class);

  private final ClientSupport<R, C> support;
  private final Codec<D> dispatchCodec;

  RetryableContinuumClient(ClientSupport<R, C> support, Codec<D> dispatchCodec) {
    this.support = support;
    this.dispatchCodec = dispatchCodec;
  }

  /**
   * Atomically creates a pending computation (deadline = now + the configured deadline) with the
   * given continuation and dispatch breadcrumb. The dispatch payload is written once and handed
   * back verbatim on every retry — embed your external idempotency key in it.
   *
   * @param continuation what should receive the outcome
   * @param dispatch how to (re)start the work
   * @return the pending computation (attempt 1)
   */
  public Computation create(C continuation, D dispatch) {
    return create(continuation, dispatch, null);
  }

  /**
   * As {@link #create(Object, Object)} with a per-call deadline.
   *
   * @param continuation what should receive the outcome
   * @param dispatch how to (re)start the work
   * @param deadlineOverride the per-attempt timeout for this computation
   * @return the pending computation (attempt 1)
   */
  public Computation create(C continuation, D dispatch, Duration deadlineOverride) {
    Objects.requireNonNull(dispatch, "dispatch must not be null");
    return support.create(continuation, dispatchCodec.encode(dispatch), deadlineOverride);
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
   * decodes each, and invokes the consumer.
   *
   * <p><strong>Returning acknowledges the delivery; throwing releases it.</strong> That is the
   * whole contract, and both directions are load-bearing:
   *
   * <ul>
   *   <li><strong>Return normally</strong> and the delivery is acknowledged — deleted, never
   *       redelivered. This is how you <em>consume without acting</em>: a consumer that decides a
   *       delivery is stale or irrelevant returns, and it stops coming back.
   *   <li><strong>Throw any {@link RuntimeException}</strong> and the delivery is released with the
   *       call-site backoff and an incremented {@link TypedDelivery#deliveryAttempt()}, to be
   *       redelivered later. One delivery's failure never blocks the others in the batch.
   * </ul>
   *
   * <p>Nothing caps redelivery, so a consumer that always throws is retried forever. Use {@link
   * TypedDelivery#deliveryAttempt()} to dead-letter and return, rather than throwing indefinitely.
   *
   * <p>Delivery is at-least-once: deduplicate on {@link TypedDelivery#continuationId()}, which is
   * stable across redeliveries.
   *
   * <p>The consumer receives the whole {@link TypedDelivery}, not just the continuation and
   * outcome: identities to correlate a log line against — or to check a delivery against your own
   * state before acting — {@link TypedDelivery#elapsedTime()} to record end-to-end duration as the
   * outcome arrives, and {@link TypedDelivery#deliveryAttempt()} for a give-up policy.
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
   * Sweeps up to a batch of this kind's overdue computations, consulting the retry for each:
   * redispatched computations get an extended deadline and an incremented attempt count; {@code
   * notRetried} terminalizes as {@code Expired(RETRY_EXHAUSTED, reason)} through the normal
   * delivery path; a throwing retry leaves the computation untouched for the next pump. Duplicate
   * redispatch requests across concurrent pumps are possible — the idempotency key in the dispatch
   * payload makes them harmless.
   *
   * @param batchSize the maximum expired computations to process
   * @param retry performs (or schedules) the redispatch and reports what it did
   * @return the number processed
   */
  public int retryExpiredComputations(BatchSize batchSize, Retry<D> retry) {
    Objects.requireNonNull(retry, "retry must not be null");
    int reaped = 0;
    Instant observedAt = support.now();
    for (Computation computation : support.findExpired(batchSize)) {
      if (computation.dispatchPayload() == null) {
        support.failExpired(
            computation,
            ExpiryKind.RETRY_DISALLOWED,
            "expired after "
                + ClientSupport.describeElapsed(
                    Duration.between(computation.submittedAt(), observedAt)));
        reaped++;
        continue;
      }
      try {
        RetryResult result =
            retry.onTimeout(
                dispatchCodec.decode(computation.dispatchPayload()),
                new ExpiryContext(
                    computation.id(),
                    computation.kind(),
                    computation.attemptCount(),
                    computation.submittedAt(),
                    computation.deadline(),
                    observedAt));
        switch (result) {
          case RetryResult.Retried(Duration timeout) ->
              support
                  .continuum()
                  .repository()
                  .extendDeadline(
                      computation.id(),
                      support.now().plus(timeout),
                      computation.attemptCount() + 1);
          case RetryResult.RetriedDefault() ->
              support
                  .continuum()
                  .repository()
                  .extendDeadline(
                      computation.id(),
                      support.now().plus(support.deadline()),
                      computation.attemptCount() + 1);
          case RetryResult.NotRetried(String reason) ->
              support.failExpired(computation, ExpiryKind.RETRY_EXHAUSTED, reason);
        }
        reaped++;
      } catch (RuntimeException e) {
        log.warn(
            "retry of computation {} failed; leaving pending for the next reap",
            computation.id().value(),
            e);
      }
    }
    return reaped;
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
