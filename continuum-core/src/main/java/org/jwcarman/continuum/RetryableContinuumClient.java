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
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.api.Backoff;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Lease;
import org.jwcarman.continuum.api.ResultTtl;
import org.jwcarman.continuum.api.TypedOutcome;
import org.jwcarman.continuum.api.TypedRegistration;
import org.jwcarman.continuum.retry.Retry;
import org.jwcarman.continuum.retry.Retry.RetryResult;
import org.jwcarman.continuum.retry.RetryContext;
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

  public Computation create(C continuation, D dispatch) {
    return create(continuation, dispatch, null);
  }

  public Computation create(C continuation, D dispatch, Duration deadlineOverride) {
    Objects.requireNonNull(dispatch, "dispatch must not be null");
    return support.create(continuation, dispatchCodec.encode(dispatch), deadlineOverride);
  }

  public CompletionResult complete(ComputationId id, R result) {
    return support.complete(id, result);
  }

  public CompletionResult fail(ComputationId id, String message) {
    return support.fail(id, message);
  }

  public TypedRegistration<R> register(ComputationId id, C continuation) {
    return support.register(id, continuation);
  }

  public int deliverResults(BatchSize batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    return support.deliverResults(
        batchSize, ClientSupport.DEFAULT_LEASE, ClientSupport.DEFAULT_BACKOFF, consumer);
  }

  public int deliverResults(
      BatchSize batchSize, Lease lease, Backoff backoff, BiConsumer<C, TypedOutcome<R>> consumer) {
    return support.deliverResults(batchSize, lease, backoff, consumer);
  }

  public int retryExpiredComputations(BatchSize batchSize, Retry<D> retry) {
    Objects.requireNonNull(retry, "retry must not be null");
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      if (computation.dispatchPayload() == null) {
        support.failExpired(
            computation,
            ExpiryKind.RETRY_DISALLOWED,
            "deadline " + computation.deadline() + " passed");
        reaped++;
        continue;
      }
      try {
        RetryResult result =
            retry.onTimeout(
                dispatchCodec.decode(computation.dispatchPayload()),
                new RetryContext(
                    computation.id(),
                    computation.kind(),
                    computation.attemptCount(),
                    computation.deadline()));
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

  public int purgeExpiredResults(BatchSize batchSize, ResultTtl ttl) {
    return support.purgeExpiredResults(batchSize, ttl);
  }

  public ComputationKind kind() {
    return support.kind();
  }
}
