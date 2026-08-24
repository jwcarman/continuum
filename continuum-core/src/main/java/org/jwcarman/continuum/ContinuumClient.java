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
import java.util.function.BiConsumer;
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

  private final ClientSupport<R, C> support;

  ContinuumClient(ClientSupport<R, C> support) {
    this.support = support;
  }

  public Computation create(C continuation) {
    return support.create(continuation, null, null);
  }

  public Computation create(C continuation, Duration deadlineOverride) {
    return support.create(continuation, null, deadlineOverride);
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

  public int failExpiredComputations(BatchSize batchSize) {
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      support.failExpired(
          computation,
          ExpiryKind.RETRY_DISALLOWED,
          "deadline " + computation.deadline() + " passed");
      reaped++;
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
