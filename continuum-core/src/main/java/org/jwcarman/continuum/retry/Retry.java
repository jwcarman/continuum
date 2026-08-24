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
package org.jwcarman.continuum.retry;

import java.time.Duration;
import java.util.Objects;
import org.jwcarman.continuum.api.ExpiryContext;

/**
 * The policy consulted when a computation's deadline lapses: it decides whether the work is
 * redispatched and reports back what it did. Continuum never redispatches on its own — a {@code
 * Retry} performs the dispatch and the expiry pump interprets the {@link RetryResult}.
 *
 * @param <D> the dispatch type
 */
@FunctionalInterface
public interface Retry<D> {

  /**
   * Handles one expired computation: performs (or schedules) the redispatch itself — using the same
   * dispatch payload as every prior attempt — and reports what it did.
   *
   * @param dispatch the decoded write-once dispatch payload
   * @param context Continuum's durable facts about the expired computation
   * @return what happened: retried (with which timeout) or not
   */
  RetryResult onTimeout(D dispatch, ExpiryContext context);

  /**
   * The declarative front door: attempt limits and timeouts as config, a handler that only
   * dispatches, results derived mechanically.
   *
   * @param customizer fills in the config
   * @param <D> the dispatch type
   * @return the built retry
   */
  static <D> Retry<D> of(RetryCustomizer<D> customizer) {
    Objects.requireNonNull(customizer, "customizer must not be null");
    DefaultRetryConfig<D> config = new DefaultRetryConfig<>();
    customizer.customize(config);
    return config.buildRetry();
  }

  /**
   * What a {@link Retry} did about one expired computation — pure data, interpreted by the pump.
   */
  sealed interface RetryResult {

    /**
     * The work was redispatched; check back after the given timeout.
     *
     * @param timeout the new per-attempt timeout (new deadline = now + timeout)
     */
    record Retried(Duration timeout) implements RetryResult {

      /**
       * Requires an explicit timeout; use {@link RetriedDefault} to inherit the client's deadline.
       *
       * @throws NullPointerException if {@code timeout} is null
       */
      public Retried {
        Objects.requireNonNull(timeout, "timeout must not be null");
      }
    }

    /** The work was redispatched; extend by the client's configured deadline. */
    record RetriedDefault() implements RetryResult {}

    /**
     * The retry declined; the computation expires as {@code RETRY_EXHAUSTED}.
     *
     * @param reason the retry's words, carried on the expired outcome
     */
    record NotRetried(String reason) implements RetryResult {

      /**
       * Requires a reason — it becomes the expired outcome's message.
       *
       * @throws NullPointerException if {@code reason} is null
       */
      public NotRetried {
        Objects.requireNonNull(reason, "reason must not be null");
      }
    }

    /**
     * Redispatched; extend by the client's configured deadline.
     *
     * @return the result
     */
    static RetryResult retried() {
      return new RetriedDefault();
    }

    /**
     * Redispatched; extend by the given timeout.
     *
     * @param timeout the new per-attempt timeout
     * @return the result
     */
    static RetryResult retried(Duration timeout) {
      return new Retried(timeout);
    }

    /**
     * Declined; expire as {@code RETRY_EXHAUSTED} with the given reason.
     *
     * @param reason diagnostic prose carried on the expired outcome
     * @return the result
     */
    static RetryResult notRetried(String reason) {
      return new NotRetried(reason);
    }
  }
}
