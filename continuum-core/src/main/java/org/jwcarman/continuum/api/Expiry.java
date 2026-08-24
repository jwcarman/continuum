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
package org.jwcarman.continuum.api;

import java.time.Duration;
import java.util.Objects;

/**
 * The policy consulted when a <em>non-retryable</em> computation's deadline lapses: keep waiting,
 * or give up with a reason.
 *
 * <p>This is the counterpart to {@code Retry} for kinds that must never be redispatched. It takes
 * no dispatch payload and never dispatches anything — it only decides whether the wait continues.
 * That is what makes an open-ended wait expressible without inventing a dispatch breadcrumb: the
 * two-type client's shape still declares the kind non-retryable by construction.
 *
 * <pre>{@code
 * approvals.failExpiredComputations(BatchSize.of(50), ctx ->
 *     ctx.elapsedTime().compareTo(Duration.ofDays(7)) > 0
 *         ? ExpiryResult.expired("no response within 7 days")
 *         : ExpiryResult.extended());
 * }</pre>
 */
@FunctionalInterface
public interface Expiry {

  /**
   * Decides what becomes of one lapsed computation.
   *
   * @param context Continuum's durable facts about the lapse
   * @return whether the wait continues, and for how long, or the reason it ends
   */
  ExpiryResult onTimeout(ExpiryContext context);

  /**
   * What an {@link Expiry} decided about one lapsed computation — pure data, applied by the pump.
   */
  sealed interface ExpiryResult {

    /**
     * Keep waiting; check back after the given timeout.
     *
     * @param timeout how much longer to wait (new deadline = now + timeout)
     */
    record Extended(Duration timeout) implements ExpiryResult {

      /**
       * Requires an explicit timeout; use {@link ExtendedDefault} to wait another client deadline.
       *
       * @throws NullPointerException if {@code timeout} is null
       */
      public Extended {
        Objects.requireNonNull(timeout, "timeout must not be null");
      }
    }

    /** Keep waiting; extend by the client's configured deadline. */
    record ExtendedDefault() implements ExpiryResult {}

    /**
     * Stop waiting; the computation expires as {@code RETRY_DISALLOWED}.
     *
     * @param reason the policy's words, carried on the expired outcome
     */
    record Expired(String reason) implements ExpiryResult {

      /**
       * Requires a reason — it becomes the expired outcome's message.
       *
       * @throws NullPointerException if {@code reason} is null
       */
      public Expired {
        Objects.requireNonNull(reason, "reason must not be null");
      }
    }

    /**
     * Keep waiting another client deadline.
     *
     * @return the result
     */
    static ExpiryResult extended() {
      return new ExtendedDefault();
    }

    /**
     * Keep waiting for the given timeout.
     *
     * @param timeout how much longer to wait
     * @return the result
     */
    static ExpiryResult extended(Duration timeout) {
      return new Extended(timeout);
    }

    /**
     * Stop waiting; expire with the given reason.
     *
     * @param reason diagnostic prose carried on the expired outcome
     * @return the result
     */
    static ExpiryResult expired(String reason) {
      return new Expired(reason);
    }
  }
}
