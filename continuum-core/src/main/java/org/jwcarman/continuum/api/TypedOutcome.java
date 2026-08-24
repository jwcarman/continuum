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

import java.util.Objects;

/**
 * The typed mirror of {@link Outcome}, produced by a client's codecs: {@link Success} carries the
 * decoded result value; {@link Failure} and {@link Expired} carry the same facts as their raw arms.
 *
 * @param <R> the decoded result type
 */
public sealed interface TypedOutcome<R> {

  /**
   * The producer answered, with the payload decoded by the client's result codec.
   *
   * @param value the decoded result
   * @param <R> the decoded result type
   */
  record Success<R>(R value) implements TypedOutcome<R> {

    /**
     * Requires a decoded value; a codec that yields null is a codec bug.
     *
     * @throws NullPointerException if {@code value} is null
     */
    public Success {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  /**
   * The producer reported a definite "no". Carries no decoded value — failures are prose, not
   * results — so it is generic only to sit in the same sealed hierarchy.
   *
   * @param message the producer's diagnostic prose
   * @param <R> the decoded result type
   */
  record Failure<R>(String message) implements TypedOutcome<R> {

    /**
     * Requires a message — a failure with no explanation is not worth memoizing.
     *
     * @throws NullPointerException if {@code message} is null
     */
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  /**
   * The deadline passed with no answer. Like {@link Failure}, it carries prose rather than a
   * decoded value.
   *
   * @param kind which reap path expired the computation
   * @param message diagnostic prose describing the lapse
   * @param <R> the decoded result type
   */
  record Expired<R>(ExpiryKind kind, String message) implements TypedOutcome<R> {

    /**
     * Requires both the reap path and an explanation.
     *
     * @throws NullPointerException if {@code kind} or {@code message} is null
     */
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }
}
