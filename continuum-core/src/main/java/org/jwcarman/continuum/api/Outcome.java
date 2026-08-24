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

import java.util.Arrays;
import java.util.Objects;

/**
 * The terminal resolution of a computation, three-armed: the producer answered ({@link Success}),
 * the producer said no ({@link Failure}), or the deadline passed with no answer ({@link Expired}).
 * A producer reporting failure and a deadline lapsing are different facts — a known "no" versus
 * "never heard back" — and consumers switch on exactly that distinction. {@code Expired} is minted
 * only by timeout processing; {@code Continuum.complete} rejects it.
 */
public sealed interface Outcome {

  /**
   * The producer answered. Equality is by payload contents rather than array identity, so a
   * memoized outcome read back from a repository compares equal to the one that was stored.
   *
   * @param payload the encoded result, opaque at this layer
   */
  record Success(byte[] payload) implements Outcome {

    /**
     * Requires a payload; encode an absent result as zero bytes rather than null.
     *
     * @throws NullPointerException if {@code payload} is null
     */
    public Success {
      Objects.requireNonNull(payload, "payload must not be null");
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Success(byte[] otherPayload) && Arrays.equals(payload, otherPayload);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
      return "Success[" + payload.length + " bytes]";
    }
  }

  /**
   * The producer reported a definite "no". Distinct from {@link Expired}: this is a known negative
   * answer, not the absence of one.
   *
   * @param message the producer's diagnostic prose
   */
  record Failure(String message) implements Outcome {

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
   * The deadline passed with no answer. Minted only by timeout processing — {@code
   * Continuum.complete} rejects it — so its presence always means a pump observed the lapse, never
   * that a producer reported one.
   *
   * @param kind which reap path expired the computation
   * @param message diagnostic prose describing the lapse
   */
  record Expired(ExpiryKind kind, String message) implements Outcome {

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

  /**
   * A successful outcome carrying the encoded result.
   *
   * @param payload the encoded result
   * @return the outcome
   */
  static Outcome success(byte[] payload) {
    return new Success(payload);
  }

  /**
   * A producer-reported failure.
   *
   * @param message the producer's words
   * @return the outcome
   */
  static Outcome failure(String message) {
    return new Failure(message);
  }

  /**
   * An expiry — minted only by timeout processing.
   *
   * @param kind which reap path expired it
   * @param message diagnostic prose
   * @return the outcome
   */
  static Outcome expired(ExpiryKind kind, String message) {
    return new Expired(kind, message);
  }

  /**
   * The derived status of a terminal outcome.
   *
   * @param outcome the outcome
   * @return COMPLETED, FAILED, or EXPIRED
   */
  static ComputationStatus statusOf(Outcome outcome) {
    return switch (outcome) {
      case Success _ -> ComputationStatus.COMPLETED;
      case Failure _ -> ComputationStatus.FAILED;
      case Expired _ -> ComputationStatus.EXPIRED;
    };
  }
}
