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

  record Success<R>(R value) implements TypedOutcome<R> {
    public Success {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  record Failure<R>(String message) implements TypedOutcome<R> {
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  record Expired<R>(ExpiryKind kind, String message) implements TypedOutcome<R> {
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }
}
