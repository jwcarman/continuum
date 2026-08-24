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

@FunctionalInterface
public interface Retry<D> {

  RetryResult onTimeout(D dispatch, RetryContext context);

  static <D> Retry<D> of(RetryCustomizer<D> customizer) {
    Objects.requireNonNull(customizer, "customizer must not be null");
    DefaultRetryConfig<D> config = new DefaultRetryConfig<>();
    customizer.customize(config);
    return config.buildRetry();
  }

  sealed interface RetryResult {

    record Retried(Duration timeout) implements RetryResult {
      public Retried {
        Objects.requireNonNull(timeout, "timeout must not be null");
      }
    }

    record RetriedDefault() implements RetryResult {}

    record NotRetried(String reason) implements RetryResult {
      public NotRetried {
        Objects.requireNonNull(reason, "reason must not be null");
      }
    }

    static RetryResult retried() {
      return new RetriedDefault();
    }

    static RetryResult retried(Duration timeout) {
      return new Retried(timeout);
    }

    static RetryResult notRetried(String reason) {
      return new NotRetried(reason);
    }
  }
}
