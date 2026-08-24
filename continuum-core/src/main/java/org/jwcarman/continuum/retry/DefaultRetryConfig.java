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
import java.util.function.BiConsumer;
import org.jwcarman.continuum.api.ExpiryContext;
import org.jwcarman.continuum.retry.Retry.RetryResult;

final class DefaultRetryConfig<D> implements RetryConfig<D> {

  private Integer maxAttempts;
  private Duration timeout;
  private BiConsumer<D, ExpiryContext> handler;

  @Override
  public RetryConfig<D> atMost(int attempts) {
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be at least 1");
    }
    this.maxAttempts = attempts;
    return this;
  }

  @Override
  public RetryConfig<D> timeout(Duration timeout) {
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    return this;
  }

  @Override
  public RetryConfig<D> handler(BiConsumer<D, ExpiryContext> handler) {
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
    return this;
  }

  Retry<D> buildRetry() {
    Objects.requireNonNull(handler, "handler must be configured");
    Integer max = maxAttempts;
    Duration configuredTimeout = timeout;
    BiConsumer<D, ExpiryContext> configuredHandler = handler;
    return (dispatch, context) -> {
      if (max != null && context.attemptCount() >= max) {
        return RetryResult.notRetried(
            "attempts exhausted (" + context.attemptCount() + " of " + max + ")");
      }
      configuredHandler.accept(dispatch, context);
      return configuredTimeout != null
          ? RetryResult.retried(configuredTimeout)
          : RetryResult.retried();
    };
  }
}
