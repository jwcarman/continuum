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
import java.util.function.BiConsumer;
import org.jwcarman.continuum.api.ExpiryContext;

/**
 * Declarative retry configuration: {@code atMost(n)} total attempts, an optional retry-specific
 * {@code timeout}, and a handler that <em>only dispatches</em> — results are derived mechanically.
 *
 * @param <D> the dispatch type
 */
public interface RetryConfig<D> {

  /**
   * Total attempt budget — the original dispatch is attempt 1. Exhaustion declines without invoking
   * the handler. Unset means unlimited.
   *
   * @param attempts the maximum total attempts, at least 1
   * @return this config
   */
  RetryConfig<D> atMost(int attempts);

  /**
   * A retry-specific per-attempt timeout; unset means the client's configured deadline.
   *
   * @param timeout the per-attempt timeout for retries
   * @return this config
   */
  RetryConfig<D> timeout(Duration timeout);

  /**
   * The dispatch action — it only dispatches; decisions are derived from the config. Required.
   *
   * @param handler receives the decoded dispatch payload and the durable facts
   * @return this config
   */
  RetryConfig<D> handler(BiConsumer<D, ExpiryContext> handler);
}
