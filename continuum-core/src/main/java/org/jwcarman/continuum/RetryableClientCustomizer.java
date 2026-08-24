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

/**
 * Fills in a {@link RetryableClientConfig} — the only thing a customizer ever does.
 *
 * @param <R> the result type
 * @param <C> the continuation type
 * @param <D> the dispatch type
 */
@FunctionalInterface
public interface RetryableClientCustomizer<R, C, D> {

  /**
   * Applies this customizer's settings to the given config.
   *
   * @param config the mutable config to populate
   */
  void customize(RetryableClientConfig<R, C, D> config);
}
