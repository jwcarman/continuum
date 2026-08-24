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

/**
 * Which reap path expired a computation: {@link #RETRY_DISALLOWED} — the kind was never retryable
 * (no dispatch payload existed); {@link #RETRY_EXHAUSTED} — retrying was possible and the retry
 * declined to continue. Each value can only be minted by its own path, so consumers can trust it.
 */
public enum ExpiryKind {
  /**
   * The kind was never retryable — no dispatch payload existed, so there was nothing to redispatch
   * and the lapse was terminal on first observation.
   */
  RETRY_DISALLOWED,
  /**
   * Retrying was possible, but the {@code Retry} declined to continue. The reason it gave is
   * carried on the expired outcome's message.
   */
  RETRY_EXHAUSTED
}
