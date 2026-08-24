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
package org.jwcarman.continuum.spi;

/** The repository-level answer to a completion attempt. */
public enum CompletionOutcome {
  /**
   * The write won: the outcome was stored and one delivery was enqueued per registered
   * continuation, in the same transaction.
   */
  COMPLETED,
  /** An outcome was already stored. The provider must leave it untouched — first write wins. */
  ALREADY_RESOLVED,
  /** No such computation — it never existed, or its result has been purged. */
  NOT_FOUND
}
