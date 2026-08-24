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
 * The answer to a completion attempt: {@link #COMPLETED} — this call won; {@link #ALREADY_RESOLVED}
 * — the outcome was already sealed (a late or duplicate report); {@link #NOT_FOUND} — no such
 * computation, or its result has been purged.
 */
public enum CompletionResult {
  /** This call won: the outcome it supplied is now the computation's sealed, memoized result. */
  COMPLETED,
  /**
   * The outcome was already sealed by an earlier call, which wins. This report was late or a
   * duplicate, and the stored outcome is unchanged.
   */
  ALREADY_RESOLVED,
  /** No such computation — it never existed, or its result has been purged. */
  NOT_FOUND
}
