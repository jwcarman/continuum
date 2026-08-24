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
/**
 * The retry abstraction: {@link org.jwcarman.continuum.retry.Retry} performs (or schedules) a
 * redispatch itself and reports what it did. Attempt count is the only retry state Continuum
 * persists; limits and pacing are policy computed inside the retry from the durable facts carried
 * by {@link org.jwcarman.continuum.api.ExpiryContext}.
 */
package org.jwcarman.continuum.retry;
