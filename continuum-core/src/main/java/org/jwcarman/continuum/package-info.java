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
 * The construction and client surface of Continuum — durable computation coordination for Java.
 *
 * <p>Wire a {@link org.jwcarman.continuum.Continuum} over a {@link
 * org.jwcarman.continuum.spi.ContinuumRepository}, then mint one typed client per computation kind
 * with {@code continuum.client(...)}: the three-type {@link
 * org.jwcarman.continuum.RetryableContinuumClient} for retryable kinds (its shape demands a
 * dispatch payload), or the two-type {@link org.jwcarman.continuum.ContinuumClient} for
 * non-retryable kinds. Client configuration carries creation-time facts only (codecs, the
 * per-attempt deadline); daemon policy — batch sizes, leases, retry behavior, retention — is
 * supplied at the pump call sites.
 *
 * <p>The value vocabulary lives in {@link org.jwcarman.continuum.api}, the retry abstraction in
 * {@link org.jwcarman.continuum.retry}, and the persistence contract in {@link
 * org.jwcarman.continuum.spi}.
 */
package org.jwcarman.continuum;
