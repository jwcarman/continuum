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
 * The value vocabulary of Continuum.
 *
 * <p>Identities ({@link org.jwcarman.continuum.api.ComputationId}, {@link
 * org.jwcarman.continuum.api.ContinuationId}, {@link org.jwcarman.continuum.api.ComputationKind}),
 * the three-arm {@link org.jwcarman.continuum.api.Outcome} and its typed mirror {@link
 * org.jwcarman.continuum.api.TypedOutcome}, the persisted {@link
 * org.jwcarman.continuum.api.Computation} view, request/result types, and the value-typed pump
 * parameters ({@link org.jwcarman.continuum.api.BatchSize}, {@link
 * org.jwcarman.continuum.api.Lease}, {@link org.jwcarman.continuum.api.Backoff}, {@link
 * org.jwcarman.continuum.api.ResultTtl}).
 *
 * <p>This package depends on nothing else in the library.
 */
package org.jwcarman.continuum.api;
