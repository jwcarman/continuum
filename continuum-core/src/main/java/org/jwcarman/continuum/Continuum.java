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

import java.time.InstantSource;
import java.util.Optional;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationRequest;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.api.RegistrationResult;
import org.jwcarman.continuum.spi.ContinuumRepository;

/**
 * The byte[] coordination contract — a durable, memoized eventual-value primitive. One computation
 * has one globally unique identity, one terminal outcome, and any number of durable continuations
 * interested in that outcome. Payloads are opaque at this layer; the typed clients minted by {@link
 * #client} put codecs over the boundary.
 */
public interface Continuum {

  /**
   * Atomically persists a new pending computation together with its mandatory initial continuation
   * — either both exist afterward or neither does.
   *
   * @param request the kind, initial continuation payload, deadline, and optional dispatch payload
   * @return the persisted pending computation (attempt count 1)
   */
  Computation create(ComputationRequest request);

  /**
   * Registers interest in a computation, atomically with respect to completion: if still pending,
   * the continuation is durably registered and guaranteed a delivery; if already resolved, the
   * memoized outcome is returned and nothing is persisted. Never neither.
   *
   * @param id the computation to watch
   * @param continuationPayload opaque bytes describing what should receive the outcome
   * @return the registration or the memoized outcome
   * @throws org.jwcarman.continuum.api.ComputationNotFoundException if the computation never
   *     existed or was purged
   */
  RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);

  /**
   * Reports a computation's terminal outcome. First successful terminalization wins; the stored
   * outcome is immutable thereafter, and one outbox delivery is fanned out per registered
   * continuation in the same transaction.
   *
   * @param id the computation to resolve
   * @param outcome {@code Success} or {@code Failure}; {@code Expired} is rejected — expiry is
   *     minted only by timeout processing
   * @return whether this call won, lost to an earlier resolution, or found nothing
   */
  CompletionResult complete(ComputationId id, Outcome outcome);

  /**
   * Looks up a computation — pending, or memoized terminal until purged.
   *
   * @param id the computation id
   * @return the computation, or empty if unknown or purged
   */
  Optional<Computation> find(ComputationId id);

  /**
   * The single time authority for this instance; all deadline arithmetic derives from it.
   *
   * @return the instant source
   */
  InstantSource instants();

  /**
   * The persistence SPI beneath this instance — also the raw pumping surface for untyped use.
   *
   * @return the repository
   */
  ContinuumRepository repository();

  /**
   * Mints the typed client for a non-retryable kind.
   *
   * @param kind the computation kind
   * @param resultType the result type
   * @param continuationType the continuation type
   * @param customizer fills in codecs and the per-attempt deadline
   * @param <R> the result type
   * @param <C> the continuation type
   * @return the client
   */
  default <R, C> ContinuumClient<R, C> client(
      String kind,
      Class<R> resultType,
      Class<C> continuationType,
      ClientCustomizer<R, C> customizer) {
    DefaultClientConfig<R, C> config = new DefaultClientConfig<>();
    customizer.customize(config);
    return new ContinuumClient<>(
        config.buildSupport(this, new ComputationKind(kind), resultType, continuationType));
  }

  /**
   * Mints the typed client for a retryable kind.
   *
   * @param kind the computation kind
   * @param resultType the result type
   * @param continuationType the continuation type
   * @param dispatchType the dispatch type
   * @param customizer fills in codecs and the per-attempt deadline
   * @param <R> the result type
   * @param <C> the continuation type
   * @param <D> the dispatch type
   * @return the client
   */
  default <R, C, D> RetryableContinuumClient<R, C, D> client(
      String kind,
      Class<R> resultType,
      Class<C> continuationType,
      Class<D> dispatchType,
      RetryableClientCustomizer<R, C, D> customizer) {
    DefaultRetryableClientConfig<R, C, D> config = new DefaultRetryableClientConfig<>();
    customizer.customize(config);
    return new RetryableContinuumClient<>(
        config.buildSupport(this, new ComputationKind(kind), resultType, continuationType),
        config.resolveDispatchCodec(dispatchType));
  }
}
