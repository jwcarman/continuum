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

public interface Continuum {

  Computation create(ComputationRequest request);

  RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);

  CompletionResult complete(ComputationId id, Outcome outcome);

  Optional<Computation> find(ComputationId id);

  InstantSource instants();

  ContinuumRepository repository();

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
