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
import java.util.Objects;
import java.util.Optional;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationNotFoundException;
import org.jwcarman.continuum.api.ComputationRequest;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.api.RegistrationResult;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

/**
 * The standard {@link Continuum}: coordination logic over a {@link ContinuumRepository}. There is
 * one time authority per instance — the given {@link InstantSource} — from which all deadline
 * arithmetic derives.
 *
 * @param repository the persistence SPI beneath this instance
 * @param instants the single time authority for this instance
 */
public record DefaultContinuum(ContinuumRepository repository, InstantSource instants)
    implements Continuum {

  private static final String ID_NULL_MESSAGE = "id must not be null";

  /**
   * Requires both collaborators — there is no default repository and no implicit system clock.
   *
   * @throws NullPointerException if {@code repository} or {@code instants} is null
   */
  public DefaultContinuum {
    Objects.requireNonNull(repository, "repository must not be null");
    Objects.requireNonNull(instants, "instants must not be null");
  }

  /**
   * Coordinates against {@code repository} on the system instant source.
   *
   * @param repository the persistence SPI beneath this instance
   */
  public DefaultContinuum(ContinuumRepository repository) {
    this(repository, InstantSource.system());
  }

  @Override
  public Computation create(ComputationRequest request) {
    Objects.requireNonNull(request, "request must not be null");
    Computation computation =
        new Computation(
            ComputationId.random(),
            request.kind(),
            ComputationStatus.PENDING,
            instants.instant(),
            request.deadline(),
            request.dispatchPayload(),
            1,
            null);
    repository.createComputation(
        computation,
        new StoredContinuation(ContinuationId.random(), request.continuationPayload()));
    return computation;
  }

  @Override
  public RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload) {
    Objects.requireNonNull(id, ID_NULL_MESSAGE);
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    ContinuationId continuationId = ContinuationId.random();
    return switch (repository.registerContinuation(
        id, new StoredContinuation(continuationId, continuationPayload))) {
      case RegistrationOutcome.Registered _ -> new RegistrationResult.Registered(continuationId);
      case RegistrationOutcome.Resolved(Outcome memoized) ->
          new RegistrationResult.Resolved(memoized);
      case RegistrationOutcome.NotFound _ -> throw new ComputationNotFoundException(id);
    };
  }

  @Override
  public CompletionResult complete(ComputationId id, Outcome outcome) {
    Objects.requireNonNull(id, ID_NULL_MESSAGE);
    Objects.requireNonNull(outcome, "outcome must not be null");
    if (outcome instanceof Outcome.Expired) {
      throw new IllegalArgumentException(
          "Expired outcomes are minted by timeout processing; producers report success or failure");
    }
    return switch (repository.complete(id, outcome, instants.instant())) {
      case COMPLETED -> CompletionResult.COMPLETED;
      case ALREADY_RESOLVED -> CompletionResult.ALREADY_RESOLVED;
      case NOT_FOUND -> CompletionResult.NOT_FOUND;
    };
  }

  @Override
  public Optional<Computation> find(ComputationId id) {
    Objects.requireNonNull(id, ID_NULL_MESSAGE);
    return repository.findComputation(id);
  }
}
