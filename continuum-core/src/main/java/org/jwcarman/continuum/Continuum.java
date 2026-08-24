package org.jwcarman.continuum;

import java.time.InstantSource;
import java.util.Optional;
import org.jwcarman.continuum.spi.ContinuumRepository;

public interface Continuum {

  Computation create(ComputationRequest request);

  RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);

  CompletionResult complete(ComputationId id, Outcome outcome);

  Optional<Computation> find(ComputationId id);

  InstantSource instants();

  ContinuumRepository repository();
}
