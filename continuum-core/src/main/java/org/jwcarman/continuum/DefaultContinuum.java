package org.jwcarman.continuum;

import java.time.InstantSource;
import java.util.Objects;
import java.util.Optional;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

public final class DefaultContinuum implements Continuum {

  private final ContinuumRepository repository;
  private final InstantSource instants;

  public DefaultContinuum(ContinuumRepository repository, InstantSource instants) {
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.instants = Objects.requireNonNull(instants, "instants must not be null");
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
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    ContinuationId continuationId = ContinuationId.random();
    return switch (repository.registerContinuation(
        id, new StoredContinuation(continuationId, continuationPayload))) {
      case RegistrationOutcome.Registered _ -> new RegistrationResult.Registered(continuationId);
      case RegistrationOutcome.Resolved resolved ->
          new RegistrationResult.Resolved(resolved.outcome());
      case RegistrationOutcome.NotFound _ -> throw new ComputationNotFoundException(id);
    };
  }

  @Override
  public CompletionResult complete(ComputationId id, Outcome outcome) {
    Objects.requireNonNull(id, "id must not be null");
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
    Objects.requireNonNull(id, "id must not be null");
    return repository.findComputation(id);
  }

  @Override
  public InstantSource instants() {
    return instants;
  }

  @Override
  public ContinuumRepository repository() {
    return repository;
  }
}
