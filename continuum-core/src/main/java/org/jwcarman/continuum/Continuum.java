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
