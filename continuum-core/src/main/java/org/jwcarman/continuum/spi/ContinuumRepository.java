package org.jwcarman.continuum.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.Outcome;

public interface ContinuumRepository {

  void createComputation(Computation computation, StoredContinuation initial);

  RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation continuation);

  CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt);

  Optional<Computation> findComputation(ComputationId id);

  List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now);

  void acknowledgeDelivery(DeliveryId id);

  void releaseDelivery(DeliveryId id, Instant retryAt);

  List<Computation> findExpired(ComputationKind kind, Instant now, int limit);

  void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);

  int purgeResults(ComputationKind kind, Instant olderThan, int limit);
}
