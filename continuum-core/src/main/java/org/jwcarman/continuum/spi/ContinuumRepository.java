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
package org.jwcarman.continuum.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.Outcome;

/**
 * The persistence contract: semantic atomic operations, not generic CRUD. The correctness heart:
 * {@code createComputation} persists the computation and its initial continuation as one unit;
 * {@code registerContinuation} is atomic against completion (durable registration or the memoized
 * outcome, never neither); {@code complete} performs the ownership transfer — delete the pending
 * record, write the result, fan out one outbox delivery per continuation — in one transaction.
 * Certify implementations against the TCK in {@code continuum-testing}.
 */
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
