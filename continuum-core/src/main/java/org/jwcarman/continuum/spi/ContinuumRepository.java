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

  /**
   * Atomically persists the computation and its initial continuation — both or neither.
   *
   * @param computation the pending computation
   * @param initial the mandatory first continuation
   */
  void createComputation(Computation computation, StoredContinuation initial);

  /**
   * Registers a continuation atomically with respect to completion: persisted if pending, the
   * memoized outcome if terminal, not-found otherwise.
   *
   * @param id the computation
   * @param continuation the continuation to persist if still pending
   * @return the atomic outcome
   */
  RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation continuation);

  /**
   * The ownership transfer, in one transaction: verify pending, delete the pending record, write
   * the memoized result, create one outbox delivery per registered continuation, delete the
   * continuations. First terminalization wins.
   *
   * @param id the computation to resolve
   * @param outcome the terminal outcome
   * @param completedAt the resolution instant
   * @return whether this call won, lost, or found nothing
   */
  CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt);

  /**
   * Looks up a computation — the pending record, or the memoized terminal view until purged.
   *
   * @param id the computation id
   * @return the computation, or empty
   */
  Optional<Computation> findComputation(ComputationId id);

  /**
   * Leases up to {@code limit} available deliveries of the given kind. Claimers must never block
   * one another; a lapsed lease makes a delivery claimable again.
   *
   * @param workerId diagnostic identity recorded on the claim
   * @param kind the kind to claim from
   * @param limit the maximum deliveries to lease
   * @param lease how long the claim holds
   * @param now the current instant
   * @return the leased deliveries
   */
  List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now);

  /**
   * Deletes a processed delivery — the outbox holds active obligations only.
   *
   * @param id the delivery
   */
  void acknowledgeDelivery(DeliveryId id);

  /**
   * Returns a failed delivery to the pool, incrementing its attempt count.
   *
   * @param id the delivery
   * @param retryAt when it becomes claimable again
   */
  void releaseDelivery(DeliveryId id, Instant retryAt);

  /**
   * Pending computations of the kind whose deadline has passed ({@code deadline <= now}).
   *
   * @param kind the kind to sweep
   * @param now the current instant
   * @param limit the maximum to return
   * @return overdue pending computations, oldest deadline first
   */
  List<Computation> findExpired(ComputationKind kind, Instant now, int limit);

  /**
   * Records a redispatch: new deadline and attempt count, atomically, if still pending.
   *
   * @param id the computation
   * @param newDeadline the extended deadline
   * @param attemptCount the new total attempt count
   */
  void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);

  /**
   * Deletes up to {@code limit} memoized results of the kind completed before the cutoff.
   *
   * @param kind the kind to purge
   * @param olderThan the completion-time cutoff
   * @param limit the maximum to delete
   * @return the number deleted
   */
  int purgeResults(ComputationKind kind, Instant olderThan, int limit);
}
