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
package org.jwcarman.continuum.memory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jwcarman.continuum.api.CompletionDelivery;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

/**
 * A faithful in-JVM {@link ContinuumRepository} — TCK-certified, real atomicity under a single
 * lock, lease-honoring claims. For tests and embedded single-process use; nothing survives a
 * restart.
 */
public final class InMemoryContinuumRepository implements ContinuumRepository {

  private final Object lock = new Object();
  private final Map<ComputationId, Computation> pending = new HashMap<>();
  private final Map<ComputationId, List<StoredContinuation>> continuations = new HashMap<>();
  private final Map<ComputationId, TerminalRecord> results = new HashMap<>();
  private final Map<DeliveryId, OutboxItem> outbox = new LinkedHashMap<>();

  /** Creates an empty repository; all state lives in this instance and dies with it. */
  public InMemoryContinuumRepository() {
    // Fields are initialized inline; nothing further to do.
  }

  private record TerminalRecord(Computation computation, Instant completedAt) {}

  private static final class OutboxItem {
    private final DeliveryId id;
    private final CompletionDelivery delivery;
    private Instant availableAt;
    private Instant claimedUntil;
    private int attemptCount;

    private OutboxItem(DeliveryId id, CompletionDelivery delivery, Instant availableAt) {
      this.id = id;
      this.delivery = delivery;
      this.availableAt = availableAt;
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    synchronized (lock) {
      if (pending.containsKey(computation.id()) || results.containsKey(computation.id())) {
        throw new ContinuumPersistenceException(
            "duplicate computation id: " + computation.id().value());
      }
      pending.put(computation.id(), computation);
      continuations.put(computation.id(), new ArrayList<>(List.of(initial)));
    }
  }

  @Override
  public RegistrationOutcome registerContinuation(
      ComputationId id, StoredContinuation continuation) {
    synchronized (lock) {
      if (pending.containsKey(id)) {
        continuations.get(id).add(continuation);
        return new RegistrationOutcome.Registered();
      }
      TerminalRecord terminal = results.get(id);
      if (terminal != null) {
        return new RegistrationOutcome.Resolved(terminal.computation().outcome());
      }
      return new RegistrationOutcome.NotFound();
    }
  }

  @Override
  public CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt) {
    synchronized (lock) {
      Computation current = pending.remove(id);
      if (current == null) {
        return results.containsKey(id)
            ? CompletionOutcome.ALREADY_RESOLVED
            : CompletionOutcome.NOT_FOUND;
      }
      Computation terminal =
          new Computation(
              current.id(),
              current.kind(),
              Outcome.statusOf(outcome),
              current.submittedAt(),
              current.deadline(),
              null,
              current.attemptCount(),
              outcome);
      results.put(id, new TerminalRecord(terminal, completedAt));
      for (StoredContinuation continuation : continuations.remove(id)) {
        OutboxItem item =
            new OutboxItem(
                DeliveryId.random(),
                new CompletionDelivery(
                    id,
                    current.kind(),
                    continuation.id(),
                    continuation.payload(),
                    outcome,
                    current.submittedAt(),
                    completedAt),
                completedAt);
        outbox.put(item.id, item);
      }
      return CompletionOutcome.COMPLETED;
    }
  }

  @Override
  public Optional<Computation> findComputation(ComputationId id) {
    synchronized (lock) {
      Computation current = pending.get(id);
      if (current != null) {
        return Optional.of(current);
      }
      return Optional.ofNullable(results.get(id)).map(TerminalRecord::computation);
    }
  }

  @Override
  public List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now) {
    synchronized (lock) {
      List<ClaimedDelivery> claimed = new ArrayList<>();
      outbox.values().stream()
          .filter(item -> item.delivery.kind().equals(kind))
          .filter(item -> !item.availableAt.isAfter(now))
          .filter(item -> item.claimedUntil == null || !item.claimedUntil.isAfter(now))
          .sorted(Comparator.comparing(item -> item.availableAt))
          .limit(limit)
          .forEach(
              item -> {
                item.claimedUntil = now.plus(lease);
                claimed.add(new ClaimedDelivery(item.id, item.delivery, item.attemptCount));
              });
      return claimed;
    }
  }

  @Override
  public void acknowledgeDelivery(DeliveryId id) {
    synchronized (lock) {
      outbox.remove(id);
    }
  }

  @Override
  public void releaseDelivery(DeliveryId id, Instant retryAt) {
    synchronized (lock) {
      OutboxItem item = outbox.get(id);
      if (item != null) {
        item.claimedUntil = null;
        item.availableAt = retryAt;
        item.attemptCount++;
      }
    }
  }

  @Override
  public List<Computation> findExpired(ComputationKind kind, Instant now, int limit) {
    synchronized (lock) {
      return pending.values().stream()
          .filter(computation -> computation.kind().equals(kind))
          .filter(computation -> !computation.deadline().isAfter(now))
          .sorted(Comparator.comparing(Computation::deadline))
          .limit(limit)
          .toList();
    }
  }

  @Override
  public void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount) {
    synchronized (lock) {
      pending.computeIfPresent(
          id,
          (key, computation) ->
              new Computation(
                  computation.id(),
                  computation.kind(),
                  computation.status(),
                  computation.submittedAt(),
                  newDeadline,
                  computation.dispatchPayload(),
                  attemptCount,
                  null));
    }
  }

  @Override
  public int purgeResults(ComputationKind kind, Instant olderThan, int limit) {
    synchronized (lock) {
      int purged = 0;
      Iterator<Map.Entry<ComputationId, TerminalRecord>> iterator = results.entrySet().iterator();
      while (iterator.hasNext() && purged < limit) {
        Map.Entry<ComputationId, TerminalRecord> entry = iterator.next();
        if (entry.getValue().computation().kind().equals(kind)
            && entry.getValue().completedAt().isBefore(olderThan)) {
          iterator.remove();
          purged++;
        }
      }
      return purged;
    }
  }
}
