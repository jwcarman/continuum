package org.jwcarman.continuum;

import java.time.Duration;
import java.util.function.BiConsumer;

public final class ContinuumClient<R, C> {

  private final ClientSupport<R, C> support;

  ContinuumClient(ClientSupport<R, C> support) {
    this.support = support;
  }

  public Computation create(C continuation) {
    return support.create(continuation, null, null);
  }

  public Computation create(C continuation, Duration deadlineOverride) {
    return support.create(continuation, null, deadlineOverride);
  }

  public CompletionResult complete(ComputationId id, R result) {
    return support.complete(id, result);
  }

  public CompletionResult fail(ComputationId id, String message) {
    return support.fail(id, message);
  }

  public TypedRegistration<R> register(ComputationId id, C continuation) {
    return support.register(id, continuation);
  }

  public int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    return support.deliverResults(batchSize, consumer);
  }

  public int reapExpiredComputations(int batchSize) {
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      support.failExpired(
          computation,
          ExpiryKind.RETRY_DISALLOWED,
          "deadline " + computation.deadline() + " passed");
      reaped++;
    }
    return reaped;
  }

  public int purgeExpiredResults(int batchSize, Duration ttl) {
    return support.purgeExpiredResults(batchSize, ttl);
  }

  public ComputationKind kind() {
    return support.kind();
  }
}
