package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.Retry.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryableContinuumClient<R, C, D> {

  private static final Logger log = LoggerFactory.getLogger(RetryableContinuumClient.class);

  private final ClientSupport<R, C> support;
  private final Codec<D> dispatchCodec;

  RetryableContinuumClient(ClientSupport<R, C> support, Codec<D> dispatchCodec) {
    this.support = support;
    this.dispatchCodec = dispatchCodec;
  }

  public Computation create(C continuation, D dispatch) {
    return create(continuation, dispatch, null);
  }

  public Computation create(C continuation, D dispatch, Duration deadlineOverride) {
    Objects.requireNonNull(dispatch, "dispatch must not be null");
    return support.create(continuation, dispatchCodec.encode(dispatch), deadlineOverride);
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

  public int reapExpiredComputations(int batchSize, Retry<D> retry) {
    Objects.requireNonNull(retry, "retry must not be null");
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      if (computation.dispatchPayload() == null) {
        support.failExpired(
            computation,
            ExpiryKind.RETRY_DISALLOWED,
            "deadline " + computation.deadline() + " passed");
        reaped++;
        continue;
      }
      try {
        RetryResult result =
            retry.onTimeout(
                dispatchCodec.decode(computation.dispatchPayload()),
                new RetryContext(
                    computation.id(),
                    computation.kind(),
                    computation.attemptCount(),
                    computation.deadline()));
        switch (result) {
          case RetryResult.Retried(Duration timeout) ->
              support
                  .continuum()
                  .repository()
                  .extendDeadline(
                      computation.id(),
                      support.now().plus(timeout),
                      computation.attemptCount() + 1);
          case RetryResult.RetriedDefault() ->
              support
                  .continuum()
                  .repository()
                  .extendDeadline(
                      computation.id(),
                      support.now().plus(support.deadline()),
                      computation.attemptCount() + 1);
          case RetryResult.NotRetried(String reason) ->
              support.failExpired(computation, ExpiryKind.RETRY_EXHAUSTED, reason);
        }
        reaped++;
      } catch (RuntimeException e) {
        log.warn(
            "retry of computation {} failed; leaving pending for the next reap",
            computation.id().value(),
            e);
      }
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
