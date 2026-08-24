package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.continuum.Retry.RetryResult;

final class DefaultRetryConfig<D> implements RetryConfig<D> {

  private Integer maxAttempts;
  private Duration timeout;
  private BiConsumer<D, RetryContext> handler;

  @Override
  public RetryConfig<D> atMost(int attempts) {
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be at least 1");
    }
    this.maxAttempts = attempts;
    return this;
  }

  @Override
  public RetryConfig<D> timeout(Duration timeout) {
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    return this;
  }

  @Override
  public RetryConfig<D> handler(BiConsumer<D, RetryContext> handler) {
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
    return this;
  }

  Retry<D> buildRetry() {
    Objects.requireNonNull(handler, "handler must be configured");
    Integer max = maxAttempts;
    Duration configuredTimeout = timeout;
    BiConsumer<D, RetryContext> configuredHandler = handler;
    return (dispatch, context) -> {
      if (max != null && context.attemptCount() >= max) {
        return RetryResult.notRetried(
            "attempts exhausted (" + context.attemptCount() + " of " + max + ")");
      }
      configuredHandler.accept(dispatch, context);
      return configuredTimeout != null
          ? RetryResult.retried(configuredTimeout)
          : RetryResult.retried();
    };
  }
}
