package org.jwcarman.continuum;

import java.time.Duration;
import java.util.function.BiConsumer;

public interface RetryConfig<D> {

  RetryConfig<D> atMost(int attempts);

  RetryConfig<D> timeout(Duration timeout);

  RetryConfig<D> handler(BiConsumer<D, RetryContext> handler);
}
