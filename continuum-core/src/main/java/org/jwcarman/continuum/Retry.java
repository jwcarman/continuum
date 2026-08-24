package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;

@FunctionalInterface
public interface Retry<D> {

  RetryResult onTimeout(D dispatch, RetryContext context);

  static <D> Retry<D> of(RetryCustomizer<D> customizer) {
    Objects.requireNonNull(customizer, "customizer must not be null");
    DefaultRetryConfig<D> config = new DefaultRetryConfig<>();
    customizer.customize(config);
    return config.buildRetry();
  }

  sealed interface RetryResult {

    record Retried(Duration timeout) implements RetryResult {
      public Retried {
        Objects.requireNonNull(timeout, "timeout must not be null");
      }
    }

    record RetriedDefault() implements RetryResult {}

    record NotRetried(String reason) implements RetryResult {
      public NotRetried {
        Objects.requireNonNull(reason, "reason must not be null");
      }
    }

    static RetryResult retried() {
      return new RetriedDefault();
    }

    static RetryResult retried(Duration timeout) {
      return new Retried(timeout);
    }

    static RetryResult notRetried(String reason) {
      return new NotRetried(reason);
    }
  }
}
