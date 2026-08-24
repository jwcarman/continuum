package org.jwcarman.continuum;

@FunctionalInterface
public interface RetryableClientCustomizer<R, C, D> {

  void customize(RetryableClientConfig<R, C, D> config);
}
