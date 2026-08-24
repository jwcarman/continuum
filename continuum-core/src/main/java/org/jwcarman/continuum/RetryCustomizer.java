package org.jwcarman.continuum;

@FunctionalInterface
public interface RetryCustomizer<D> {

  void customize(RetryConfig<D> config);
}
