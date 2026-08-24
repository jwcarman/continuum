package org.jwcarman.continuum;

@FunctionalInterface
public interface ClientCustomizer<R, C> {

  void customize(ClientConfig<R, C> config);
}
