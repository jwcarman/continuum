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
package org.jwcarman.continuum.autoconfigure;

import java.time.InstantSource;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.DefaultContinuum;
import org.jwcarman.continuum.memory.InMemoryContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Configures a {@link Continuum} from whatever {@link ContinuumRepository} is available, falling
 * back to in-memory persistence (with a warning) when no durable repository was configured.
 */
@AutoConfiguration
public class ContinuumAutoConfiguration {

  private static final Logger log = LoggerFactory.getLogger(ContinuumAutoConfiguration.class);

  /** Instantiated by Spring Boot's auto-configuration machinery, not by application code. */
  public ContinuumAutoConfiguration() {
    // Spring instantiates this class reflectively; nothing to initialize.
  }

  /**
   * Contributes an in-memory repository as the last resort, only when the application context
   * defines no {@link ContinuumRepository} of its own — and logs a warning, because the fallback
   * silently loses every computation on restart.
   *
   * @return a fresh {@link InMemoryContinuumRepository}
   */
  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository inMemoryContinuumRepository() {
    log.warn(
        "No durable Continuum persistence configured; defaulting to the in-memory repository. "
            + "Computations will NOT survive restarts. Add continuum-jdbc and a DataSource "
            + "(or define your own ContinuumRepository bean) for durability.");
    return new InMemoryContinuumRepository();
  }

  /**
   * Contributes the {@link Continuum} facade unless the application defines its own, wiring it to
   * whichever {@link ContinuumRepository} won the context and to an {@link InstantSource} bean if
   * one is present — tests supply a fixed source here; production falls back to {@link
   * InstantSource#system()}.
   *
   * @param repository the repository backing durability and atomicity
   * @param instants an optional clock override; absent means the system clock
   * @return the configured {@link Continuum}
   */
  @Bean
  @ConditionalOnMissingBean(Continuum.class)
  public Continuum continuum(
      ContinuumRepository repository, ObjectProvider<InstantSource> instants) {
    return new DefaultContinuum(repository, instants.getIfAvailable(InstantSource::system));
  }
}
