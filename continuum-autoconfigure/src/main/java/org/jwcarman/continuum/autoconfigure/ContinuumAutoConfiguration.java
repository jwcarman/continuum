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

  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository inMemoryContinuumRepository() {
    log.warn(
        "No durable Continuum persistence configured; defaulting to the in-memory repository. "
            + "Computations will NOT survive restarts. Add continuum-jdbc and a DataSource "
            + "(or define your own ContinuumRepository bean) for durability.");
    return new InMemoryContinuumRepository();
  }

  @Bean
  @ConditionalOnMissingBean(Continuum.class)
  public Continuum continuum(
      ContinuumRepository repository, ObjectProvider<InstantSource> instants) {
    return new DefaultContinuum(repository, instants.getIfAvailable(InstantSource::system));
  }
}
