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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.InstantSource;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.DefaultContinuum;
import org.jwcarman.continuum.jdbc.JdbcContinuumRepository;
import org.jwcarman.continuum.memory.InMemoryContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(OutputCaptureExtension.class)
class ContinuumAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  JdbcContinuumAutoConfiguration.class, ContinuumAutoConfiguration.class));

  @Configuration(proxyBeanMethods = false)
  static class DataSourceConfiguration {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomRepositoryConfiguration {
    static final ContinuumRepository CUSTOM = new InMemoryContinuumRepository();

    @Bean
    ContinuumRepository customRepository() {
      return CUSTOM;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class CustomContinuumConfiguration {
    static final Continuum CUSTOM = new DefaultContinuum(new InMemoryContinuumRepository());

    @Bean
    Continuum continuum() {
      return CUSTOM;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class FixedInstantsConfiguration {
    static final InstantSource FIXED = InstantSource.fixed(Instant.parse("2026-01-01T00:00:00Z"));

    @Bean
    InstantSource instants() {
      return FIXED;
    }
  }

  @Nested
  class Repository_selection {
    @Test
    void defaults_to_memory_with_a_warning_when_no_datasource_exists(CapturedOutput output) {
      runner.run(
          context -> {
            assertThat(context.getBean(ContinuumRepository.class))
                .isInstanceOf(InMemoryContinuumRepository.class);
            assertThat(output).contains("Computations will NOT survive restarts");
          });
    }

    @Test
    void defaults_to_memory_when_continuum_jdbc_is_not_on_the_classpath(CapturedOutput output) {
      runner
          .withClassLoader(new FilteredClassLoader(JdbcContinuumRepository.class))
          .withUserConfiguration(DataSourceConfiguration.class)
          .run(
              context -> {
                assertThat(context.getBean(ContinuumRepository.class))
                    .isInstanceOf(InMemoryContinuumRepository.class);
                assertThat(output).contains("defaulting to the in-memory repository");
              });
    }

    @Test
    void uses_jdbc_when_continuum_jdbc_and_a_datasource_are_present(CapturedOutput output) {
      runner
          .withUserConfiguration(DataSourceConfiguration.class)
          .run(
              context -> {
                assertThat(context.getBean(ContinuumRepository.class))
                    .isInstanceOf(JdbcContinuumRepository.class);
                assertThat(output).doesNotContain("in-memory");
              });
    }

    @Test
    void a_boot_auto_configured_datasource_is_seen_by_the_jdbc_provider() {
      // DataSourceAutoConfiguration must be ordered BEFORE our provider or its
      // @ConditionalOnBean(DataSource.class) evaluates against a not-yet-registered bean.
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  DataSourceAutoConfiguration.class,
                  JdbcContinuumAutoConfiguration.class,
                  ContinuumAutoConfiguration.class))
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(JdbcContinuumRepository.class));
    }

    @Test
    void an_application_defined_repository_wins() {
      runner
          .withUserConfiguration(CustomRepositoryConfiguration.class, DataSourceConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isSameAs(CustomRepositoryConfiguration.CUSTOM));
    }
  }

  @Nested
  class Continuum_bean {
    @Test
    void continuum_is_wired_over_the_selected_repository() {
      runner.run(
          context -> {
            Continuum continuum = context.getBean(Continuum.class);
            assertThat(continuum.repository()).isSameAs(context.getBean(ContinuumRepository.class));
            assertThat(continuum.instants()).isEqualTo(InstantSource.system());
          });
    }

    @Test
    void an_application_defined_continuum_wins() {
      runner
          .withUserConfiguration(CustomContinuumConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(Continuum.class))
                      .isSameAs(CustomContinuumConfiguration.CUSTOM));
    }

    @Test
    void an_application_defined_instant_source_is_used() {
      runner
          .withUserConfiguration(FixedInstantsConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(Continuum.class).instants())
                      .isSameAs(FixedInstantsConfiguration.FIXED));
    }
  }
}
