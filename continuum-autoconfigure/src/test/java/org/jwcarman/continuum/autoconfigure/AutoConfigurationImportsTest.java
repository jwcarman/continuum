package org.jwcarman.continuum.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.jdbc.JdbcContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Boots through the real auto-configuration discovery mechanism (the {@code
 * AutoConfiguration.imports} resource), which {@code AutoConfigurations.of(...)}-based tests bypass
 * — a typo in that file would otherwise go undetected.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class AutoConfigurationImportsTest {

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class EmptyApplication {}

  @Test
  void the_imports_file_registers_the_auto_configurations() {
    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(EmptyApplication.class).web(WebApplicationType.NONE).run()) {
      // H2 and spring-boot-jdbc are on the test classpath, so Boot's own
      // DataSourceAutoConfiguration also activates via discovery and our JDBC provider
      // must win over the memory fallback — proving imports AND ordering end to end.
      assertThat(context.getBean(ContinuumRepository.class))
          .isInstanceOf(JdbcContinuumRepository.class);
      assertThat(context.getBean(Continuum.class)).isNotNull();
    }
  }
}
