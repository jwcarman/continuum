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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.context.annotation.ConfigurationCondition.ConfigurationPhase;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

/**
 * The selection rule, in the shape of Spring Session's {@code store-type}: an explicit property
 * wins; otherwise the single candidate wins; two candidates fail startup naming the property; and a
 * user-defined {@link ContinuumRepository} bean always wins over auto-detection.
 *
 * <p>Evaluated in the {@link ConfigurationCondition.ConfigurationPhase#REGISTER_BEAN} phase — the
 * same phase {@code @ConditionalOnBean} uses — because the candidate check inspects the bean
 * factory for a {@code DataSource} or {@code MongoClient} bean, and those are not yet registered
 * during the earlier {@code PARSE_CONFIGURATION} phase that {@link SpringBootCondition} defaults
 * to.
 *
 * <p>Because auto-configuration ordering does not relate the JDBC configuration to Boot's Mongo
 * one, the ambiguity between two candidates may be reported from either provider's
 * {@code @ConditionalOnPersistenceType} evaluation — the message is the same either way.
 */
final class OnPersistenceTypeCondition extends SpringBootCondition
    implements ConfigurationCondition {

  static final String PROPERTY = "continuum.persistence.type";

  private static final String JDBC_REPOSITORY =
      "org.jwcarman.continuum.jdbc.JdbcContinuumRepository";
  private static final String MONGO_REPOSITORY =
      "org.jwcarman.continuum.mongo.MongoContinuumRepository";
  private static final String DATA_SOURCE = "javax.sql.DataSource";
  private static final String MONGO_CLIENT = "com.mongodb.client.MongoClient";

  /**
   * The phase in which this condition is evaluated.
   *
   * @return {@link ConfigurationPhase#REGISTER_BEAN}, so the {@code DataSource} / {@code
   *     MongoClient} candidate beans have already been registered when this condition runs
   */
  @Override
  public ConfigurationPhase getConfigurationPhase() {
    return ConfigurationPhase.REGISTER_BEAN;
  }

  @Override
  public ConditionOutcome getMatchOutcome(
      ConditionContext context, AnnotatedTypeMetadata metadata) {
    PersistenceType wanted =
        PersistenceType.valueOf(
            String.valueOf(
                metadata
                    .getAnnotationAttributes(ConditionalOnPersistenceType.class.getName())
                    .get("value")));
    ConditionMessage.Builder message = ConditionMessage.forCondition("ContinuumPersistence");
    if (context.getBeanFactory() != null
        && context
                .getBeanFactory()
                .getBeanNamesForType(ContinuumRepository.class, true, false)
                .length
            > 0) {
      return ConditionOutcome.noMatch(
          message.because("a ContinuumRepository bean is already defined"));
    }
    String configured = context.getEnvironment().getProperty(PROPERTY);
    if (configured != null) {
      PersistenceType selected;
      try {
        selected = PersistenceType.valueOf(configured.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        // Deliberately no cause: AssertJ's rootCause() (and Boot's own failure-analysis
        // reporting) walks to the deepest throwable, and the raw enum-lookup exception names
        // neither the property nor the valid choices — only this message does.
        throw new IllegalStateException(
            "Unknown " + PROPERTY + " '" + configured + "'; expected one of jdbc, mongo, memory.");
      }
      return selected == wanted
          ? ConditionOutcome.match(message.because(PROPERTY + "=" + configured))
          : ConditionOutcome.noMatch(message.because(PROPERTY + "=" + configured));
    }
    List<PersistenceType> candidates = new ArrayList<>();
    if (candidate(context, JDBC_REPOSITORY, DATA_SOURCE)) {
      candidates.add(PersistenceType.JDBC);
    }
    if (candidate(context, MONGO_REPOSITORY, MONGO_CLIENT)) {
      candidates.add(PersistenceType.MONGO);
    }
    if (candidates.size() > 1) {
      throw new IllegalStateException(
          "Multiple Continuum persistence providers are available ("
              + "jdbc: continuum-jdbc with a DataSource; mongo: continuum-mongo with a MongoClient"
              + "); set "
              + PROPERTY
              + " to jdbc, mongo or memory to choose.");
    }
    return candidates.contains(wanted)
        ? ConditionOutcome.match(message.because("only candidate is " + wanted))
        : ConditionOutcome.noMatch(message.because("candidates: " + candidates));
  }

  private static boolean candidate(ConditionContext context, String repository, String client) {
    ClassLoader loader = context.getClassLoader();
    if (!ClassUtils.isPresent(repository, loader) || !ClassUtils.isPresent(client, loader)) {
      return false;
    }
    ListableBeanFactory beans = context.getBeanFactory();
    return beans != null
        && beans.getBeanNamesForType(ClassUtils.resolveClassName(client, loader), true, false)
                .length
            > 0;
  }
}
