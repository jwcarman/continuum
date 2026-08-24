# Continuum v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Continuum v1 — a Java library for durable asynchronous computation coordination (create → pending → complete → outbox delivery) with pluggable persistence.

**Architecture:** Presence-means-pending data model: a computation row exists only while pending; every terminalization (success / producer failure / expiry) atomically deletes it, writes a memoized result row, and fans out outbox deliveries. A byte[]-based `Continuum` core + `ContinuumRepository` SPI, with typed clients layered on top (`ContinuumClient<R,C>` for non-retryable kinds, `RetryableContinuumClient<R,C,D>` for retryable ones — two unrelated final classes; the shape is the retryability declaration) via `org.jwcarman.codec`. No threads and no pump classes: `deliverResults` / `reapExpiredComputations` (retry-consulting shape on three-type clients, always-fail shape on two-type clients) / `purgeExpiredResults` are batch methods on the client, scheduled by the application per kind. Outcome is three-armed (`Success(byte[])` / `Failure(String)` / `Expired(ExpiryKind, String)`); status is always derived, never stored; retryability ≡ dispatch-payload presence; `attemptCount` starts at 1.

**Tech Stack:** Java 25, Maven multi-module, JUnit 5 + AssertJ + Mockito, codec-core 0.1.0, PostgreSQL via Testcontainers (jdbc module), slf4j.

**Spec:** `docs/superpowers/specs/2026-08-23-continuum-design.md` (design) and `docs/superpowers/specs/2026-08-23-continuum-specification.md` (functional spec).

## Global Constraints

- groupId `org.jwcarman.continuum`, version `0.1.0-SNAPSHOT`, Java 25.
- **Never suppress warnings** (no `@SuppressWarnings` etc.) — fix the cause (e.g., avoid generic singleton casts by constructing new instances).
- **No star imports, no fully-qualified type names in code** — explicit single-symbol imports.
- Tests: snake_case method names + `@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)`, `@Nested` classes per behavior axis.
- Before EVERY commit: run `./mvnw -q spotless:apply` (google-java-format is checked at `validate`).
- Payloads are opaque `byte[]` everywhere in core/SPI/storage. Retryable ≡ `dispatchPayload != null`. `attemptCount` starts at 1. Status is derived from table residency, never stored.
- House construction idiom: factories take a named `XxxCustomizer` functional interface receiving an `XxxConfig` *interface* (fluent setters returning itself, no `build()` in the contract); the concrete config class is the private builder.
- Commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01UB7XefzStHgTCeH95rwJMc`

---

### Task 1: Project scaffold (parent pom, core module, repo files)

**Files:**
- Create: `pom.xml` (parent), `continuum-core/pom.xml`, `.gitignore`, `LICENSE`, `README.md`, `CHANGELOG.md`
- Copy: `mvnw` + `.mvn/` from `/Users/jcarman/IdeaProjects/substrate/`

**Interfaces:**
- Produces: a building multi-module skeleton; parent manages junit/assertj/mockito/testcontainers/slf4j/codec-core versions; `ci`/`release`/`license` profiles; Spotless.

- [ ] **Step 1: Copy boilerplate files**

```bash
cd /Users/jcarman/IdeaProjects/continuum
cp /Users/jcarman/IdeaProjects/substrate/LICENSE .
cp /Users/jcarman/IdeaProjects/substrate/.gitignore .
cp /Users/jcarman/IdeaProjects/substrate/mvnw .
cp -R /Users/jcarman/IdeaProjects/substrate/.mvn .mvn 2>/dev/null || mvn -N wrapper:wrapper
```

- [ ] **Step 2: Write parent `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>Continuum Parent</name>
    <description>Durable computation coordination for Java</description>
    <url>https://github.com/jwcarman/continuum</url>
    <inceptionYear>2026</inceptionYear>

    <developers>
        <developer>
            <id>jwcarman</id>
            <name>James Carman</name>
        </developer>
    </developers>

    <licenses>
        <license>
            <name>Apache 2</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
            <comments>A business-friendly OSS license</comments>
        </license>
    </licenses>

    <scm>
        <url>https://github.com/jwcarman/continuum</url>
        <connection>scm:git:git@github.com:jwcarman/continuum.git</connection>
        <developerConnection>scm:git:git@github.com:jwcarman/continuum.git</developerConnection>
    </scm>

    <properties>
        <maven.compiler.release>25</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <jacocoArgLine></jacocoArgLine>

        <codec.version>0.1.0</codec.version>
        <slf4j.version>2.0.17</slf4j.version>
        <logback.version>1.5.18</logback.version>
        <junit.version>5.14.0</junit.version>
        <assertj.version>3.27.3</assertj.version>
        <mockito.version>5.20.0</mockito.version>
        <bytebuddy.version>1.17.5</bytebuddy.version>
        <testcontainers.version>1.21.3</testcontainers.version>
        <postgresql.version>42.7.7</postgresql.version>

        <surefire.plugin.version>3.5.4</surefire.plugin.version>
        <spotless.version>3.4.0</spotless.version>
        <gpg.plugin.version>3.2.8</gpg.plugin.version>
        <javadoc.plugin.version>3.12.0</javadoc.plugin.version>
        <source.plugin.version>3.4.0</source.plugin.version>
        <sonatype.plugin.version>0.10.0</sonatype.plugin.version>
        <license.plugin.version>5.0.0</license.plugin.version>
        <jacoco.plugin.version>0.8.14</jacoco.plugin.version>
        <sonar.plugin.version>5.5.0.6356</sonar.plugin.version>

        <license.owner>James Carman</license.owner>

        <sonar.organization>jwcarman</sonar.organization>
        <sonar.host.url>https://sonarcloud.io</sonar.host.url>
        <sonar.projectKey>jwcarman_continuum</sonar.projectKey>
    </properties>

    <modules>
        <module>continuum-core</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.junit</groupId>
                <artifactId>junit-bom</artifactId>
                <version>${junit.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.jwcarman.codec</groupId>
                <artifactId>codec-core</artifactId>
                <version>${codec.version}</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>${logback.version}</version>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
            </dependency>
            <dependency>
                <groupId>org.mockito</groupId>
                <artifactId>mockito-core</artifactId>
                <version>${mockito.version}</version>
            </dependency>
            <dependency>
                <groupId>net.bytebuddy</groupId>
                <artifactId>byte-buddy-agent</artifactId>
                <version>${bytebuddy.version}</version>
            </dependency>
            <dependency>
                <groupId>org.postgresql</groupId>
                <artifactId>postgresql</artifactId>
                <version>${postgresql.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-core</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-memory</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-testing</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-jdbc</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.bytebuddy</groupId>
            <artifactId>byte-buddy-agent</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <executions>
                    <execution>
                        <goals>
                            <goal>properties</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>${surefire.plugin.version}</version>
                <configuration>
                    <argLine>@{jacocoArgLine} -javaagent:"${org.mockito:mockito-core:jar}" -javaagent:"${net.bytebuddy:byte-buddy-agent:jar}" -Xshare:off</argLine>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>${surefire.plugin.version}</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <argLine>@{jacocoArgLine} -Xshare:off</argLine>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.sonarsource.scanner.maven</groupId>
                <artifactId>sonar-maven-plugin</artifactId>
                <version>${sonar.plugin.version}</version>
            </plugin>
            <plugin>
                <groupId>com.diffplug.spotless</groupId>
                <artifactId>spotless-maven-plugin</artifactId>
                <version>${spotless.version}</version>
                <configuration>
                    <java>
                        <googleJavaFormat>
                            <style>GOOGLE</style>
                        </googleJavaFormat>
                    </java>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>check</goal>
                        </goals>
                        <phase>validate</phase>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    <profiles>
        <profile>
            <id>release</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.sonatype.central</groupId>
                        <artifactId>central-publishing-maven-plugin</artifactId>
                        <version>${sonatype.plugin.version}</version>
                        <extensions>true</extensions>
                        <configuration>
                            <publishingServerId>central</publishingServerId>
                            <tokenAuth>true</tokenAuth>
                            <autoPublish>true</autoPublish>
                        </configuration>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-source-plugin</artifactId>
                        <version>${source.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>attach-sources</id>
                                <phase>verify</phase>
                                <goals>
                                    <goal>jar-no-fork</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-javadoc-plugin</artifactId>
                        <version>${javadoc.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>attach-javadocs</id>
                                <phase>verify</phase>
                                <goals>
                                    <goal>jar</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-gpg-plugin</artifactId>
                        <version>${gpg.plugin.version}</version>
                        <executions>
                            <execution>
                                <id>sign-artifacts</id>
                                <phase>verify</phase>
                                <goals>
                                    <goal>sign</goal>
                                </goals>
                            </execution>
                        </executions>
                        <configuration>
                            <gpgArguments>
                                <arg>--pinentry-mode</arg>
                                <arg>loopback</arg>
                            </gpgArguments>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
        <profile>
            <id>ci</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.jacoco</groupId>
                        <artifactId>jacoco-maven-plugin</artifactId>
                        <version>${jacoco.plugin.version}</version>
                        <configuration>
                            <propertyName>jacocoArgLine</propertyName>
                        </configuration>
                        <executions>
                            <execution>
                                <id>prepare-agent</id>
                                <goals>
                                    <goal>prepare-agent</goal>
                                </goals>
                            </execution>
                            <execution>
                                <id>report</id>
                                <goals>
                                    <goal>report</goal>
                                </goals>
                                <configuration>
                                    <formats>
                                        <format>XML</format>
                                    </formats>
                                </configuration>
                            </execution>
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </profile>
        <profile>
            <id>license</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>com.mycila</groupId>
                        <artifactId>license-maven-plugin</artifactId>
                        <version>${license.plugin.version}</version>
                        <configuration>
                            <properties>
                                <owner>${license.owner}</owner>
                            </properties>
                            <licenseSets>
                                <licenseSet>
                                    <inlineHeader><![CDATA[Copyright © ${year} ${owner}

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.]]></inlineHeader>
                                    <excludes>
                                        <exclude>**/README.md</exclude>
                                        <exclude>src/test/resources/**</exclude>
                                        <exclude>src/main/resources/**/*.sql</exclude>
                                        <exclude>LICENSE</exclude>
                                        <exclude>.github/**</exclude>
                                        <exclude>**/*.txt</exclude>
                                        <exclude>**/*.md</exclude>
                                        <exclude>docs/**</exclude>
                                        <exclude>.mvn/**</exclude>
                                        <exclude>mvnw</exclude>
                                    </excludes>
                                </licenseSet>
                            </licenseSets>
                        </configuration>
                        <executions>
                            <execution>
                                <goals>
                                    <goal>check</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>
</project>
```

- [ ] **Step 3: Write `continuum-core/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.jwcarman.continuum</groupId>
        <artifactId>continuum-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>
    <artifactId>continuum-core</artifactId>
    <name>Continuum Core</name>
    <description>Continuum API, SPI, pumps, and typed client</description>
    <dependencies>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jwcarman.codec</groupId>
            <artifactId>codec-core</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 4: Write `README.md` stub (`# Continuum` + one-line description) and `CHANGELOG.md` stub (`# Changelog` + `## [Unreleased]`)**

- [ ] **Step 5: Verify the build**

Run: `./mvnw -q verify`
Expected: BUILD SUCCESS (no sources yet; spotless passes trivially).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: maven multi-module scaffold (parent + core)"
```

---

### Task 2: Core value types (`org.jwcarman.continuum`)

**Files:**
- Create: `continuum-core/src/main/java/org/jwcarman/continuum/ComputationId.java`, `ComputationKind.java`, `ContinuationId.java`, `ExpiryKind.java`, `ComputationStatus.java`, `Outcome.java`, `Computation.java`, `ComputationRequest.java`, `RegistrationResult.java`, `CompletionResult.java`, `CompletionDelivery.java`, `ComputationNotFoundException.java`
- Test: `continuum-core/src/test/java/org/jwcarman/continuum/ValueTypesTest.java`

**Interfaces:**
- Produces (exact, used by every later task):
  - `record ComputationId(UUID value)` with `static ComputationId random()`
  - `record ComputationKind(String value)` (non-blank)
  - `record ContinuationId(UUID value)` with `static ContinuationId random()`
  - `enum ExpiryKind { RETRY_DISALLOWED, RETRY_EXHAUSTED }`
  - `enum ComputationStatus { PENDING, COMPLETED, FAILED, EXPIRED }`
  - `sealed interface Outcome` with `record Success(byte[] payload)`, `record Failure(String message)`, `record Expired(ExpiryKind kind, String message)`; statics `Outcome success(byte[])`, `Outcome failure(String)`, `Outcome expired(ExpiryKind, String)`; `static ComputationStatus statusOf(Outcome)` → COMPLETED/FAILED/EXPIRED
  - `record Computation(ComputationId id, ComputationKind kind, ComputationStatus status, Instant createdAt, Instant deadline, byte[] dispatchPayload, int attemptCount, Outcome outcome)` with `boolean retryable()` (dispatchPayload != null); dispatchPayload and outcome nullable, all else required
  - `record ComputationRequest(ComputationKind kind, byte[] continuationPayload, Instant deadline, byte[] dispatchPayload)` — dispatchPayload nullable, rest required
  - `sealed interface RegistrationResult` with `record Registered(ContinuationId continuationId)`, `record Resolved(Outcome outcome)`
  - `enum CompletionResult { COMPLETED, ALREADY_RESOLVED, NOT_FOUND }`
  - `record CompletionDelivery(ComputationId computationId, ComputationKind kind, ContinuationId continuationId, byte[] continuationPayload, Outcome outcome)` — all required
  - `class ComputationNotFoundException extends RuntimeException` with ctor `(ComputationId id)`, message `"computation not found: " + id.value()`

- [ ] **Step 1: Write the failing tests**

```java
package org.jwcarman.continuum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ValueTypesTest {

  @Nested
  class Identifiers {
    @Test
    void random_computation_ids_are_unique() {
      assertThat(ComputationId.random()).isNotEqualTo(ComputationId.random());
    }

    @Test
    void computation_kind_rejects_blank() {
      assertThatIllegalArgumentException().isThrownBy(() -> new ComputationKind(" "));
    }

    @Test
    void computation_kind_rejects_null() {
      assertThatNullPointerException().isThrownBy(() -> new ComputationKind(null));
    }
  }

  @Nested
  class Outcomes {
    @Test
    void success_status_is_completed() {
      assertThat(Outcome.statusOf(Outcome.success(new byte[] {1}))).isEqualTo(ComputationStatus.COMPLETED);
    }

    @Test
    void failure_status_is_failed() {
      assertThat(Outcome.statusOf(Outcome.failure("boom"))).isEqualTo(ComputationStatus.FAILED);
    }

    @Test
    void expired_status_is_expired() {
      assertThat(Outcome.statusOf(Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (3 of 3)")))
          .isEqualTo(ComputationStatus.EXPIRED);
    }

    @Test
    void success_equality_compares_payload_contents() {
      assertThat(Outcome.success(new byte[] {1, 2})).isEqualTo(Outcome.success(new byte[] {1, 2}));
    }
  }

  @Nested
  class Requests {
    @Test
    void request_requires_continuation_payload() {
      assertThatNullPointerException()
          .isThrownBy(() -> new ComputationRequest(new ComputationKind("k"), null, Instant.EPOCH, null));
    }

    @Test
    void request_allows_null_dispatch_payload() {
      var request = new ComputationRequest(new ComputationKind("k"), new byte[] {1}, Instant.EPOCH, null);
      assertThat(request.dispatchPayload()).isNull();
    }
  }

  @Nested
  class Computations {
    @Test
    void retryable_means_dispatch_payload_present() {
      var pending = new Computation(ComputationId.random(), new ComputationKind("k"),
          ComputationStatus.PENDING, Instant.EPOCH, Instant.EPOCH.plusSeconds(60), new byte[] {1}, 1, null);
      assertThat(pending.retryable()).isTrue();
      var bare = new Computation(ComputationId.random(), new ComputationKind("k"),
          ComputationStatus.PENDING, Instant.EPOCH, Instant.EPOCH.plusSeconds(60), null, 1, null);
      assertThat(bare.retryable()).isFalse();
    }
  }
}
```

- [ ] **Step 2: Run tests, verify they fail to compile**

Run: `./mvnw -q -pl continuum-core test`
Expected: compilation errors (types do not exist).

- [ ] **Step 3: Implement the types**

`ComputationId.java` (`ContinuationId` is identical with the name swapped):

```java
package org.jwcarman.continuum;

import java.util.Objects;
import java.util.UUID;

public record ComputationId(UUID value) {
  public ComputationId {
    Objects.requireNonNull(value, "value must not be null");
  }

  public static ComputationId random() {
    return new ComputationId(UUID.randomUUID());
  }
}
```

`ComputationKind.java`:

```java
package org.jwcarman.continuum;

import java.util.Objects;

public record ComputationKind(String value) {
  public ComputationKind {
    Objects.requireNonNull(value, "value must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
```

`ExpiryKind.java`:

```java
package org.jwcarman.continuum;

public enum ExpiryKind {
  RETRY_DISALLOWED,
  RETRY_EXHAUSTED
}
```

`ComputationStatus.java`:

```java
package org.jwcarman.continuum;

public enum ComputationStatus {
  PENDING,
  COMPLETED,
  FAILED,
  EXPIRED
}
```

`Outcome.java` — note `Success` overrides equals/hashCode/toString because record equality on `byte[]` is reference-based:

```java
package org.jwcarman.continuum;

import java.util.Arrays;
import java.util.Objects;

public sealed interface Outcome {

  record Success(byte[] payload) implements Outcome {
    public Success {
      Objects.requireNonNull(payload, "payload must not be null");
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Success other && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
      return "Success[" + payload.length + " bytes]";
    }
  }

  record Failure(String message) implements Outcome {
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  record Expired(ExpiryKind kind, String message) implements Outcome {
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  static Outcome success(byte[] payload) {
    return new Success(payload);
  }

  static Outcome failure(String message) {
    return new Failure(message);
  }

  static Outcome expired(ExpiryKind kind, String message) {
    return new Expired(kind, message);
  }

  static ComputationStatus statusOf(Outcome outcome) {
    return switch (outcome) {
      case Success s -> ComputationStatus.COMPLETED;
      case Failure f -> ComputationStatus.FAILED;
      case Expired e -> ComputationStatus.EXPIRED;
    };
  }
}
```

`Computation.java`:

```java
package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record Computation(
    ComputationId id,
    ComputationKind kind,
    ComputationStatus status,
    Instant createdAt,
    Instant deadline,
    byte[] dispatchPayload,
    int attemptCount,
    Outcome outcome) {

  public Computation {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
    if (attemptCount < 1) {
      throw new IllegalArgumentException("attemptCount must be at least 1");
    }
  }

  public boolean retryable() {
    return dispatchPayload != null;
  }
}
```

`ComputationRequest.java`:

```java
package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record ComputationRequest(
    ComputationKind kind, byte[] continuationPayload, Instant deadline, byte[] dispatchPayload) {

  public ComputationRequest {
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }
}
```

`RegistrationResult.java`:

```java
package org.jwcarman.continuum;

import java.util.Objects;

public sealed interface RegistrationResult {

  record Registered(ContinuationId continuationId) implements RegistrationResult {
    public Registered {
      Objects.requireNonNull(continuationId, "continuationId must not be null");
    }
  }

  record Resolved(Outcome outcome) implements RegistrationResult {
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }
}
```

`CompletionResult.java`:

```java
package org.jwcarman.continuum;

public enum CompletionResult {
  COMPLETED,
  ALREADY_RESOLVED,
  NOT_FOUND
}
```

`CompletionDelivery.java`:

```java
package org.jwcarman.continuum;

import java.util.Objects;

public record CompletionDelivery(
    ComputationId computationId,
    ComputationKind kind,
    ContinuationId continuationId,
    byte[] continuationPayload,
    Outcome outcome) {

  public CompletionDelivery {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(continuationId, "continuationId must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
  }
}
```

`ComputationNotFoundException.java`:

```java
package org.jwcarman.continuum;

public class ComputationNotFoundException extends RuntimeException {

  public ComputationNotFoundException(ComputationId id) {
    super("computation not found: " + id.value());
  }
}
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `./mvnw -q -pl continuum-core test`
Expected: all tests PASS.

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: core value types (three-arm Outcome, derived status)"
```

---

### Task 3: Persistence SPI (`org.jwcarman.continuum.spi`)

**Files:**
- Create: `continuum-core/src/main/java/org/jwcarman/continuum/spi/StoredContinuation.java`, `DeliveryId.java`, `ClaimedDelivery.java`, `RegistrationOutcome.java`, `CompletionOutcome.java`, `ContinuumPersistenceException.java`, `ContinuumRepository.java`
- Test: `continuum-core/src/test/java/org/jwcarman/continuum/spi/SpiTypesTest.java`

**Interfaces:**
- Produces:
  - `record StoredContinuation(ContinuationId id, byte[] payload)` — both required
  - `record DeliveryId(UUID value)` with `static DeliveryId random()`
  - `record ClaimedDelivery(DeliveryId id, CompletionDelivery delivery, int attemptCount)`
  - `sealed interface RegistrationOutcome` with `record Registered()`, `record Resolved(Outcome outcome)`, `record NotFound()`
  - `enum CompletionOutcome { COMPLETED, ALREADY_RESOLVED, NOT_FOUND }`
  - `class ContinuumPersistenceException extends RuntimeException` with ctors `(String message, Throwable cause)` and `(String message)`
  - `interface ContinuumRepository`:

```java
public interface ContinuumRepository {
  void createComputation(Computation computation, StoredContinuation initial);
  RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation continuation);
  CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt);
  Optional<Computation> findComputation(ComputationId id);
  List<ClaimedDelivery> claimDeliveries(String workerId, ComputationKind kind, int limit, Duration lease, Instant now);
  void acknowledgeDelivery(DeliveryId id);
  void releaseDelivery(DeliveryId id, Instant retryAt);
  List<Computation> findExpired(ComputationKind kind, Instant now, int limit);
  void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount);
  int purgeResults(ComputationKind kind, Instant olderThan, int limit);
}
```

- [ ] **Step 1: Write the failing test**

```java
package org.jwcarman.continuum.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.ContinuationId;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SpiTypesTest {

  @Test
  void random_delivery_ids_are_unique() {
    assertThat(DeliveryId.random()).isNotEqualTo(DeliveryId.random());
  }

  @Test
  void stored_continuation_requires_payload() {
    assertThatNullPointerException().isThrownBy(() -> new StoredContinuation(ContinuationId.random(), null));
  }
}
```

- [ ] **Step 2: Run, expect compile failure** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 3: Implement**

All records follow the Task 2 pattern (`Objects.requireNonNull` in compact constructors; `DeliveryId.random()` mirrors `ComputationId.random()`). `RegistrationOutcome`:

```java
package org.jwcarman.continuum.spi;

import java.util.Objects;
import org.jwcarman.continuum.Outcome;

public sealed interface RegistrationOutcome {

  record Registered() implements RegistrationOutcome {}

  record Resolved(Outcome outcome) implements RegistrationOutcome {
    public Resolved {
      Objects.requireNonNull(outcome, "outcome must not be null");
    }
  }

  record NotFound() implements RegistrationOutcome {}
}
```

`ContinuumRepository` exactly as in the Interfaces block above (imports: `java.time.Duration`, `java.time.Instant`, `java.util.List`, `java.util.Optional`, and the `org.jwcarman.continuum` types). `ContinuumPersistenceException`:

```java
package org.jwcarman.continuum.spi;

public class ContinuumPersistenceException extends RuntimeException {

  public ContinuumPersistenceException(String message) {
    super(message);
  }

  public ContinuumPersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
```

- [ ] **Step 4: Run tests, verify pass** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: persistence SPI (kind-scoped pumping operations)"
```

---

### Task 4: `Continuum` interface + `DefaultContinuum`

**Files:**
- Create: `continuum-core/src/main/java/org/jwcarman/continuum/Continuum.java`, `DefaultContinuum.java`
- Test: `continuum-core/src/test/java/org/jwcarman/continuum/DefaultContinuumTest.java`

**Interfaces:**
- Consumes: Task 2 value types, Task 3 SPI.
- Produces:
  - `interface Continuum` — `Computation create(ComputationRequest)`, `RegistrationResult registerContinuation(ComputationId, byte[])`, `CompletionResult complete(ComputationId, Outcome)` (throws `IllegalArgumentException` on `Outcome.Expired`), `Optional<Computation> find(ComputationId)`, `InstantSource instants()`, `ContinuumRepository repository()`. (The `client(...)` default methods are added in Task 8.)
  - `final class DefaultContinuum implements Continuum` — ctor `(ContinuumRepository, InstantSource)`.

- [ ] **Step 1: Write the failing tests**

```java
package org.jwcarman.continuum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.InstantSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;
import org.mockito.ArgumentCaptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DefaultContinuumTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("k");

  private ContinuumRepository repository;
  private DefaultContinuum continuum;

  @BeforeEach
  void set_up() {
    repository = mock(ContinuumRepository.class);
    continuum = new DefaultContinuum(repository, InstantSource.fixed(NOW));
  }

  private ComputationRequest request() {
    return new ComputationRequest(KIND, "c".getBytes(UTF_8), NOW.plusSeconds(300), "d".getBytes(UTF_8));
  }

  @Nested
  class Creating {
    @Test
    void persists_pending_computation_with_initial_continuation() {
      var computation = continuum.create(request());

      var computationCaptor = ArgumentCaptor.forClass(Computation.class);
      var continuationCaptor = ArgumentCaptor.forClass(StoredContinuation.class);
      verify(repository).createComputation(computationCaptor.capture(), continuationCaptor.capture());

      assertThat(computationCaptor.getValue()).isEqualTo(computation);
      assertThat(computation.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(computation.createdAt()).isEqualTo(NOW);
      assertThat(computation.attemptCount()).isEqualTo(1);
      assertThat(computation.outcome()).isNull();
      assertThat(continuationCaptor.getValue().payload()).isEqualTo("c".getBytes(UTF_8));
    }
  }

  @Nested
  class Registering {
    @Test
    void registered_result_carries_the_generated_continuation_id() {
      when(repository.registerContinuation(any(), any())).thenReturn(new RegistrationOutcome.Registered());
      var result = continuum.registerContinuation(ComputationId.random(), "x".getBytes(UTF_8));
      assertThat(result).isInstanceOf(RegistrationResult.Registered.class);
    }

    @Test
    void resolved_result_carries_the_memoized_outcome() {
      var outcome = Outcome.success("r".getBytes(UTF_8));
      when(repository.registerContinuation(any(), any())).thenReturn(new RegistrationOutcome.Resolved(outcome));
      var result = continuum.registerContinuation(ComputationId.random(), "x".getBytes(UTF_8));
      assertThat(result).isEqualTo(new RegistrationResult.Resolved(outcome));
    }

    @Test
    void unknown_computation_throws() {
      when(repository.registerContinuation(any(), any())).thenReturn(new RegistrationOutcome.NotFound());
      var id = ComputationId.random();
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(id, "x".getBytes(UTF_8)));
    }
  }

  @Nested
  class Completing {
    @Test
    void rejects_expired_outcomes() {
      var id = ComputationId.random();
      var expired = Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (3 of 3)");
      assertThatIllegalArgumentException().isThrownBy(() -> continuum.complete(id, expired));
    }

    @Test
    void maps_repository_outcomes() {
      var id = ComputationId.random();
      var outcome = Outcome.success("r".getBytes(UTF_8));
      when(repository.complete(eq(id), eq(outcome), eq(NOW))).thenReturn(CompletionOutcome.ALREADY_RESOLVED);
      assertThat(continuum.complete(id, outcome)).isEqualTo(CompletionResult.ALREADY_RESOLVED);
    }
  }
}
```

- [ ] **Step 2: Run, expect compile failure** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 3: Implement**

`Continuum.java`:

```java
package org.jwcarman.continuum;

import java.time.InstantSource;
import java.util.Optional;
import org.jwcarman.continuum.spi.ContinuumRepository;

public interface Continuum {

  Computation create(ComputationRequest request);

  RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload);

  CompletionResult complete(ComputationId id, Outcome outcome);

  Optional<Computation> find(ComputationId id);

  InstantSource instants();

  ContinuumRepository repository();
}
```

`DefaultContinuum.java`:

```java
package org.jwcarman.continuum;

import java.time.InstantSource;
import java.util.Objects;
import java.util.Optional;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.StoredContinuation;

public final class DefaultContinuum implements Continuum {

  private final ContinuumRepository repository;
  private final InstantSource instants;

  public DefaultContinuum(ContinuumRepository repository, InstantSource instants) {
    this.repository = Objects.requireNonNull(repository, "repository must not be null");
    this.instants = Objects.requireNonNull(instants, "instants must not be null");
  }

  @Override
  public Computation create(ComputationRequest request) {
    Objects.requireNonNull(request, "request must not be null");
    Computation computation =
        new Computation(
            ComputationId.random(),
            request.kind(),
            ComputationStatus.PENDING,
            instants.instant(),
            request.deadline(),
            request.dispatchPayload(),
            1,
            null);
    repository.createComputation(
        computation, new StoredContinuation(ContinuationId.random(), request.continuationPayload()));
    return computation;
  }

  @Override
  public RegistrationResult registerContinuation(ComputationId id, byte[] continuationPayload) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(continuationPayload, "continuationPayload must not be null");
    ContinuationId continuationId = ContinuationId.random();
    return switch (repository.registerContinuation(id, new StoredContinuation(continuationId, continuationPayload))) {
      case org.jwcarman.continuum.spi.RegistrationOutcome.Registered r ->
          new RegistrationResult.Registered(continuationId);
      case org.jwcarman.continuum.spi.RegistrationOutcome.Resolved resolved ->
          new RegistrationResult.Resolved(resolved.outcome());
      case org.jwcarman.continuum.spi.RegistrationOutcome.NotFound n ->
          throw new ComputationNotFoundException(id);
    };
  }

  @Override
  public CompletionResult complete(ComputationId id, Outcome outcome) {
    Objects.requireNonNull(id, "id must not be null");
    Objects.requireNonNull(outcome, "outcome must not be null");
    if (outcome instanceof Outcome.Expired) {
      throw new IllegalArgumentException(
          "Expired outcomes are minted by timeout processing; producers report success or failure");
    }
    return switch (repository.complete(id, outcome, instants.instant())) {
      case COMPLETED -> CompletionResult.COMPLETED;
      case ALREADY_RESOLVED -> CompletionResult.ALREADY_RESOLVED;
      case NOT_FOUND -> CompletionResult.NOT_FOUND;
    };
  }

  @Override
  public Optional<Computation> find(ComputationId id) {
    Objects.requireNonNull(id, "id must not be null");
    return repository.findComputation(id);
  }

  @Override
  public InstantSource instants() {
    return instants;
  }

  @Override
  public ContinuumRepository repository() {
    return repository;
  }
}
```

NOTE (no-FQN rule): the `switch` above uses FQNs only to illustrate which types are meant — in the real file, `import org.jwcarman.continuum.spi.RegistrationOutcome;` and write `case RegistrationOutcome.Registered r ->` etc.

- [ ] **Step 4: Run tests, verify pass** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: Continuum interface and DefaultContinuum coordination"
```

---

### Task 5: `continuum-memory` — in-memory repository

**Files:**
- Create: `continuum-memory/pom.xml`, `continuum-memory/src/main/java/org/jwcarman/continuum/memory/InMemoryContinuumRepository.java`
- Modify: root `pom.xml` — add `<module>continuum-memory</module>`
- Test: `continuum-memory/src/test/java/org/jwcarman/continuum/memory/InMemoryContinuumRepositoryTest.java`

**Interfaces:**
- Consumes: `ContinuumRepository` and all SPI/value types.
- Produces: `public final class InMemoryContinuumRepository implements ContinuumRepository` with a no-arg constructor. Faithful semantics: single-lock atomicity, presence-means-pending, lease-honoring claims, kind-scoped operations. Expired means `!deadline.isAfter(now)` (deadline <= now) — every provider must use this exact comparison.

- [ ] **Step 1: Module pom**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.jwcarman.continuum</groupId>
        <artifactId>continuum-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>
    <artifactId>continuum-memory</artifactId>
    <name>Continuum Memory</name>
    <description>In-memory Continuum persistence for tests and embedded use</description>
    <dependencies>
        <dependency>
            <groupId>org.jwcarman.continuum</groupId>
            <artifactId>continuum-core</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: Write failing tests** (representative direct-semantics tests; the full battery arrives with the TCK in Task 6)

```java
package org.jwcarman.continuum.memory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.ContinuationId;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class InMemoryContinuumRepositoryTest {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("k");

  private InMemoryContinuumRepository repository;

  @BeforeEach
  void set_up() {
    repository = new InMemoryContinuumRepository();
  }

  private Computation pending(ComputationId id) {
    return new Computation(id, KIND, ComputationStatus.PENDING, NOW, NOW.plusSeconds(300), null, 1, null);
  }

  @Test
  void complete_transfers_pending_row_to_result_and_creates_deliveries() {
    var id = ComputationId.random();
    repository.createComputation(pending(id), new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));

    var outcome = Outcome.success("r".getBytes(UTF_8));
    assertThat(repository.complete(id, outcome, NOW.plusSeconds(1))).isEqualTo(CompletionOutcome.COMPLETED);

    var found = repository.findComputation(id).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.COMPLETED);
    assertThat(found.outcome()).isEqualTo(outcome);
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(600), 10)).isEmpty();

    var claimed = repository.claimDeliveries("w", KIND, 10, Duration.ofSeconds(30), NOW.plusSeconds(1));
    assertThat(claimed).hasSize(1);
    assertThat(claimed.getFirst().delivery().outcome()).isEqualTo(outcome);
  }

  @Test
  void deadline_at_now_counts_as_expired() {
    var id = ComputationId.random();
    repository.createComputation(pending(id), new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(300), 10)).hasSize(1);
    assertThat(repository.findExpired(KIND, NOW.plusSeconds(299), 10)).isEmpty();
  }
}
```

- [ ] **Step 3: Run, expect compile failure** — `./mvnw -q -pl continuum-memory test`

- [ ] **Step 4: Implement `InMemoryContinuumRepository`**

```java
package org.jwcarman.continuum.memory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jwcarman.continuum.CompletionDelivery;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

public final class InMemoryContinuumRepository implements ContinuumRepository {

  private final Object lock = new Object();
  private final Map<ComputationId, Computation> pending = new HashMap<>();
  private final Map<ComputationId, List<StoredContinuation>> continuations = new HashMap<>();
  private final Map<ComputationId, TerminalRecord> results = new HashMap<>();
  private final Map<DeliveryId, OutboxItem> outbox = new LinkedHashMap<>();

  private record TerminalRecord(Computation computation, Instant completedAt) {}

  private static final class OutboxItem {
    private final DeliveryId id;
    private final CompletionDelivery delivery;
    private Instant availableAt;
    private String claimedBy;
    private Instant claimedUntil;
    private int attemptCount;

    private OutboxItem(DeliveryId id, CompletionDelivery delivery, Instant availableAt) {
      this.id = id;
      this.delivery = delivery;
      this.availableAt = availableAt;
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    synchronized (lock) {
      if (pending.containsKey(computation.id()) || results.containsKey(computation.id())) {
        throw new ContinuumPersistenceException("duplicate computation id: " + computation.id().value());
      }
      pending.put(computation.id(), computation);
      continuations.put(computation.id(), new ArrayList<>(List.of(initial)));
    }
  }

  @Override
  public RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation continuation) {
    synchronized (lock) {
      if (pending.containsKey(id)) {
        continuations.get(id).add(continuation);
        return new RegistrationOutcome.Registered();
      }
      TerminalRecord terminal = results.get(id);
      if (terminal != null) {
        return new RegistrationOutcome.Resolved(terminal.computation().outcome());
      }
      return new RegistrationOutcome.NotFound();
    }
  }

  @Override
  public CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt) {
    synchronized (lock) {
      Computation current = pending.remove(id);
      if (current == null) {
        return results.containsKey(id) ? CompletionOutcome.ALREADY_RESOLVED : CompletionOutcome.NOT_FOUND;
      }
      Computation terminal =
          new Computation(
              current.id(),
              current.kind(),
              Outcome.statusOf(outcome),
              current.createdAt(),
              current.deadline(),
              null,
              current.attemptCount(),
              outcome);
      results.put(id, new TerminalRecord(terminal, completedAt));
      for (StoredContinuation continuation : continuations.remove(id)) {
        OutboxItem item =
            new OutboxItem(
                DeliveryId.random(),
                new CompletionDelivery(id, current.kind(), continuation.id(), continuation.payload(), outcome),
                completedAt);
        outbox.put(item.id, item);
      }
      return CompletionOutcome.COMPLETED;
    }
  }

  @Override
  public Optional<Computation> findComputation(ComputationId id) {
    synchronized (lock) {
      Computation current = pending.get(id);
      if (current != null) {
        return Optional.of(current);
      }
      return Optional.ofNullable(results.get(id)).map(TerminalRecord::computation);
    }
  }

  @Override
  public List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now) {
    synchronized (lock) {
      List<ClaimedDelivery> claimed = new ArrayList<>();
      outbox.values().stream()
          .filter(item -> item.delivery.kind().equals(kind))
          .filter(item -> !item.availableAt.isAfter(now))
          .filter(item -> item.claimedUntil == null || !item.claimedUntil.isAfter(now))
          .sorted(Comparator.comparing(item -> item.availableAt))
          .limit(limit)
          .forEach(
              item -> {
                item.claimedBy = workerId;
                item.claimedUntil = now.plus(lease);
                claimed.add(new ClaimedDelivery(item.id, item.delivery, item.attemptCount));
              });
      return claimed;
    }
  }

  @Override
  public void acknowledgeDelivery(DeliveryId id) {
    synchronized (lock) {
      outbox.remove(id);
    }
  }

  @Override
  public void releaseDelivery(DeliveryId id, Instant retryAt) {
    synchronized (lock) {
      OutboxItem item = outbox.get(id);
      if (item != null) {
        item.claimedBy = null;
        item.claimedUntil = null;
        item.availableAt = retryAt;
        item.attemptCount++;
      }
    }
  }

  @Override
  public List<Computation> findExpired(ComputationKind kind, Instant now, int limit) {
    synchronized (lock) {
      return pending.values().stream()
          .filter(c -> c.kind().equals(kind))
          .filter(c -> !c.deadline().isAfter(now))
          .sorted(Comparator.comparing(Computation::deadline))
          .limit(limit)
          .toList();
    }
  }

  @Override
  public void extendDeadline(ComputationId id, Instant newDeadline, int attemptCount) {
    synchronized (lock) {
      pending.computeIfPresent(
          id,
          (key, c) ->
              new Computation(
                  c.id(), c.kind(), c.status(), c.createdAt(), newDeadline, c.dispatchPayload(), attemptCount, null));
    }
  }

  @Override
  public int purgeResults(ComputationKind kind, Instant olderThan, int limit) {
    synchronized (lock) {
      int purged = 0;
      Iterator<Map.Entry<ComputationId, TerminalRecord>> iterator = results.entrySet().iterator();
      while (iterator.hasNext() && purged < limit) {
        Map.Entry<ComputationId, TerminalRecord> entry = iterator.next();
        if (entry.getValue().computation().kind().equals(kind)
            && entry.getValue().completedAt().isBefore(olderThan)) {
          iterator.remove();
          purged++;
        }
      }
      return purged;
    }
  }
}
```

- [ ] **Step 5: Run tests, verify pass** — `./mvnw -q -pl continuum-memory test` (add the module to root `pom.xml` `<modules>` first)

- [ ] **Step 6: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: in-memory ContinuumRepository (presence-means-pending)"
```

---

### Task 6: `continuum-testing` — TCK + wire memory module to it

**Files:**
- Create: `continuum-testing/pom.xml`, `continuum-testing/src/main/java/org/jwcarman/continuum/testing/MutableInstantSource.java`, `continuum-testing/src/main/java/org/jwcarman/continuum/testing/ContinuumTck.java`
- Create: `continuum-memory/src/test/java/org/jwcarman/continuum/memory/InMemoryContinuumTckTest.java`
- Modify: root `pom.xml` (add module), `continuum-memory/pom.xml` (add test dep on continuum-testing)

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces:
  - `public final class MutableInstantSource implements InstantSource` — ctor `(Instant start)`, `Instant instant()`, `void advance(Duration)`, `void set(Instant)`.
  - `public abstract class ContinuumTck` — `protected abstract ContinuumRepository createRepository();` plus protected fields `instants` (MutableInstantSource, starts at `2026-01-01T00:00:00Z`), `repository`, `continuum` (a `DefaultContinuum`), initialized in a `@BeforeEach`. Task 9 appends typed-client tests to this same class.
  - TCK ships its tests in `src/main/java` so provider modules run them via a test-scope dependency + subclass; junit-jupiter and assertj are therefore **compile**-scope in this module only.

- [ ] **Step 1: `continuum-testing/pom.xml`** — like continuum-memory's pom but artifactId `continuum-testing`, name `Continuum Testing`, description `Continuum TCK for persistence providers`, and dependencies: `continuum-core` (default scope), plus `org.junit.jupiter:junit-jupiter` and `org.assertj:assertj-core` **without** `<scope>test</scope>` (they must land on the compile classpath here; the parent's test-scope declarations don't apply because these are declared explicitly). Add `<module>continuum-testing</module>` to the root pom **before** `continuum-jdbc` will need it, and add to `continuum-memory/pom.xml`:

```xml
<dependency>
    <groupId>org.jwcarman.continuum</groupId>
    <artifactId>continuum-testing</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 2: `MutableInstantSource`**

```java
package org.jwcarman.continuum.testing;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;

public final class MutableInstantSource implements InstantSource {

  private volatile Instant current;

  public MutableInstantSource(Instant start) {
    this.current = Objects.requireNonNull(start, "start must not be null");
  }

  @Override
  public Instant instant() {
    return current;
  }

  public void advance(Duration duration) {
    current = current.plus(duration);
  }

  public void set(Instant instant) {
    current = Objects.requireNonNull(instant, "instant must not be null");
  }
}
```

- [ ] **Step 3: Write `ContinuumTck` (this IS the test battery — spec §38)**

```java
package org.jwcarman.continuum.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.CompletionResult;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationNotFoundException;
import org.jwcarman.continuum.ComputationRequest;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.Continuum;
import org.jwcarman.continuum.DefaultContinuum;
import org.jwcarman.continuum.ExpiryKind;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.RegistrationResult;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumRepository;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class ContinuumTck {

  protected static final ComputationKind KIND = new ComputationKind("tck");
  protected static final Duration LEASE = Duration.ofSeconds(30);

  protected MutableInstantSource instants;
  protected ContinuumRepository repository;
  protected Continuum continuum;

  protected abstract ContinuumRepository createRepository();

  @BeforeEach
  protected void setUpTck() {
    instants = new MutableInstantSource(Instant.parse("2026-01-01T00:00:00Z"));
    repository = createRepository();
    continuum = new DefaultContinuum(repository, instants);
  }

  protected ComputationRequest request(byte[] dispatchPayload) {
    return new ComputationRequest(
        KIND, "cont".getBytes(UTF_8), instants.instant().plus(Duration.ofMinutes(5)), dispatchPayload);
  }

  protected List<ClaimedDelivery> claimAll(String workerId) {
    return repository.claimDeliveries(workerId, KIND, 100, LEASE, instants.instant());
  }

  /** Runs both tasks as concurrently as a latch can make them; rethrows any task failure. */
  protected static void concurrently(Runnable first, Runnable second) {
    CountDownLatch start = new CountDownLatch(1);
    Callable<Void> a = () -> { start.await(); first.run(); return null; };
    Callable<Void> b = () -> { start.await(); second.run(); return null; };
    try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
      Future<Void> fa = pool.submit(a);
      Future<Void> fb = pool.submit(b);
      start.countDown();
      fa.get();
      fb.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (java.util.concurrent.ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  @Nested
  class Lifecycle {
    @Test
    void create_then_find_reports_pending() {
      var computation = continuum.create(request(null));
      var found = continuum.find(computation.id()).orElseThrow();
      assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
      assertThat(found.attemptCount()).isEqualTo(1);
    }

    @Test
    void completion_delivers_to_the_initial_continuation() {
      var computation = continuum.create(request(null));
      var outcome = Outcome.success("r".getBytes(UTF_8));
      assertThat(continuum.complete(computation.id(), outcome)).isEqualTo(CompletionResult.COMPLETED);

      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(1);
      assertThat(claimed.getFirst().delivery().computationId()).isEqualTo(computation.id());
      assertThat(claimed.getFirst().delivery().continuationPayload()).isEqualTo("cont".getBytes(UTF_8));
      assertThat(claimed.getFirst().delivery().outcome()).isEqualTo(outcome);

      repository.acknowledgeDelivery(claimed.getFirst().id());
      assertThat(claimAll("w1")).isEmpty();
    }

    @Test
    void duplicate_completion_is_already_resolved_and_outcome_is_immutable() {
      var computation = continuum.create(request(null));
      var winner = Outcome.success("first".getBytes(UTF_8));
      continuum.complete(computation.id(), winner);
      assertThat(continuum.complete(computation.id(), Outcome.failure("late")))
          .isEqualTo(CompletionResult.ALREADY_RESOLVED);
      assertThat(continuum.find(computation.id()).orElseThrow().outcome()).isEqualTo(winner);
    }

    @Test
    void completing_an_unknown_computation_is_not_found() {
      assertThat(continuum.complete(ComputationId.random(), Outcome.failure("x")))
          .isEqualTo(CompletionResult.NOT_FOUND);
    }

    @Test
    void registering_against_an_unknown_computation_throws() {
      var id = ComputationId.random();
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(id, "x".getBytes(UTF_8)));
    }
  }

  @Nested
  class Registration {
    @Test
    void continuation_registered_before_completion_receives_its_own_delivery() {
      var computation = continuum.create(request(null));
      var registration = continuum.registerContinuation(computation.id(), "second".getBytes(UTF_8));
      assertThat(registration).isInstanceOf(RegistrationResult.Registered.class);

      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(2);
      assertThat(claimed.stream().map(c -> c.delivery().continuationId()).distinct()).hasSize(2);
    }

    @Test
    void late_registration_returns_the_memoized_outcome_and_persists_nothing() {
      var computation = continuum.create(request(null));
      var outcome = Outcome.success("r".getBytes(UTF_8));
      continuum.complete(computation.id(), outcome);

      var registration = continuum.registerContinuation(computation.id(), "late".getBytes(UTF_8));
      assertThat(registration).isEqualTo(new RegistrationResult.Resolved(outcome));
      assertThat(claimAll("w1")).hasSize(1); // only the initial continuation's delivery
    }

    @Test
    void register_vs_complete_race_yields_exactly_one_of_registered_or_resolved() {
      for (int i = 0; i < 50; i++) {
        var computation = continuum.create(request(null));
        var outcome = Outcome.success("r".getBytes(UTF_8));
        var registrations = new ArrayList<RegistrationResult>();

        concurrently(
            () -> registrations.add(continuum.registerContinuation(computation.id(), "b".getBytes(UTF_8))),
            () -> continuum.complete(computation.id(), outcome));

        var claimed = claimAll("w1");
        switch (registrations.getFirst()) {
          case RegistrationResult.Registered(var continuationId) ->
              assertThat(claimed.stream().map(c -> c.delivery().continuationId())).contains(continuationId);
          case RegistrationResult.Resolved(var resolved) -> {
            assertThat(resolved).isEqualTo(outcome);
            assertThat(claimed).hasSize(1);
          }
        }
        claimed.forEach(c -> repository.acknowledgeDelivery(c.id()));
      }
    }

    @Test
    void concurrent_registrations_each_produce_exactly_one_delivery() {
      var computation = continuum.create(request(null));
      int extras = 8;
      CountDownLatch start = new CountDownLatch(1);
      try (ExecutorService pool = Executors.newFixedThreadPool(extras)) {
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < extras; i++) {
          byte[] payload = ("extra-" + i).getBytes(UTF_8);
          futures.add(pool.submit(() -> {
            start.await();
            continuum.registerContinuation(computation.id(), payload);
            return null;
          }));
        }
        start.countDown();
        for (Future<?> future : futures) {
          future.get();
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("w1");
      assertThat(claimed).hasSize(extras + 1);
      assertThat(claimed.stream().map(c -> c.delivery().continuationId()).distinct()).hasSize(extras + 1);
    }
  }

  @Nested
  class Racing {
    @Test
    void complete_vs_complete_has_exactly_one_winner_whose_outcome_is_stored() {
      for (int i = 0; i < 50; i++) {
        var computation = continuum.create(request(null));
        var success = Outcome.success("s".getBytes(UTF_8));
        var failure = Outcome.failure("f");
        var resultA = new ArrayList<CompletionResult>();
        var resultB = new ArrayList<CompletionResult>();

        concurrently(
            () -> resultA.add(continuum.complete(computation.id(), success)),
            () -> resultB.add(continuum.complete(computation.id(), failure)));

        var results = List.of(resultA.getFirst(), resultB.getFirst());
        assertThat(results).containsExactlyInAnyOrder(CompletionResult.COMPLETED, CompletionResult.ALREADY_RESOLVED);
        var stored = continuum.find(computation.id()).orElseThrow().outcome();
        if (resultA.getFirst() == CompletionResult.COMPLETED) {
          assertThat(stored).isEqualTo(success);
        } else {
          assertThat(stored).isEqualTo(failure);
        }
        claimAll("w1").forEach(c -> repository.acknowledgeDelivery(c.id()));
      }
    }

    @Test
    void expiry_vs_completion_has_exactly_one_winner() {
      var computation = continuum.create(request("d".getBytes(UTF_8)));
      instants.advance(Duration.ofMinutes(6));
      var success = Outcome.success("s".getBytes(UTF_8));
      var expired = Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (1 of 1)");

      concurrently(
          () -> repository.complete(computation.id(), success, instants.instant()),
          () -> repository.complete(computation.id(), expired, instants.instant()));

      var stored = continuum.find(computation.id()).orElseThrow();
      assertThat(stored.status()).isIn(ComputationStatus.COMPLETED, ComputationStatus.EXPIRED);
      assertThat(stored.outcome()).isIn(success, expired);
    }
  }

  @Nested
  class Claiming {
    private ClaimedDelivery singleDelivery() {
      var computation = continuum.create(request(null));
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("setup");
      assertThat(claimed).hasSize(1);
      repository.releaseDelivery(claimed.getFirst().id(), instants.instant());
      return claimed.getFirst();
    }

    @Test
    void competing_consumers_claim_disjoint_deliveries() {
      continuum.complete(continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      var claimedByA = new ArrayList<ClaimedDelivery>();
      var claimedByB = new ArrayList<ClaimedDelivery>();

      concurrently(() -> claimedByA.addAll(claimAll("wA")), () -> claimedByB.addAll(claimAll("wB")));

      assertThat(claimedByA.size() + claimedByB.size()).isEqualTo(1);
    }

    @Test
    void leased_deliveries_are_reclaimable_after_lease_expiry() {
      continuum.complete(continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      assertThat(claimAll("wA")).hasSize(1);
      assertThat(claimAll("wB")).isEmpty();          // still leased
      instants.advance(LEASE.plusSeconds(1));
      assertThat(claimAll("wB")).hasSize(1);          // lease lapsed, reclaimed
    }

    @Test
    void released_deliveries_return_after_the_backoff_with_incremented_attempts() {
      continuum.complete(continuum.create(request(null)).id(), Outcome.success("r".getBytes(UTF_8)));
      var claimed = claimAll("wA");
      repository.releaseDelivery(claimed.getFirst().id(), instants.instant().plus(Duration.ofSeconds(10)));

      assertThat(claimAll("wA")).isEmpty();           // still backing off
      instants.advance(Duration.ofSeconds(11));
      var reclaimed = claimAll("wA");
      assertThat(reclaimed).hasSize(1);
      assertThat(reclaimed.getFirst().attemptCount()).isEqualTo(1);
    }
  }

  @Nested
  class Expiry {
    @Test
    void find_expired_excludes_future_deadlines_and_terminal_computations() {
      var expiring = continuum.create(request(null));
      instants.advance(Duration.ofMinutes(1));
      var young = continuum.create(request(null)); // deadline 5m from the LATER now
      instants.advance(Duration.ofMinutes(4));     // expiring's deadline (<= now) passed; young's has not

      var expired = repository.findExpired(KIND, instants.instant(), 10);
      assertThat(expired).extracting(Computation::id).containsExactly(expiring.id());

      continuum.complete(expiring.id(), Outcome.failure("f"));
      assertThat(repository.findExpired(KIND, instants.instant(), 10)).isEmpty();
      assertThat(young.id()).isNotNull();
    }

    @Test
    void extend_deadline_defers_expiry_and_records_the_attempt() {
      var computation = continuum.create(request("d".getBytes(UTF_8)));
      instants.advance(Duration.ofMinutes(6));
      repository.extendDeadline(computation.id(), instants.instant().plus(Duration.ofMinutes(5)), 2);

      assertThat(repository.findExpired(KIND, instants.instant(), 10)).isEmpty();
      assertThat(continuum.find(computation.id()).orElseThrow().attemptCount()).isEqualTo(2);
    }
  }

  @Nested
  class Purging {
    @Test
    void purge_removes_only_results_older_than_the_cutoff() {
      var old = continuum.create(request(null));
      continuum.complete(old.id(), Outcome.success("r".getBytes(UTF_8)));
      instants.advance(Duration.ofHours(2));
      var recent = continuum.create(request(null));
      continuum.complete(recent.id(), Outcome.success("r".getBytes(UTF_8)));

      int purged = repository.purgeResults(KIND, instants.instant().minus(Duration.ofHours(1)), 100);
      assertThat(purged).isEqualTo(1);
      assertThat(continuum.find(old.id())).isEmpty();
      assertThat(continuum.find(recent.id())).isPresent();
    }

    @Test
    void purged_computations_behave_as_never_known() {
      var computation = continuum.create(request(null));
      continuum.complete(computation.id(), Outcome.success("r".getBytes(UTF_8)));
      instants.advance(Duration.ofHours(2));
      repository.purgeResults(KIND, instants.instant(), 100);

      assertThat(repository.complete(computation.id(), Outcome.failure("late"), instants.instant()))
          .isEqualTo(CompletionOutcome.NOT_FOUND);
      assertThatExceptionOfType(ComputationNotFoundException.class)
          .isThrownBy(() -> continuum.registerContinuation(computation.id(), "x".getBytes(UTF_8)));
    }
  }
}
```

- [ ] **Step 4: Wire memory module to the TCK**

`continuum-memory/src/test/java/org/jwcarman/continuum/memory/InMemoryContinuumTckTest.java`:

```java
package org.jwcarman.continuum.memory;

import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

class InMemoryContinuumTckTest extends ContinuumTck {

  @Override
  protected ContinuumRepository createRepository() {
    return new InMemoryContinuumRepository();
  }
}
```

- [ ] **Step 5: Run the whole build** — `./mvnw -q verify`
Expected: TCK passes against the memory provider. If a TCK test exposes a memory-provider bug, fix the provider (the TCK is the authority).

- [ ] **Step 6: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: ContinuumTck battery + memory provider certification"
```

---

### Task 7: `Retry<D>` — typed retry abstraction

**Files:**
- Create: `continuum-core/src/main/java/org/jwcarman/continuum/Retry.java`, `RetryContext.java`, `RetryConfig.java`, `RetryCustomizer.java`, `DefaultRetryConfig.java` (package-private)
- Test: `continuum-core/src/test/java/org/jwcarman/continuum/RetryTest.java`

**Interfaces:**
- Consumes: Task 2 types.
- Produces:
  - `record RetryContext(ComputationId computationId, ComputationKind kind, int attemptCount, Instant deadline)` — all required.
  - `interface Retry<D>` with `RetryResult onTimeout(D dispatch, RetryContext context)`, `static <D> Retry<D> of(RetryCustomizer<D> customizer)`, and nested `sealed interface RetryResult` — `record Retried(Duration timeout)`, `record RetriedDefault()`, `record NotRetried(String reason)`; statics `retried()`, `retried(Duration)`, `notRetried(String)`.
  - `interface RetryConfig<D>` — `RetryConfig<D> atMost(int attempts)`, `RetryConfig<D> timeout(Duration timeout)`, `RetryConfig<D> handler(BiConsumer<D, RetryContext> handler)` (handler required at build).
  - `@FunctionalInterface interface RetryCustomizer<D>` — `void customize(RetryConfig<D> config)`.
  - Exhaustion message format (exact): `"attempts exhausted (" + context.attemptCount() + " of " + max + ")"`.

- [ ] **Step 1: Write the failing tests**

```java
package org.jwcarman.continuum;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Retry.RetryResult;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RetryTest {

  private RetryContext contextWithAttempts(int attemptCount) {
    return new RetryContext(
        ComputationId.random(), new ComputationKind("k"), attemptCount, Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Nested
  class Declarative_retries {
    @Test
    void handler_is_invoked_and_default_timeout_reported_below_the_limit() {
      var dispatched = new ArrayList<String>();
      Retry<String> retry = Retry.of(r -> r.atMost(3).handler((dispatch, ctx) -> dispatched.add(dispatch)));

      var result = retry.onTimeout("work", contextWithAttempts(2));

      assertThat(dispatched).containsExactly("work");
      assertThat(result).isEqualTo(RetryResult.retried());
    }

    @Test
    void configured_timeout_overrides_the_default() {
      Retry<String> retry =
          Retry.of(r -> r.atMost(3).timeout(Duration.ofSeconds(7)).handler((dispatch, ctx) -> {}));
      assertThat(retry.onTimeout("work", contextWithAttempts(1)))
          .isEqualTo(RetryResult.retried(Duration.ofSeconds(7)));
    }

    @Test
    void exhausted_attempts_do_not_invoke_the_handler() {
      var dispatched = new ArrayList<String>();
      Retry<String> retry = Retry.of(r -> r.atMost(3).handler((dispatch, ctx) -> dispatched.add(dispatch)));

      var result = retry.onTimeout("work", contextWithAttempts(3));

      assertThat(dispatched).isEmpty();
      assertThat(result).isEqualTo(RetryResult.notRetried("attempts exhausted (3 of 3)"));
    }

    @Test
    void handler_is_required() {
      assertThatNullPointerException().isThrownBy(() -> Retry.of(r -> r.atMost(3)));
    }

    @Test
    void without_at_most_the_retry_never_exhausts() {
      Retry<String> retry = Retry.of(r -> r.handler((dispatch, ctx) -> {}));
      assertThat(retry.onTimeout("work", contextWithAttempts(1_000)))
          .isEqualTo(RetryResult.retried());
    }
  }

  @Nested
  class Custom_retries {
    @Test
    void a_direct_implementation_controls_the_result_entirely() {
      Retry<String> retry = (dispatch, ctx) -> RetryResult.notRetried("circuit open");
      assertThat(retry.onTimeout("work", contextWithAttempts(1)))
          .isEqualTo(RetryResult.notRetried("circuit open"));
    }
  }
}
```

- [ ] **Step 2: Run, expect compile failure** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 3: Implement**

`RetryContext.java`:

```java
package org.jwcarman.continuum;

import java.time.Instant;
import java.util.Objects;

public record RetryContext(
    ComputationId computationId, ComputationKind kind, int attemptCount, Instant deadline) {

  public RetryContext {
    Objects.requireNonNull(computationId, "computationId must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    Objects.requireNonNull(deadline, "deadline must not be null");
  }
}
```

`Retry.java`:

```java
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
```

`RetryConfig.java`:

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.util.function.BiConsumer;

public interface RetryConfig<D> {

  RetryConfig<D> atMost(int attempts);

  RetryConfig<D> timeout(Duration timeout);

  RetryConfig<D> handler(BiConsumer<D, RetryContext> handler);
}
```

`RetryCustomizer.java`:

```java
package org.jwcarman.continuum;

@FunctionalInterface
public interface RetryCustomizer<D> {

  void customize(RetryConfig<D> config);
}
```

`DefaultRetryConfig.java` (package-private — the hidden builder):

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.continuum.Retry.RetryResult;

final class DefaultRetryConfig<D> implements RetryConfig<D> {

  private Integer maxAttempts;
  private Duration timeout;
  private BiConsumer<D, RetryContext> handler;

  @Override
  public RetryConfig<D> atMost(int attempts) {
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be at least 1");
    }
    this.maxAttempts = attempts;
    return this;
  }

  @Override
  public RetryConfig<D> timeout(Duration timeout) {
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    return this;
  }

  @Override
  public RetryConfig<D> handler(BiConsumer<D, RetryContext> handler) {
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
    return this;
  }

  Retry<D> buildRetry() {
    Objects.requireNonNull(handler, "handler must be configured");
    Integer max = maxAttempts;
    Duration configuredTimeout = timeout;
    BiConsumer<D, RetryContext> configuredHandler = handler;
    return (dispatch, context) -> {
      if (max != null && context.attemptCount() >= max) {
        return RetryResult.notRetried("attempts exhausted (" + context.attemptCount() + " of " + max + ")");
      }
      configuredHandler.accept(dispatch, context);
      return configuredTimeout != null ? RetryResult.retried(configuredTimeout) : RetryResult.retried();
    };
  }
}
```

- [ ] **Step 4: Run tests, verify pass** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: Retry abstraction with declarative Retry.of customizer"
```

---

### Task 8: Typed clients — `ContinuumClient` / `RetryableContinuumClient` + minting

**Files:**
- Create: `continuum-core/src/main/java/org/jwcarman/continuum/TypedOutcome.java`, `TypedRegistration.java`, `ClientConfig.java`, `ClientCustomizer.java`, `RetryableClientConfig.java`, `RetryableClientCustomizer.java`, `ClientSupport.java` (package-private), `DefaultClientConfig.java` (package-private), `DefaultRetryableClientConfig.java` (package-private), `ContinuumClient.java`, `RetryableContinuumClient.java`
- Modify: `Continuum.java` — add the two `client(...)` default methods
- Test: `continuum-core/src/test/java/org/jwcarman/continuum/ClientMintingTest.java`

**Interfaces:**
- Consumes: Tasks 2–4, 7; `org.jwcarman.codec.spi.Codec` / `CodecFactory`.
- Produces:
  - `sealed interface TypedOutcome<R>` — `record Success<R>(R value)`, `record Failure<R>(String message)`, `record Expired<R>(ExpiryKind kind, String message)`.
  - `sealed interface TypedRegistration<R>` — `record Registered<R>(ContinuationId continuationId)`, `record Resolved<R>(TypedOutcome<R> outcome)`.
  - `interface ClientConfig<R, C>` — fluent, each returning `ClientConfig<R, C>`: `codecs(CodecFactory)`, `resultCodec(Codec<R>)`, `continuationCodec(Codec<C>)`, `deadline(Duration)`, `lease(Duration)`, `backoff(Duration)`, `workerId(String)`. Defaults: lease 30s, backoff 30s, workerId `"worker-" + UUID.randomUUID()`. `deadline` is REQUIRED; codecs resolve from explicit override first, else the `CodecFactory`, else `IllegalStateException` at mint time.
  - `interface RetryableClientConfig<R, C, D>` — same seven methods returning `RetryableClientConfig<R, C, D>`, plus `dispatchCodec(Codec<D>)`.
  - `@FunctionalInterface ClientCustomizer<R, C>` / `RetryableClientCustomizer<R, C, D>` with `void customize(...)`.
  - `final class ContinuumClient<R, C>` — `create(C continuation)`, `create(C continuation, Duration deadlineOverride)`, `complete(ComputationId, R)`, `fail(ComputationId, String)`, `register(ComputationId, C)`, `kind()` (+ pump methods in Task 9).
  - `final class RetryableContinuumClient<R, C, D>` — `create(C continuation, D dispatch)`, `create(C continuation, D dispatch, Duration deadlineOverride)` (dispatch required non-null), and the same shared surface.
  - On `Continuum`:

```java
default <R, C> ContinuumClient<R, C> client(
    String kind, Class<R> resultType, Class<C> continuationType, ClientCustomizer<R, C> customizer) { ... }

default <R, C, D> RetryableContinuumClient<R, C, D> client(
    String kind, Class<R> resultType, Class<C> continuationType, Class<D> dispatchType,
    RetryableClientCustomizer<R, C, D> customizer) { ... }
```

- [ ] **Step 1: Write the failing tests** (a tiny UTF-8 string codec is the test fixture; `TestCodecs` lives in the test tree)

```java
package org.jwcarman.continuum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.codec.spi.Codec;
import org.mockito.ArgumentCaptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ClientMintingTest {

  static final Codec<String> STRINGS = new Codec<>() {
    @Override
    public byte[] encode(String value) {
      return value.getBytes(UTF_8);
    }

    @Override
    public String decode(byte[] bytes) {
      return new String(bytes, UTF_8);
    }
  };

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  private Continuum continuum;

  @BeforeEach
  void set_up() {
    continuum = mock(Continuum.class, org.mockito.Mockito.CALLS_REAL_METHODS);
    when(continuum.instants()).thenReturn(InstantSource.fixed(NOW));
  }

  private RetryableContinuumClient<String, String, String> retryableClient() {
    return continuum.client("tool", String.class, String.class, String.class,
        cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS).dispatchCodec(STRINGS)
                  .deadline(Duration.ofMinutes(5)));
  }

  @Nested
  class Minting {
    @Test
    void deadline_is_required() {
      assertThatIllegalStateException().isThrownBy(() ->
          continuum.client("k", String.class, String.class,
              cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS)));
    }

    @Test
    void unresolvable_codec_fails_at_mint_time() {
      assertThatIllegalStateException().isThrownBy(() ->
          continuum.client("k", String.class, String.class,
              cfg -> cfg.continuationCodec(STRINGS).deadline(Duration.ofMinutes(1))));
    }
  }

  @Nested
  class Creating {
    @Test
    void retryable_create_encodes_both_payloads_and_computes_the_deadline() {
      when(continuum.create(any())).thenAnswer(invocation -> {
        ComputationRequest request = invocation.getArgument(0);
        return new Computation(ComputationId.random(), request.kind(), ComputationStatus.PENDING,
            NOW, request.deadline(), request.dispatchPayload(), 1, null);
      });

      retryableClient().create("my-continuation", "my-dispatch");

      var captor = ArgumentCaptor.forClass(ComputationRequest.class);
      verify(continuum).create(captor.capture());
      var request = captor.getValue();
      assertThat(request.kind()).isEqualTo(new ComputationKind("tool"));
      assertThat(request.continuationPayload()).isEqualTo("my-continuation".getBytes(UTF_8));
      assertThat(request.dispatchPayload()).isEqualTo("my-dispatch".getBytes(UTF_8));
      assertThat(request.deadline()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
    }

    @Test
    void one_shot_create_never_carries_a_dispatch_payload() {
      when(continuum.create(any())).thenAnswer(invocation -> {
        ComputationRequest request = invocation.getArgument(0);
        return new Computation(ComputationId.random(), request.kind(), ComputationStatus.PENDING,
            NOW, request.deadline(), request.dispatchPayload(), 1, null);
      });
      var client = continuum.client("approval", String.class, String.class,
          cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS).deadline(Duration.ofDays(3)));

      var computation = client.create("who-to-tell");
      assertThat(computation.dispatchPayload()).isNull();
      assertThat(computation.retryable()).isFalse();
    }
  }

  @Nested
  class Completing_and_registering {
    @Test
    void complete_encodes_the_result_as_success() {
      var id = ComputationId.random();
      when(continuum.complete(eq(id), any())).thenReturn(CompletionResult.COMPLETED);
      retryableClient().complete(id, "the-result");
      verify(continuum).complete(id, Outcome.success("the-result".getBytes(UTF_8)));
    }

    @Test
    void fail_reports_a_producer_failure() {
      var id = ComputationId.random();
      when(continuum.complete(eq(id), any())).thenReturn(CompletionResult.COMPLETED);
      retryableClient().fail(id, "tool blew up");
      verify(continuum).complete(id, Outcome.failure("tool blew up"));
    }

    @Test
    void register_decodes_a_resolved_outcome() {
      var id = ComputationId.random();
      when(continuum.registerContinuation(eq(id), any()))
          .thenReturn(new RegistrationResult.Resolved(Outcome.success("r".getBytes(UTF_8))));
      var registration = retryableClient().register(id, "late-party");
      assertThat(registration).isEqualTo(new TypedRegistration.Resolved<>(new TypedOutcome.Success<>("r")));
    }
  }
}
```

- [ ] **Step 2: Run, expect compile failure** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 3: Implement**

`TypedOutcome.java`:

```java
package org.jwcarman.continuum;

import java.util.Objects;

public sealed interface TypedOutcome<R> {

  record Success<R>(R value) implements TypedOutcome<R> {
    public Success {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  record Failure<R>(String message) implements TypedOutcome<R> {
    public Failure {
      Objects.requireNonNull(message, "message must not be null");
    }
  }

  record Expired<R>(ExpiryKind kind, String message) implements TypedOutcome<R> {
    public Expired {
      Objects.requireNonNull(kind, "kind must not be null");
      Objects.requireNonNull(message, "message must not be null");
    }
  }
}
```

`TypedRegistration.java` follows the same pattern (`Registered<R>(ContinuationId continuationId)`, `Resolved<R>(TypedOutcome<R> outcome)`, null-checked).

`ClientConfig.java` / `RetryableClientConfig.java` / the two customizers: exactly the signatures in the Interfaces block (plain interfaces, javadoc noting deadline is required and codec resolution order).

`ClientSupport.java` — the shared engine both clients delegate to (package-private):

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClientSupport<R, C> {

  private static final Logger log = LoggerFactory.getLogger(ClientSupport.class);

  private final Continuum continuum;
  private final ComputationKind kind;
  private final Codec<R> resultCodec;
  private final Codec<C> continuationCodec;
  private final Duration deadline;
  private final Duration lease;
  private final Duration backoff;
  private final String workerId;

  ClientSupport(Continuum continuum, ComputationKind kind, Codec<R> resultCodec, Codec<C> continuationCodec,
      Duration deadline, Duration lease, Duration backoff, String workerId) {
    this.continuum = continuum;
    this.kind = kind;
    this.resultCodec = resultCodec;
    this.continuationCodec = continuationCodec;
    this.deadline = deadline;
    this.lease = lease;
    this.backoff = backoff;
    this.workerId = workerId;
  }

  ComputationKind kind() {
    return kind;
  }

  Continuum continuum() {
    return continuum;
  }

  Duration deadline() {
    return deadline;
  }

  Instant now() {
    return continuum.instants().instant();
  }

  Computation create(C continuation, byte[] dispatchPayload, Duration deadlineOverride) {
    Objects.requireNonNull(continuation, "continuation must not be null");
    Duration effective = deadlineOverride != null ? deadlineOverride : deadline;
    return continuum.create(new ComputationRequest(
        kind, continuationCodec.encode(continuation), now().plus(effective), dispatchPayload));
  }

  CompletionResult complete(ComputationId id, R result) {
    Objects.requireNonNull(result, "result must not be null");
    return continuum.complete(id, Outcome.success(resultCodec.encode(result)));
  }

  CompletionResult fail(ComputationId id, String message) {
    Objects.requireNonNull(message, "message must not be null");
    return continuum.complete(id, Outcome.failure(message));
  }

  TypedRegistration<R> register(ComputationId id, C continuation) {
    Objects.requireNonNull(continuation, "continuation must not be null");
    return switch (continuum.registerContinuation(id, continuationCodec.encode(continuation))) {
      case RegistrationResult.Registered(var continuationId) -> new TypedRegistration.Registered<>(continuationId);
      case RegistrationResult.Resolved(var outcome) -> new TypedRegistration.Resolved<>(decode(outcome));
    };
  }

  TypedOutcome<R> decode(Outcome outcome) {
    return switch (outcome) {
      case Outcome.Success success -> new TypedOutcome.Success<>(resultCodec.decode(success.payload()));
      case Outcome.Failure failure -> new TypedOutcome.Failure<>(failure.message());
      case Outcome.Expired expired -> new TypedOutcome.Expired<>(expired.kind(), expired.message());
    };
  }

  int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    Objects.requireNonNull(consumer, "consumer must not be null");
    ContinuumRepository repository = continuum.repository();
    List<ClaimedDelivery> claimed = repository.claimDeliveries(workerId, kind, batchSize, lease, now());
    int delivered = 0;
    for (ClaimedDelivery delivery : claimed) {
      try {
        consumer.accept(
            continuationCodec.decode(delivery.delivery().continuationPayload()),
            decode(delivery.delivery().outcome()));
        repository.acknowledgeDelivery(delivery.id());
        delivered++;
      } catch (RuntimeException e) {
        log.warn("delivery {} failed; releasing for retry", delivery.id().value(), e);
        repository.releaseDelivery(delivery.id(), now().plus(backoff));
      }
    }
    return delivered;
  }

  int purgeExpiredResults(int batchSize, Duration ttl) {
    Objects.requireNonNull(ttl, "ttl must not be null");
    return continuum.repository().purgeResults(kind, now().minus(ttl), batchSize);
  }

  int failExpired(Computation computation, ExpiryKind expiryKind, String message) {
    continuum.repository().complete(computation.id(), Outcome.expired(expiryKind, message), now());
    return 1;
  }

  List<Computation> findExpired(int batchSize) {
    return continuum.repository().findExpired(kind, now(), batchSize);
  }
}
```

`ContinuumClient.java`:

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.util.function.BiConsumer;

public final class ContinuumClient<R, C> {

  private final ClientSupport<R, C> support;

  ContinuumClient(ClientSupport<R, C> support) {
    this.support = support;
  }

  public Computation create(C continuation) {
    return support.create(continuation, null, null);
  }

  public Computation create(C continuation, Duration deadlineOverride) {
    return support.create(continuation, null, deadlineOverride);
  }

  public CompletionResult complete(ComputationId id, R result) {
    return support.complete(id, result);
  }

  public CompletionResult fail(ComputationId id, String message) {
    return support.fail(id, message);
  }

  public TypedRegistration<R> register(ComputationId id, C continuation) {
    return support.register(id, continuation);
  }

  public int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    return support.deliverResults(batchSize, consumer);
  }

  public int reapExpiredComputations(int batchSize) {
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      reaped += support.failExpired(
          computation, ExpiryKind.RETRY_DISALLOWED, "deadline " + computation.deadline() + " passed");
    }
    return reaped;
  }

  public int purgeExpiredResults(int batchSize, Duration ttl) {
    return support.purgeExpiredResults(batchSize, ttl);
  }

  public ComputationKind kind() {
    return support.kind();
  }
}
```

`RetryableContinuumClient.java`:

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.Retry.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryableContinuumClient<R, C, D> {

  private static final Logger log = LoggerFactory.getLogger(RetryableContinuumClient.class);

  private final ClientSupport<R, C> support;
  private final Codec<D> dispatchCodec;

  RetryableContinuumClient(ClientSupport<R, C> support, Codec<D> dispatchCodec) {
    this.support = support;
    this.dispatchCodec = dispatchCodec;
  }

  public Computation create(C continuation, D dispatch) {
    return create(continuation, dispatch, null);
  }

  public Computation create(C continuation, D dispatch, Duration deadlineOverride) {
    Objects.requireNonNull(dispatch, "dispatch must not be null");
    return support.create(continuation, dispatchCodec.encode(dispatch), deadlineOverride);
  }

  public CompletionResult complete(ComputationId id, R result) {
    return support.complete(id, result);
  }

  public CompletionResult fail(ComputationId id, String message) {
    return support.fail(id, message);
  }

  public TypedRegistration<R> register(ComputationId id, C continuation) {
    return support.register(id, continuation);
  }

  public int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    return support.deliverResults(batchSize, consumer);
  }

  public int reapExpiredComputations(int batchSize, Retry<D> retry) {
    Objects.requireNonNull(retry, "retry must not be null");
    int reaped = 0;
    for (Computation computation : support.findExpired(batchSize)) {
      if (computation.dispatchPayload() == null) {
        reaped += support.failExpired(
            computation, ExpiryKind.RETRY_DISALLOWED, "deadline " + computation.deadline() + " passed");
        continue;
      }
      try {
        RetryResult result = retry.onTimeout(
            dispatchCodec.decode(computation.dispatchPayload()),
            new RetryContext(
                computation.id(), computation.kind(), computation.attemptCount(), computation.deadline()));
        switch (result) {
          case RetryResult.Retried(Duration timeout) ->
              support.continuum().repository().extendDeadline(
                  computation.id(), support.now().plus(timeout), computation.attemptCount() + 1);
          case RetryResult.RetriedDefault() ->
              support.continuum().repository().extendDeadline(
                  computation.id(), support.now().plus(support.deadline()), computation.attemptCount() + 1);
          case RetryResult.NotRetried(String reason) ->
              support.continuum().repository().complete(
                  computation.id(), Outcome.expired(ExpiryKind.RETRY_EXHAUSTED, reason), support.now());
        }
        reaped++;
      } catch (RuntimeException e) {
        log.warn("retry of computation {} failed; leaving pending for the next reap",
            computation.id().value(), e);
      }
    }
    return reaped;
  }

  public int purgeExpiredResults(int batchSize, Duration ttl) {
    return support.purgeExpiredResults(batchSize, ttl);
  }

  public ComputationKind kind() {
    return support.kind();
  }
}
```

`DefaultClientConfig.java` (package-private; `DefaultRetryableClientConfig` is the same shape plus `dispatchCodec` and returning `RetryableClientConfig<R, C, D>` from each setter):

```java
package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;

final class DefaultClientConfig<R, C> implements ClientConfig<R, C> {

  private CodecFactory codecFactory;
  private Codec<R> resultCodec;
  private Codec<C> continuationCodec;
  private Duration deadline;
  private Duration lease = Duration.ofSeconds(30);
  private Duration backoff = Duration.ofSeconds(30);
  private String workerId = "worker-" + UUID.randomUUID();

  @Override
  public ClientConfig<R, C> codecs(CodecFactory factory) {
    this.codecFactory = Objects.requireNonNull(factory, "factory must not be null");
    return this;
  }

  @Override
  public ClientConfig<R, C> resultCodec(Codec<R> codec) {
    this.resultCodec = Objects.requireNonNull(codec, "codec must not be null");
    return this;
  }

  // continuationCodec / deadline / lease / backoff / workerId setters follow the same pattern

  <T> Codec<T> resolve(Codec<T> explicit, Class<T> type, String role) {
    if (explicit != null) {
      return explicit;
    }
    if (codecFactory != null) {
      return codecFactory.create(type);
    }
    throw new IllegalStateException("no codec configured for " + role + " type " + type.getName());
  }

  ClientSupport<R, C> buildSupport(Continuum continuum, ComputationKind kind, Class<R> resultType, Class<C> continuationType) {
    if (deadline == null) {
      throw new IllegalStateException("deadline is required");
    }
    return new ClientSupport<>(continuum, kind,
        resolve(resultCodec, resultType, "result"),
        resolve(continuationCodec, continuationType, "continuation"),
        deadline, lease, backoff, workerId);
  }
}
```

`Continuum.java` — add the default methods (plus imports):

```java
default <R, C> ContinuumClient<R, C> client(
    String kind, Class<R> resultType, Class<C> continuationType, ClientCustomizer<R, C> customizer) {
  DefaultClientConfig<R, C> config = new DefaultClientConfig<>();
  customizer.customize(config);
  return new ContinuumClient<>(config.buildSupport(this, new ComputationKind(kind), resultType, continuationType));
}

default <R, C, D> RetryableContinuumClient<R, C, D> client(
    String kind, Class<R> resultType, Class<C> continuationType, Class<D> dispatchType,
    RetryableClientCustomizer<R, C, D> customizer) {
  DefaultRetryableClientConfig<R, C, D> config = new DefaultRetryableClientConfig<>();
  customizer.customize(config);
  return new RetryableContinuumClient<>(
      config.buildSupport(this, new ComputationKind(kind), resultType, continuationType),
      config.resolveDispatchCodec(dispatchType));
}
```

- [ ] **Step 4: Run tests, verify pass** — `./mvnw -q -pl continuum-core test`

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: typed clients (ContinuumClient / RetryableContinuumClient) with pump methods"
```

---

### Task 9: Typed-layer TCK coverage (pump methods end to end)

**Files:**
- Modify: `continuum-testing/src/main/java/org/jwcarman/continuum/testing/ContinuumTck.java` — append a `@Nested class Typed_clients`

**Interfaces:**
- Consumes: Tasks 6–8. Runs against every provider via the existing TCK subclasses — no new wiring.

- [ ] **Step 1: Append the failing tests to `ContinuumTck`** (new imports: `RetryableContinuumClient`, `ContinuumClient`, `TypedOutcome`, `Retry`, `Codec`, `AtomicReference`, `CopyOnWriteArrayList`)

```java
@Nested
class Typed_clients {

  static final org.jwcarman.codec.spi.Codec<String> STRINGS = new org.jwcarman.codec.spi.Codec<>() {
    @Override
    public byte[] encode(String value) {
      return value.getBytes(UTF_8);
    }

    @Override
    public String decode(byte[] bytes) {
      return new String(bytes, UTF_8);
    }
  };
  // (real file: import org.jwcarman.codec.spi.Codec and use the simple name)

  private RetryableContinuumClient<String, String, String> retryable() {
    return continuum.client("typed-tool", String.class, String.class, String.class,
        cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS).dispatchCodec(STRINGS)
                  .deadline(Duration.ofMinutes(5)).backoff(Duration.ofSeconds(10)));
  }

  private ContinuumClient<String, String> oneShot() {
    return continuum.client("typed-approval", String.class, String.class,
        cfg -> cfg.resultCodec(STRINGS).continuationCodec(STRINGS).deadline(Duration.ofMinutes(5)));
  }

  @Test
  void create_complete_deliver_roundtrip_with_user_types() {
    var client = retryable();
    var computation = client.create("route-me", "dispatch-me");
    client.complete(computation.id(), "the-answer");

    var received = new java.util.concurrent.CopyOnWriteArrayList<String>();
    int delivered = client.deliverResults(10, (continuation, outcome) -> {
      assertThat(continuation).isEqualTo("route-me");
      assertThat(outcome).isEqualTo(new TypedOutcome.Success<>("the-answer"));
      received.add(continuation);
    });
    assertThat(delivered).isEqualTo(1);
    assertThat(received).hasSize(1);
    assertThat(client.deliverResults(10, (c, o) -> {})).isZero(); // acknowledged, gone
  }

  @Test
  void failing_consumer_releases_the_delivery_for_redelivery_after_backoff() {
    var client = retryable();
    var computation = client.create("route-me", "dispatch-me");
    client.complete(computation.id(), "the-answer");

    assertThat(client.deliverResults(10, (c, o) -> { throw new IllegalStateException("consumer crash"); }))
        .isZero();
    assertThat(client.deliverResults(10, (c, o) -> {})).isZero();  // backing off
    instants.advance(Duration.ofSeconds(11));
    assertThat(client.deliverResults(10, (c, o) -> {})).isEqualTo(1);
  }

  @Test
  void reap_consults_the_retry_and_extends_the_deadline() {
    var client = retryable();
    var computation = client.create("route-me", "dispatch-me");
    instants.advance(Duration.ofMinutes(6));

    var redispatched = new java.util.concurrent.atomic.AtomicReference<String>();
    int reaped = client.reapExpiredComputations(10, Retry.of(r -> r.atMost(3)
        .handler((dispatch, ctx) -> {
          assertThat(ctx.computationId()).isEqualTo(computation.id());
          assertThat(ctx.attemptCount()).isEqualTo(1);
          redispatched.set(dispatch);
        })));

    assertThat(reaped).isEqualTo(1);
    assertThat(redispatched.get()).isEqualTo("dispatch-me");
    var found = continuum.find(computation.id()).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
    assertThat(found.attemptCount()).isEqualTo(2);
    assertThat(found.deadline()).isEqualTo(instants.instant().plus(Duration.ofMinutes(5)));
  }

  @Test
  void exhausted_retries_expire_the_computation_and_deliver_the_expiry() {
    var client = retryable();
    var computation = client.create("route-me", "dispatch-me");
    var retry = Retry.<String>of(r -> r.atMost(1).handler((dispatch, ctx) -> {}));

    instants.advance(Duration.ofMinutes(6));
    assertThat(client.reapExpiredComputations(10, retry)).isEqualTo(1);

    assertThat(continuum.find(computation.id()).orElseThrow().status()).isEqualTo(ComputationStatus.EXPIRED);
    var outcomes = new java.util.concurrent.CopyOnWriteArrayList<TypedOutcome<String>>();
    client.deliverResults(10, (continuation, outcome) -> outcomes.add(outcome));
    assertThat(outcomes)
        .containsExactly(new TypedOutcome.Expired<>(ExpiryKind.RETRY_EXHAUSTED, "attempts exhausted (1 of 1)"));
  }

  @Test
  void one_shot_reap_always_expires_with_retry_disallowed() {
    var client = oneShot();
    var computation = client.create("route-me");
    instants.advance(Duration.ofMinutes(6));

    assertThat(client.reapExpiredComputations(10)).isEqualTo(1);

    var found = continuum.find(computation.id()).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.EXPIRED);
    assertThat(found.outcome())
        .isEqualTo(Outcome.expired(ExpiryKind.RETRY_DISALLOWED, "deadline " + found.deadline() + " passed"));
  }

  @Test
  void purge_via_the_client_uses_call_site_ttl() {
    var client = oneShot();
    var computation = client.create("route-me");
    client.complete(computation.id(), "done");
    instants.advance(Duration.ofHours(2));

    assertThat(client.purgeExpiredResults(100, Duration.ofHours(1))).isEqualTo(1);
    assertThat(continuum.find(computation.id())).isEmpty();
  }
}
```

- [ ] **Step 2: Run against the memory provider** — `./mvnw -q -pl continuum-testing,continuum-memory test`
Expected: PASS (fix core/provider code if any test exposes a bug — the TCK is the authority).

- [ ] **Step 3: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "test: typed-client pump coverage in the TCK"
```

---

### Task 10: `continuum-jdbc` — PostgreSQL provider

**Files:**
- Create: `continuum-jdbc/pom.xml`, `continuum-jdbc/src/main/resources/org/jwcarman/continuum/jdbc/continuum-postgresql.sql`, `continuum-jdbc/src/main/java/org/jwcarman/continuum/jdbc/JdbcContinuumRepository.java`
- Test: `continuum-jdbc/src/test/java/org/jwcarman/continuum/jdbc/JdbcContinuumTckIT.java`, `PostgresSupport.java`, `OutboxFailureInjectionIT.java`
- Modify: root `pom.xml` — add `<module>continuum-jdbc</module>`

**Interfaces:**
- Consumes: SPI (Task 3), TCK (Tasks 6+9).
- Produces: `public final class JdbcContinuumRepository implements ContinuumRepository`, ctor `(DataSource dataSource)`. Schema resource path: `org/jwcarman/continuum/jdbc/continuum-postgresql.sql`. All SQLExceptions wrapped in `ContinuumPersistenceException`.

- [ ] **Step 1: Module pom** — parent like the others; artifactId `continuum-jdbc`, name `Continuum JDBC`, description `PostgreSQL Continuum persistence`. Dependencies: `continuum-core`; `org.postgresql:postgresql` with `<scope>provided</scope>` (the app supplies its driver); test-scope: `continuum-testing`, `org.testcontainers:junit-jupiter`, `org.testcontainers:postgresql`, and `org.postgresql:postgresql` (test needs the real driver — redeclare with `<scope>test</scope>`... a dependency cannot appear twice; instead use `<scope>runtime</scope>`? No: declare the driver once with `<scope>provided</scope>` — provided IS on the test classpath, so no second declaration is needed).

- [ ] **Step 2: Schema DDL** (`continuum-postgresql.sql`)

```sql
CREATE TABLE IF NOT EXISTS continuum_computation (
    id UUID PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL,
    dispatch_payload BYTEA,
    attempt_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_computation_kind_deadline
    ON continuum_computation (kind, deadline_at);

CREATE TABLE IF NOT EXISTS continuum_continuation (
    id UUID PRIMARY KEY,
    computation_id UUID NOT NULL REFERENCES continuum_computation (id),
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_continuation_computation
    ON continuum_continuation (computation_id);

CREATE TABLE IF NOT EXISTS continuum_result (
    computation_id UUID PRIMARY KEY,
    kind VARCHAR(200) NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload BYTEA,
    expiry_kind VARCHAR(20),
    message TEXT,
    deadline_at TIMESTAMPTZ NOT NULL,
    attempt_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_result_kind_completed
    ON continuum_result (kind, completed_at);

CREATE TABLE IF NOT EXISTS continuum_outbox (
    id UUID PRIMARY KEY,
    computation_id UUID NOT NULL,
    continuation_id UUID NOT NULL,
    kind VARCHAR(200) NOT NULL,
    continuation_payload BYTEA NOT NULL,
    outcome_type VARCHAR(20) NOT NULL,
    outcome_payload BYTEA,
    expiry_kind VARCHAR(20),
    message TEXT,
    available_at TIMESTAMPTZ NOT NULL,
    claimed_by VARCHAR(200),
    claimed_until TIMESTAMPTZ,
    attempt_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_continuum_outbox_kind_available
    ON continuum_outbox (kind, available_at);
```

- [ ] **Step 3: TCK integration test (the failing test)**

`PostgresSupport.java` (test tree) — container + datasource + schema + truncation:

```java
package org.jwcarman.continuum.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

final class PostgresSupport {

  private PostgresSupport() {}

  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  static {
    POSTGRES.start();
  }

  static DataSource dataSource() {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setURL(POSTGRES.getJdbcUrl());
    dataSource.setUser(POSTGRES.getUsername());
    dataSource.setPassword(POSTGRES.getPassword());
    return dataSource;
  }

  static void applySchemaAndTruncate(DataSource dataSource) {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        InputStream schema = PostgresSupport.class.getResourceAsStream(
            "/org/jwcarman/continuum/jdbc/continuum-postgresql.sql")) {
      statement.execute(new String(schema.readAllBytes(), StandardCharsets.UTF_8));
      statement.execute(
          "TRUNCATE continuum_outbox, continuum_result, continuum_continuation, continuum_computation");
    } catch (SQLException | IOException e) {
      throw new IllegalStateException("failed to prepare postgres schema", e);
    }
  }
}
```

`JdbcContinuumTckIT.java`:

```java
package org.jwcarman.continuum.jdbc;

import javax.sql.DataSource;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

class JdbcContinuumTckIT extends ContinuumTck {

  @Override
  protected ContinuumRepository createRepository() {
    DataSource dataSource = PostgresSupport.dataSource();
    PostgresSupport.applySchemaAndTruncate(dataSource);
    return new JdbcContinuumRepository(dataSource);
  }
}
```

- [ ] **Step 4: Run, expect failure** — `./mvnw -q -pl continuum-jdbc verify` (compile error: `JdbcContinuumRepository` missing)

- [ ] **Step 5: Implement `JdbcContinuumRepository`**

Structure: a `dataSource` field; every SPI method runs through one transaction helper; row↔object mapping helpers shared by result/outbox reads. Outcome column mapping: `SUCCESS` → `outcome_payload`; `FAILURE` → `message`; `EXPIRED` → `expiry_kind` + `message`.

```java
package org.jwcarman.continuum.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.jwcarman.continuum.CompletionDelivery;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.ContinuationId;
import org.jwcarman.continuum.ExpiryKind;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

public final class JdbcContinuumRepository implements ContinuumRepository {

  private final DataSource dataSource;

  public JdbcContinuumRepository(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
  }

  @FunctionalInterface
  private interface SqlWork<T> {
    T perform(Connection connection) throws SQLException;
  }

  private <T> T inTransaction(SqlWork<T> work) {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try {
        T result = work.perform(connection);
        connection.commit();
        return result;
      } catch (SQLException | RuntimeException e) {
        connection.rollback();
        throw e;
      }
    } catch (SQLException e) {
      throw new ContinuumPersistenceException("database operation failed", e);
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    inTransaction(connection -> {
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO continuum_computation "
              + "(id, kind, deadline_at, dispatch_payload, attempt_count, created_at, last_updated_at) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
        insert.setObject(1, computation.id().value());
        insert.setString(2, computation.kind().value());
        insert.setTimestamp(3, Timestamp.from(computation.deadline()));
        insert.setBytes(4, computation.dispatchPayload());
        insert.setInt(5, computation.attemptCount());
        insert.setTimestamp(6, Timestamp.from(computation.createdAt()));
        insert.setTimestamp(7, Timestamp.from(computation.createdAt()));
        insert.executeUpdate();
      }
      insertContinuation(connection, computation.id(), initial, computation.createdAt());
      return null;
    });
  }

  private void insertContinuation(
      Connection connection, ComputationId computationId, StoredContinuation continuation, Instant createdAt)
      throws SQLException {
    try (PreparedStatement insert = connection.prepareStatement(
        "INSERT INTO continuum_continuation (id, computation_id, payload, created_at) VALUES (?, ?, ?, ?)")) {
      insert.setObject(1, continuation.id().value());
      insert.setObject(2, computationId.value());
      insert.setBytes(3, continuation.payload());
      insert.setTimestamp(4, Timestamp.from(createdAt));
      insert.executeUpdate();
    }
  }

  @Override
  public RegistrationOutcome registerContinuation(ComputationId id, StoredContinuation continuation) {
    return inTransaction(connection -> {
      if (lockPendingRow(connection, id)) {
        insertContinuation(connection, id, continuation, Instant.now());
        return new RegistrationOutcome.Registered();
      }
      Optional<Outcome> memoized = readResultOutcome(connection, id);
      return memoized
          .<RegistrationOutcome>map(RegistrationOutcome.Resolved::new)
          .orElseGet(RegistrationOutcome.NotFound::new);
    });
  }

  private boolean lockPendingRow(Connection connection, ComputationId id) throws SQLException {
    try (PreparedStatement select = connection.prepareStatement(
        "SELECT id FROM continuum_computation WHERE id = ? FOR UPDATE")) {
      select.setObject(1, id.value());
      try (ResultSet row = select.executeQuery()) {
        return row.next();
      }
    }
  }

  @Override
  public CompletionOutcome complete(ComputationId id, Outcome outcome, Instant completedAt) {
    return inTransaction(connection -> {
      Computation pending = lockAndReadPending(connection, id);
      if (pending == null) {
        return readResultOutcome(connection, id).isPresent()
            ? CompletionOutcome.ALREADY_RESOLVED
            : CompletionOutcome.NOT_FOUND;
      }
      try (PreparedStatement insert = connection.prepareStatement(
          "INSERT INTO continuum_result "
              + "(computation_id, kind, outcome_type, outcome_payload, expiry_kind, message, "
              + " deadline_at, attempt_count, created_at, completed_at) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        insert.setObject(1, id.value());
        insert.setString(2, pending.kind().value());
        setOutcomeColumns(insert, 3, outcome); // fills outcome_type, outcome_payload, expiry_kind, message
        insert.setTimestamp(7, Timestamp.from(pending.deadline()));
        insert.setInt(8, pending.attemptCount());
        insert.setTimestamp(9, Timestamp.from(pending.createdAt()));
        insert.setTimestamp(10, Timestamp.from(completedAt));
        insert.executeUpdate();
      }
      for (StoredContinuation continuation : readContinuations(connection, id)) {
        try (PreparedStatement insert = connection.prepareStatement(
            "INSERT INTO continuum_outbox "
                + "(id, computation_id, continuation_id, kind, continuation_payload, "
                + " outcome_type, outcome_payload, expiry_kind, message, available_at, attempt_count, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)")) {
          insert.setObject(1, DeliveryId.random().value());
          insert.setObject(2, id.value());
          insert.setObject(3, continuation.id().value());
          insert.setString(4, pending.kind().value());
          insert.setBytes(5, continuation.payload());
          setOutcomeColumns(insert, 6, outcome);
          insert.setTimestamp(10, Timestamp.from(completedAt));
          insert.setTimestamp(11, Timestamp.from(completedAt));
          insert.executeUpdate();
        }
      }
      executeUpdate(connection, "DELETE FROM continuum_continuation WHERE computation_id = ?", id.value());
      executeUpdate(connection, "DELETE FROM continuum_computation WHERE id = ?", id.value());
      return CompletionOutcome.COMPLETED;
    });
  }

  // lockAndReadPending: SELECT kind, deadline_at, dispatch_payload, attempt_count, created_at
  //   FROM continuum_computation WHERE id = ? FOR UPDATE → Computation(status PENDING) or null.
  // readContinuations: SELECT id, payload FROM continuum_continuation WHERE computation_id = ?.
  // readResultOutcome: SELECT outcome_type, outcome_payload, expiry_kind, message
  //   FROM continuum_result WHERE computation_id = ? → Optional<Outcome> via readOutcome.
  // executeUpdate(connection, sql, arg): trivial helper.

  private void setOutcomeColumns(PreparedStatement statement, int firstIndex, Outcome outcome)
      throws SQLException {
    switch (outcome) {
      case Outcome.Success success -> {
        statement.setString(firstIndex, "SUCCESS");
        statement.setBytes(firstIndex + 1, success.payload());
        statement.setString(firstIndex + 2, null);
        statement.setString(firstIndex + 3, null);
      }
      case Outcome.Failure failure -> {
        statement.setString(firstIndex, "FAILURE");
        statement.setBytes(firstIndex + 1, null);
        statement.setString(firstIndex + 2, null);
        statement.setString(firstIndex + 3, failure.message());
      }
      case Outcome.Expired expired -> {
        statement.setString(firstIndex, "EXPIRED");
        statement.setBytes(firstIndex + 1, null);
        statement.setString(firstIndex + 2, expired.kind().name());
        statement.setString(firstIndex + 3, expired.message());
      }
    }
  }

  private static Outcome readOutcome(ResultSet row, String typeColumn, String payloadColumn,
      String expiryColumn, String messageColumn) throws SQLException {
    return switch (row.getString(typeColumn)) {
      case "SUCCESS" -> Outcome.success(row.getBytes(payloadColumn));
      case "FAILURE" -> Outcome.failure(row.getString(messageColumn));
      case "EXPIRED" ->
          Outcome.expired(ExpiryKind.valueOf(row.getString(expiryColumn)), row.getString(messageColumn));
      default -> throw new ContinuumPersistenceException("unknown outcome_type");
    };
  }

  @Override
  public Optional<Computation> findComputation(ComputationId id) {
    return inTransaction(connection -> {
      // 1) SELECT from continuum_computation (no lock) → map to PENDING Computation.
      // 2) else SELECT from continuum_result → Computation(id, kind, Outcome.statusOf(outcome),
      //      created_at, deadline_at, null dispatch, attempt_count, outcome).
      // 3) else Optional.empty().
      ...
    });
  }

  @Override
  public List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now) {
    return inTransaction(connection -> {
      List<ClaimedDelivery> claimed = new ArrayList<>();
      try (PreparedStatement select = connection.prepareStatement(
          "SELECT id, computation_id, continuation_id, continuation_payload, "
              + " outcome_type, outcome_payload, expiry_kind, message, attempt_count "
              + "FROM continuum_outbox "
              + "WHERE kind = ? AND available_at <= ? AND (claimed_until IS NULL OR claimed_until <= ?) "
              + "ORDER BY available_at LIMIT ? FOR UPDATE SKIP LOCKED")) {
        select.setString(1, kind.value());
        select.setTimestamp(2, Timestamp.from(now));
        select.setTimestamp(3, Timestamp.from(now));
        select.setInt(4, limit);
        try (ResultSet row = select.executeQuery()) {
          while (row.next()) {
            claimed.add(new ClaimedDelivery(
                new DeliveryId(row.getObject("id", UUID.class)),
                new CompletionDelivery(
                    new ComputationId(row.getObject("computation_id", UUID.class)),
                    kind,
                    new ContinuationId(row.getObject("continuation_id", UUID.class)),
                    row.getBytes("continuation_payload"),
                    readOutcome(row, "outcome_type", "outcome_payload", "expiry_kind", "message")),
                row.getInt("attempt_count")));
          }
        }
      }
      try (PreparedStatement update = connection.prepareStatement(
          "UPDATE continuum_outbox SET claimed_by = ?, claimed_until = ? WHERE id = ?")) {
        for (ClaimedDelivery delivery : claimed) {
          update.setString(1, workerId);
          update.setTimestamp(2, Timestamp.from(now.plus(lease)));
          update.setObject(3, delivery.id().value());
          update.addBatch();
        }
        update.executeBatch();
      }
      return claimed;
    });
  }

  // acknowledgeDelivery: DELETE FROM continuum_outbox WHERE id = ?
  // releaseDelivery:     UPDATE continuum_outbox SET claimed_by = NULL, claimed_until = NULL,
  //                        available_at = ?, attempt_count = attempt_count + 1 WHERE id = ?
  // findExpired:         SELECT ... FROM continuum_computation WHERE kind = ? AND deadline_at <= ?
  //                        ORDER BY deadline_at LIMIT ?   (no lock: reap actions are separate
  //                        first-wins transactions; duplicates are safe)
  // extendDeadline:      UPDATE continuum_computation SET deadline_at = ?, attempt_count = ?,
  //                        last_updated_at = CURRENT_TIMESTAMP WHERE id = ?
  //                        (last_updated_at is diagnostic-only; DB clock acceptable there)
  // purgeResults:        DELETE FROM continuum_result WHERE computation_id IN (
  //                        SELECT computation_id FROM continuum_result
  //                        WHERE kind = ? AND completed_at < ? LIMIT ?) → return update count
}
```

The commented method bodies marked `...`/`// method: SQL` above MUST be written out fully in the real class — they are one `PreparedStatement` each, following the exact SQL given. Timestamps: always `Timestamp.from(instant)` on write and `resultSet.getTimestamp(col).toInstant()` on read.

- [ ] **Step 6: Run the TCK against Postgres** — `./mvnw -q -pl continuum-jdbc verify`
Expected: all TCK tests (including typed-client and race batteries) PASS. Docker required.

- [ ] **Step 7: Failure-injection test (completion atomicity — spec §38)**

`OutboxFailureInjectionIT.java` — proxies the DataSource so the outbox INSERT throws mid-completion-transaction; asserts full rollback:

```java
package org.jwcarman.continuum.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.Computation;
import org.jwcarman.continuum.ComputationId;
import org.jwcarman.continuum.ComputationKind;
import org.jwcarman.continuum.ComputationStatus;
import org.jwcarman.continuum.ContinuationId;
import org.jwcarman.continuum.Outcome;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.StoredContinuation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OutboxFailureInjectionIT {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private static final ComputationKind KIND = new ComputationKind("inject");

  private final AtomicBoolean failOutboxInsert = new AtomicBoolean(false);

  private DataSource failingDataSource(DataSource delegate) {
    InvocationHandler dataSourceHandler = (proxy, method, args) -> {
      Object result = invoke(delegate, method, args);
      if ("getConnection".equals(method.getName())) {
        Connection connection = (Connection) result;
        return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Connection.class},
            (connProxy, connMethod, connArgs) -> {
              if ("prepareStatement".equals(connMethod.getName())
                  && failOutboxInsert.get()
                  && ((String) connArgs[0]).startsWith("INSERT INTO continuum_outbox")) {
                throw new SQLException("injected outbox failure");
              }
              return invoke(connection, connMethod, connArgs);
            });
      }
      return result;
    };
    return (DataSource) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {DataSource.class}, dataSourceHandler);
  }

  private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test
  void failed_outbox_insert_rolls_back_the_entire_completion() {
    DataSource real = PostgresSupport.dataSource();
    PostgresSupport.applySchemaAndTruncate(real);
    var repository = new JdbcContinuumRepository(failingDataSource(real));

    var id = ComputationId.random();
    repository.createComputation(
        new Computation(id, KIND, ComputationStatus.PENDING, NOW, NOW.plusSeconds(300), null, 1, null),
        new StoredContinuation(ContinuationId.random(), "c".getBytes(UTF_8)));

    failOutboxInsert.set(true);
    assertThatExceptionOfType(ContinuumPersistenceException.class)
        .isThrownBy(() -> repository.complete(id, Outcome.success("r".getBytes(UTF_8)), NOW.plusSeconds(1)));
    failOutboxInsert.set(false);

    // nothing committed: still pending, continuation intact, no result, no outbox rows
    var found = repository.findComputation(id).orElseThrow();
    assertThat(found.status()).isEqualTo(ComputationStatus.PENDING);
    assertThat(repository.claimDeliveries("w", KIND, 10, java.time.Duration.ofSeconds(30), NOW.plusSeconds(2)))
        .isEmpty();

    // and the computation is still completable afterwards
    assertThat(repository.complete(id, Outcome.success("r".getBytes(UTF_8)), NOW.plusSeconds(3)))
        .isEqualTo(CompletionOutcome.COMPLETED);
  }
}
```

- [ ] **Step 8: Run** — `./mvnw -q -pl continuum-jdbc verify` → PASS

- [ ] **Step 9: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: PostgreSQL provider certified against the TCK"
```

---

### Task 11: `continuum-bom` + README + CHANGELOG

**Files:**
- Create: `continuum-bom/pom.xml`
- Modify: root `pom.xml` (add module first in the list), `README.md`, `CHANGELOG.md`

- [ ] **Step 1: BOM pom** — parent as usual, artifactId `continuum-bom`, `<packaging>pom</packaging>`, name `Continuum BOM`, and a `dependencyManagement` block listing all four artifacts (`continuum-core`, `continuum-memory`, `continuum-jdbc`, `continuum-testing`) at `${project.version}`. No `<dependencies>` section.

- [ ] **Step 2: README.md** — sections: what Continuum is (the spec §40 definition: "One computation. One eventual result. Any number of durable continuations."), Maven coordinates (`org.jwcarman.continuum:continuum-core` + bom usage), a quick-start walking the full lifecycle with a `RetryableContinuumClient` (mint → create → external `complete` → scheduled `deliverResults`/`reapExpiredComputations`/`purgeExpiredResults`, mirroring the design doc §4 example verbatim), a module table, the pump/scheduling model (no threads; fixed-delay examples), and license.

- [ ] **Step 3: CHANGELOG.md** — under `## [Unreleased]`, an `### Added` list naming the core API, typed clients, memory and PostgreSQL providers, and the TCK.

- [ ] **Step 4: Run full build** — `./mvnw -q verify` → PASS

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "feat: continuum-bom, README, CHANGELOG"
```

---

### Task 12: CI workflows, license headers, final verification

**Files:**
- Create: `.github/workflows/maven.yml`, `.github/workflows/maven-publish.yml`

- [ ] **Step 1: `maven.yml`** (adapted from substrate — verbatim content):

```yaml
name: CI with Maven

on:
  push:
    branches: [ "main" ]
  pull_request:
    types:
      - opened
      - synchronize
      - reopened
    branches: [ "main" ]

permissions:
  contents: read

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout Code
      uses: actions/checkout@v6
      with:
        fetch-depth: 0

    - name: Set up JDK 25
      uses: actions/setup-java@v5
      with:
        java-version: '25'
        distribution: 'liberica'
        cache: maven

    - name: Build with Maven
      env:
        SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}
      run: mvn -Pci -B verify sonar:sonar
```

- [ ] **Step 2: `maven-publish.yml`** (adapted from substrate — verbatim content):

```yaml
name: Publish package to the Maven Central Repository
on:
  release:
    types: [created]

permissions:
  contents: read

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6

      - name: Set up Maven Central Repository
        uses: actions/setup-java@v5
        with:
          java-version: '25'
          distribution: 'liberica'
          server-id: central
          server-username: MAVEN_USERNAME
          server-password: MAVEN_PASSWORD
          gpg-private-key: ${{ secrets.GPG_SIGNING_KEY }}
          gpg-passphrase: MAVEN_GPG_PASSPHRASE

      - name: Set version
        run: mvn versions:set -DnewVersion=${{ github.event.release.tag_name }}

      - name: Publish package
        run: mvn -P release --batch-mode deploy -DskipTests
        env:
          MAVEN_USERNAME: ${{ secrets.CENTRAL_TOKEN_USERNAME }}
          MAVEN_PASSWORD: ${{ secrets.CENTRAL_TOKEN_PASSWORD }}
          MAVEN_GPG_PASSPHRASE: ${{ secrets.GPG_SIGNING_KEY_PASSWORD }}
```

- [ ] **Step 3: License headers** — `./mvnw -Plicense license:format` then `./mvnw -Plicense license:check` → PASS.

- [ ] **Step 4: Full gate** — `./mvnw -q -Plicense verify` → BUILD SUCCESS (all modules, unit + integration tests, spotless, headers).

- [ ] **Step 5: Format and commit**

```bash
./mvnw -q spotless:apply
git add -A && git commit -m "chore: CI workflows and license headers"
```

---

## Plan Self-Review Notes

- **Spec coverage:** design §1 → Tasks 1, 11, 12; §2 → Tasks 5, 10; §3 → Tasks 2–4; §4 → Tasks 7–9; §5 (SPI) → Task 3; §6 → Task 5; §7 → Task 10; §8 (TCK) → Tasks 6, 9, 10 (failure injection in Task 10 Step 7). Spec §38's "create crash" case is covered by Task 10's transaction helper (rollback on any failure) plus the completion-injection IT; a create-time injection variant may be added in the jdbc module if desired, following the same proxy pattern.
- **Type consistency:** all signatures flow from the Interfaces blocks of Tasks 2, 3, 7, 8; later tasks reference only those names. `Expired` outcomes are written exclusively through `ContinuumRepository.complete` (never `Continuum.complete`, which rejects them).
- **Known judgment calls an executor may hit:** dependency versions in Task 1 are best-known pins — if resolution fails, bump to the latest release of the same major; `records`-with-byte[] equality is only customized where tests compare values (`Outcome.Success`); `last_updated_at` uses the DB clock (diagnostic-only column).
