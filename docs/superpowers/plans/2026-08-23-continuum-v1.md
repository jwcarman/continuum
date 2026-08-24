# Continuum v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Continuum v1 — a Java library for durable asynchronous computation coordination (create → pending → complete → outbox delivery) with pluggable persistence.

**Architecture:** Presence-means-pending data model: a computation row exists only while pending; completion atomically deletes it, writes a memoized result row, and fans out outbox deliveries. A byte[]-based `Continuum` core + `ContinuumRepository` SPI, with a typed `ContinuumClient<R,C,D>` DSL layered on top via `org.jwcarman.codec`. App-pumped (no threads): `DeliveryPump` and `TimeoutReaper`.

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
