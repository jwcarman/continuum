# MongoDB Persistence (`continuum-mongo`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `org.jwcarman.continuum:continuum-mongo` — a TCK-certified MongoDB implementation of `ContinuumRepository` with a topology guard, an index helper, and Spring Boot auto-configuration — in 0.5.0.

**Architecture:** Four collections mirror the JDBC tables. Every multi-document operation runs in `ClientSession.withTransaction` (replica set required); the outbox claim is a per-document `findOneAndUpdate` compare-and-set that plays the role `SKIP LOCKED` plays in SQL. A guard on first use refuses topologies that cannot meet the TCK. Boot selection follows the Spring Session `store-type` precedent: `continuum.persistence.type`, auto-detected when absent, a hard failure when ambiguous.

**Tech Stack:** Java 21, Maven reactor, `org.mongodb:mongodb-driver-sync` 5.3.1, Testcontainers `mongodb` (`mongo:8.2`), JUnit 5 + AssertJ + Mockito, Spring Boot 4.0.x auto-configuration, `ContinuumTck` from `continuum-testing`.

**Spec:** `docs/superpowers/specs/2026-08-25-mongo-persistence-design.md`

## Global Constraints

- Never suppress warnings (`@SuppressWarnings` etc. are banned); no star imports; explicit single-symbol imports everywhere.
- Every source file carries the Apache-2.0 header (copy it from any existing `.java`/`pom.xml`; `mvn -P license verify` checks it).
- `mvn spotless:apply` before every commit; the `ci` profile's dependency analyzer fails on unused/undeclared dependencies — declare exactly what you import, exempt with an inline reason only when a dependency is used reflectively.
- Repository constructor and `MongoContinuumRepository.assumeMongoDb` open no connection; detection happens on first operation.
- `java.util.Date` appears nowhere in `continuum-mongo`.
- Instants are BSON `date` (millisecond precision). Timestamp vocabulary: `submittedAt`, `completedAt`, `createdAt` (row bookkeeping only), `lastUpdatedAt`.
- Identities are canonical 36-character UUID strings in `_id`.
- MongoDB floor 5.0; standalone servers refused; DocumentDB, Cosmos DB, FerretDB refused by name; `mongo:8.2` in tests (never `mongo:8.0` — it refuses to start on Linux ≥ 6.19, SERVER-121912).
- Commit trailer on every commit:
  ```
  Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi
  ```
- Test naming: `*Test` = unit (surefire, every build); `*IT` = container-backed (failsafe, every build). Class-level `@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)`, snake_case method names, `@Nested` groups.
- Full gate before pushing: `mvn clean && mvn -P ci,license verify` then `mvn -P release javadoc:jar -DskipTests` (zero warnings).

---

## File structure

| File | Responsibility |
|---|---|
| `continuum-mongo/pom.xml` | module: core + driver (compile); testing + testcontainers-mongodb (test) |
| `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/package-info.java` | package javadoc |
| `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/MongoContinuumRepository.java` | the ten SPI operations, `ensureIndexes()`, constructors, escape hatch |
| `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/Documents.java` | package-private: field-name constants, `Instant`-aware codec registry, outcome ↔ document mapping, UUID ↔ string |
| `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/TopologyGuard.java` | package-private: `hello`/`buildInfo` detection and the refusal messages |
| `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/MongoContinuumTckIT.java` | full TCK on `mongo:8.2` via the detecting constructor |
| `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/EnsureIndexesIT.java` | index creation and idempotence against the container |
| `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/TopologyGuardTest.java` | mocked-command guard tests (permit / refuse / escape hatch / no connection at construction) |
| `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/PersistenceType.java` | `JDBC`, `MONGO`, `MEMORY` |
| `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/OnPersistenceTypeCondition.java` | the selection rule (property, auto-detect, ambiguity failure) |
| `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/ConditionalOnPersistenceType.java` | annotation over the condition |
| `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/MongoContinuumAutoConfiguration.java` | `MongoContinuumRepository` bean, `continuum.mongo.*` properties |
| `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/MongoContinuumProperties.java` | `continuum.mongo.database`, `continuum.mongo.ensure-indexes` |
| `continuum-autoconfigure/src/test/java/org/jwcarman/continuum/autoconfigure/MongoContinuumAutoConfigurationTest.java` | selection matrix via `ApplicationContextRunner` |
| `docs/guides/persistence.md`, `docs/guides/spring-boot.md`, `README.md`, `CHANGELOG.md` | documentation |

---

### Task 1: Module scaffold and the TCK-certified repository

The spike proved the design; this task turns it into the real class, test-first against the TCK.

**Files:**
- Create: `continuum-mongo/pom.xml`
- Create: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/package-info.java`
- Create: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/Documents.java`
- Create: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/MongoContinuumRepository.java`
- Create: `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/MongoContinuumTckIT.java`
- Modify: `pom.xml` — `<modules>` (after `continuum-jdbc`), `<properties>` (`mongodb.version`), `<dependencyManagement>` (driver + `continuum-mongo`)
- Modify: `continuum-bom/pom.xml` — `continuum-mongo` entry after `continuum-jdbc`

**Interfaces:**
- Produces: `public final class MongoContinuumRepository implements ContinuumRepository` with `public MongoContinuumRepository(MongoClient client, String databaseName)` and, for this task only, the private constructor shape that Task 3 extends with the guard.
- Produces: `Documents` constants used by Tasks 2 and 3: `Documents.COMPUTATIONS`, `Documents.CONTINUATIONS`, `Documents.RESULTS`, `Documents.OUTBOX` (collection names), field constants listed below, `Documents.codecRegistry()`.

- [ ] **Step 1: Create the module pom**

`continuum-mongo/pom.xml` (header block copied verbatim from `continuum-jdbc/pom.xml`):

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.jwcarman.continuum</groupId>
        <artifactId>continuum-parent</artifactId>
        <version>0.5.0-SNAPSHOT</version>
    </parent>
    <artifactId>continuum-mongo</artifactId>
    <name>Continuum MongoDB</name>
    <description>MongoDB Continuum persistence</description>
    <dependencies>
        <dependency>
            <groupId>org.jwcarman.continuum</groupId>
            <artifactId>continuum-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>bson</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jwcarman.continuum</groupId>
            <artifactId>continuum-testing</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mongodb</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

`bson` and `mongodb-driver-core` are declared because the code imports `org.bson.*` and `com.mongodb.client.model.*`/`com.mongodb.MongoClientSettings` directly; the analyzer would otherwise flag them as used-undeclared.

- [ ] **Step 2: Register the module, the driver version and the BOM entry**

In `pom.xml`:

```xml
<!-- <modules>, after continuum-jdbc -->
        <module>continuum-mongo</module>
```

```xml
<!-- <properties>, after postgresql.version -->
        <mongodb.version>5.3.1</mongodb.version>
```

```xml
<!-- <dependencyManagement>, after the continuum-jdbc entry -->
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-mongo</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>org.mongodb</groupId>
                <artifactId>mongodb-driver-sync</artifactId>
                <version>${mongodb.version}</version>
            </dependency>
            <dependency>
                <groupId>org.mongodb</groupId>
                <artifactId>mongodb-driver-core</artifactId>
                <version>${mongodb.version}</version>
            </dependency>
            <dependency>
                <groupId>org.mongodb</groupId>
                <artifactId>bson</artifactId>
                <version>${mongodb.version}</version>
            </dependency>
```

In `continuum-bom/pom.xml`, after the `continuum-jdbc` entry:

```xml
            <dependency>
                <groupId>org.jwcarman.continuum</groupId>
                <artifactId>continuum-mongo</artifactId>
                <version>${project.version}</version>
            </dependency>
```

- [ ] **Step 3: Write the failing TCK suite**

`continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/MongoContinuumTckIT.java`:

```java
package org.jwcarman.continuum.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;
import org.testcontainers.containers.MongoDBContainer;

/**
 * Certifies MongoDB through the ordinary detecting constructor, so the guard's admission of a
 * genuine replica set is itself under test on every build. {@code MongoDBContainer} initiates a
 * single-node replica set, which is all transactions need. The image is 8.2, not 8.0: 8.0 refuses
 * to start on Linux kernels 6.19+ (SERVER-121912), which Docker Desktop currently ships.
 */
class MongoContinuumTckIT extends ContinuumTck {

  static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.2");
  static final MongoClient CLIENT;
  static final String DATABASE = "continuum";

  static {
    MONGO.start();
    CLIENT = MongoClients.create(MONGO.getConnectionString());
  }

  @Override
  protected ContinuumRepository createRepository() {
    CLIENT.getDatabase(DATABASE).drop();
    return new MongoContinuumRepository(CLIENT, DATABASE);
  }
}
```

- [ ] **Step 4: Run it to verify it fails**

Run: `mvn -q -pl continuum-mongo -am install -DskipTests` then `mvn -pl continuum-mongo verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure — `MongoContinuumRepository` does not exist.

- [ ] **Step 5: Write `Documents`**

`continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/Documents.java`:

```java
package org.jwcarman.continuum.mongo;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.MongoClientSettings;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Binary;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * The document vocabulary: collection and field names, the codec registry that keeps {@code
 * java.util.Date} out of this module, and the value mappings every operation shares.
 */
final class Documents {

  static final String COMPUTATIONS = "continuum_computation";
  static final String CONTINUATIONS = "continuum_continuation";
  static final String RESULTS = "continuum_result";
  static final String OUTBOX = "continuum_outbox";

  static final String ID = "_id";
  static final String KIND = "kind";
  static final String COMPUTATION_ID = "computationId";
  static final String CONTINUATION_ID = "continuationId";
  static final String DEADLINE_AT = "deadlineAt";
  static final String DISPATCH_PAYLOAD = "dispatchPayload";
  static final String ATTEMPT_COUNT = "attemptCount";
  static final String SUBMITTED_AT = "submittedAt";
  static final String COMPLETED_AT = "completedAt";
  static final String LAST_UPDATED_AT = "lastUpdatedAt";
  static final String CREATED_AT = "createdAt";
  static final String PAYLOAD = "payload";
  static final String CONTINUATION_PAYLOAD = "continuationPayload";
  static final String OUTCOME = "outcome";
  static final String AVAILABLE_AT = "availableAt";
  static final String CLAIMED_BY = "claimedBy";
  static final String CLAIMED_UNTIL = "claimedUntil";

  static final String OUTCOME_TYPE = "type";
  static final String OUTCOME_PAYLOAD = "payload";
  static final String OUTCOME_EXPIRY_KIND = "expiryKind";
  static final String OUTCOME_MESSAGE = "message";

  private static final String SUCCESS = "SUCCESS";
  private static final String FAILURE = "FAILURE";
  private static final String EXPIRED = "EXPIRED";

  private Documents() {}

  /**
   * The driver's default registry already encodes {@link Instant}; this adds the decoding side by
   * mapping BSON {@code date} to {@link Instant} when a {@link Document} is read, so no operation
   * ever sees a {@code java.util.Date}.
   */
  static CodecRegistry codecRegistry() {
    BsonTypeClassMap instants = new BsonTypeClassMap(Map.of(BsonType.DATE_TIME, Instant.class));
    return fromRegistries(
        fromProviders(new DocumentCodecProvider(instants)),
        MongoClientSettings.getDefaultCodecRegistry());
  }

  static Document outcomeDocument(Outcome outcome) {
    return switch (outcome) {
      case Outcome.Success(byte[] payload) ->
          new Document(OUTCOME_TYPE, SUCCESS).append(OUTCOME_PAYLOAD, new Binary(payload));
      case Outcome.Failure(String message) ->
          new Document(OUTCOME_TYPE, FAILURE).append(OUTCOME_MESSAGE, message);
      case Outcome.Expired(ExpiryKind expiryKind, String message) ->
          new Document(OUTCOME_TYPE, EXPIRED)
              .append(OUTCOME_EXPIRY_KIND, expiryKind.name())
              .append(OUTCOME_MESSAGE, message);
    };
  }

  static Outcome readOutcome(Document document) {
    return switch (document.getString(OUTCOME_TYPE)) {
      case SUCCESS -> Outcome.success(bytes(document.get(OUTCOME_PAYLOAD, Binary.class)));
      case FAILURE -> Outcome.failure(document.getString(OUTCOME_MESSAGE));
      case EXPIRED ->
          Outcome.expired(
              ExpiryKind.valueOf(document.getString(OUTCOME_EXPIRY_KIND)),
              document.getString(OUTCOME_MESSAGE));
      default ->
          throw new ContinuumPersistenceException(
              "unknown outcome type: " + document.getString(OUTCOME_TYPE));
    };
  }

  static String id(UUID uuid) {
    return uuid.toString();
  }

  static UUID uuid(String id) {
    return UUID.fromString(id);
  }

  static Binary binary(byte[] bytes) {
    return bytes == null ? null : new Binary(bytes);
  }

  static byte[] bytes(Binary binary) {
    return binary == null ? null : binary.getData();
  }
}
```

- [ ] **Step 6: Write `MongoContinuumRepository`**

`continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/MongoContinuumRepository.java`:

```java
package org.jwcarman.continuum.mongo;

import static org.jwcarman.continuum.mongo.Documents.ATTEMPT_COUNT;
import static org.jwcarman.continuum.mongo.Documents.AVAILABLE_AT;
import static org.jwcarman.continuum.mongo.Documents.CLAIMED_BY;
import static org.jwcarman.continuum.mongo.Documents.CLAIMED_UNTIL;
import static org.jwcarman.continuum.mongo.Documents.COMPLETED_AT;
import static org.jwcarman.continuum.mongo.Documents.COMPUTATIONS;
import static org.jwcarman.continuum.mongo.Documents.COMPUTATION_ID;
import static org.jwcarman.continuum.mongo.Documents.CONTINUATIONS;
import static org.jwcarman.continuum.mongo.Documents.CONTINUATION_ID;
import static org.jwcarman.continuum.mongo.Documents.CONTINUATION_PAYLOAD;
import static org.jwcarman.continuum.mongo.Documents.CREATED_AT;
import static org.jwcarman.continuum.mongo.Documents.DEADLINE_AT;
import static org.jwcarman.continuum.mongo.Documents.DISPATCH_PAYLOAD;
import static org.jwcarman.continuum.mongo.Documents.ID;
import static org.jwcarman.continuum.mongo.Documents.KIND;
import static org.jwcarman.continuum.mongo.Documents.LAST_UPDATED_AT;
import static org.jwcarman.continuum.mongo.Documents.OUTBOX;
import static org.jwcarman.continuum.mongo.Documents.OUTCOME;
import static org.jwcarman.continuum.mongo.Documents.PAYLOAD;
import static org.jwcarman.continuum.mongo.Documents.RESULTS;
import static org.jwcarman.continuum.mongo.Documents.SUBMITTED_AT;
import static org.jwcarman.continuum.mongo.Documents.binary;
import static org.jwcarman.continuum.mongo.Documents.bytes;
import static org.jwcarman.continuum.mongo.Documents.id;
import static org.jwcarman.continuum.mongo.Documents.outcomeDocument;
import static org.jwcarman.continuum.mongo.Documents.readOutcome;
import static org.jwcarman.continuum.mongo.Documents.uuid;

import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.jwcarman.continuum.api.CompletionDelivery;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationStatus;
import org.jwcarman.continuum.api.ContinuationId;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.CompletionOutcome;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.spi.DeliveryId;
import org.jwcarman.continuum.spi.RegistrationOutcome;
import org.jwcarman.continuum.spi.StoredContinuation;

/**
 * MongoDB persistence over a plain {@link MongoClient}, certified on MongoDB 5.0+ replica sets —
 * the full TCK battery, concurrency suites included, on every build.
 *
 * <p>Four collections mirror the JDBC tables. Every operation that touches more than one document
 * runs in a transaction, which is why a replica set is required: standalone servers have no
 * multi-document transactions, and the ownership transfer in {@link #complete} must be atomic or a
 * crash between the delete and the outbox insert loses deliveries silently. The outbox claim needs
 * no locking clause at all — each claim is a single-document compare-and-set on {@code
 * claimedUntil}, so competing claimers never block and never double-claim.
 *
 * <p>Instants are stored as BSON {@code date}, millisecond precision. Identities are canonical UUID
 * strings, so UUIDv7 time-ordering survives.
 */
public final class MongoContinuumRepository implements ContinuumRepository {

  private static final int DUPLICATE_KEY = 11000;

  private final MongoClient client;
  private final MongoCollection<Document> computations;
  private final MongoCollection<Document> continuations;
  private final MongoCollection<Document> results;
  private final MongoCollection<Document> outbox;

  /**
   * Creates a repository over the named database. Opens no connection; the topology is verified on
   * first use.
   *
   * @param client the application's client — it owns pooling and credentials
   * @param databaseName the database holding the four continuum collections
   */
  public MongoContinuumRepository(MongoClient client, String databaseName) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    MongoDatabase database =
        client
            .getDatabase(Objects.requireNonNull(databaseName, "databaseName must not be null"))
            .withCodecRegistry(Documents.codecRegistry());
    this.computations = database.getCollection(COMPUTATIONS);
    this.continuations = database.getCollection(CONTINUATIONS);
    this.results = database.getCollection(RESULTS);
    this.outbox = database.getCollection(OUTBOX);
  }

  private <T> T inTransaction(Function<ClientSession, T> body) {
    try (ClientSession session = client.startSession()) {
      return session.withTransaction(() -> body.apply(session));
    }
  }

  @Override
  public void createComputation(Computation computation, StoredContinuation initial) {
    String id = id(computation.id().value());
    try {
      inTransaction(
          session -> {
            if (results.find(session, Filters.eq(ID, id)).first() != null) {
              throw new ContinuumPersistenceException("duplicate computation id: " + id);
            }
            computations.insertOne(
                session,
                new Document(ID, id)
                    .append(KIND, computation.kind().value())
                    .append(DEADLINE_AT, computation.deadline())
                    .append(DISPATCH_PAYLOAD, binary(computation.dispatchPayload()))
                    .append(ATTEMPT_COUNT, computation.attemptCount())
                    .append(SUBMITTED_AT, computation.submittedAt())
                    .append(LAST_UPDATED_AT, computation.submittedAt()));
            continuations.insertOne(
                session, continuationDocument(id, initial, computation.submittedAt()));
            return null;
          });
    } catch (MongoWriteException e) {
      if (e.getError().getCode() == DUPLICATE_KEY) {
        throw new ContinuumPersistenceException("duplicate computation id: " + id, e);
      }
      throw new ContinuumPersistenceException("createComputation failed", e);
    }
  }

  @Override
  public RegistrationOutcome registerContinuation(
      ComputationId computationId, StoredContinuation continuation) {
    String id = id(computationId.value());
    return inTransaction(
        session -> {
          // The pending-row lock, Mongo-style: updating the pending document takes a write
          // intent on it, so a concurrent complete() deleting it conflicts and one side
          // retries. Nothing else about the update matters.
          Document pending =
              computations.findOneAndUpdate(
                  session, Filters.eq(ID, id), Updates.currentDate(LAST_UPDATED_AT));
          if (pending != null) {
            // createdAt is bookkeeping, derived from the SPI's instants like JDBC does — the
            // repository never consults a clock of its own.
            continuations.insertOne(
                session,
                continuationDocument(id, continuation, pending.get(SUBMITTED_AT, Instant.class)));
            return new RegistrationOutcome.Registered();
          }
          Document terminal = results.find(session, Filters.eq(ID, id)).first();
          if (terminal != null) {
            return new RegistrationOutcome.Resolved(
                readOutcome(terminal.get(OUTCOME, Document.class)));
          }
          return new RegistrationOutcome.NotFound();
        });
  }

  @Override
  public CompletionOutcome complete(
      ComputationId computationId, Outcome outcome, Instant completedAt) {
    String id = id(computationId.value());
    return inTransaction(
        session -> {
          Document pending = computations.findOneAndDelete(session, Filters.eq(ID, id));
          if (pending == null) {
            return results.find(session, Filters.eq(ID, id)).first() != null
                ? CompletionOutcome.ALREADY_RESOLVED
                : CompletionOutcome.NOT_FOUND;
          }
          Document outcomeDocument = outcomeDocument(outcome);
          Instant submittedAt = pending.get(SUBMITTED_AT, Instant.class);
          results.insertOne(
              session,
              new Document(ID, id)
                  .append(KIND, pending.getString(KIND))
                  .append(OUTCOME, outcomeDocument)
                  .append(DEADLINE_AT, pending.get(DEADLINE_AT, Instant.class))
                  .append(ATTEMPT_COUNT, pending.getInteger(ATTEMPT_COUNT))
                  .append(SUBMITTED_AT, submittedAt)
                  .append(COMPLETED_AT, completedAt));
          List<Document> deliveries = new ArrayList<>();
          for (Document continuation :
              continuations.find(session, Filters.eq(COMPUTATION_ID, id))) {
            deliveries.add(
                new Document(ID, id(DeliveryId.random().value()))
                    .append(COMPUTATION_ID, id)
                    .append(CONTINUATION_ID, continuation.getString(ID))
                    .append(KIND, pending.getString(KIND))
                    .append(CONTINUATION_PAYLOAD, continuation.get(PAYLOAD, Binary.class))
                    .append(OUTCOME, outcomeDocument)
                    .append(AVAILABLE_AT, completedAt)
                    .append(CLAIMED_BY, null)
                    .append(CLAIMED_UNTIL, null)
                    .append(ATTEMPT_COUNT, 0)
                    .append(CREATED_AT, completedAt)
                    .append(SUBMITTED_AT, submittedAt)
                    .append(COMPLETED_AT, completedAt));
          }
          if (!deliveries.isEmpty()) {
            outbox.insertMany(session, deliveries);
          }
          continuations.deleteMany(session, Filters.eq(COMPUTATION_ID, id));
          return CompletionOutcome.COMPLETED;
        });
  }

  @Override
  public Optional<Computation> findComputation(ComputationId computationId) {
    String id = id(computationId.value());
    Document pending = computations.find(Filters.eq(ID, id)).first();
    if (pending != null) {
      return Optional.of(pendingComputation(pending));
    }
    Document terminal = results.find(Filters.eq(ID, id)).first();
    if (terminal == null) {
      return Optional.empty();
    }
    Outcome outcome = readOutcome(terminal.get(OUTCOME, Document.class));
    return Optional.of(
        new Computation(
            computationId,
            new ComputationKind(terminal.getString(KIND)),
            Outcome.statusOf(outcome),
            terminal.get(SUBMITTED_AT, Instant.class),
            terminal.get(DEADLINE_AT, Instant.class),
            null,
            terminal.getInteger(ATTEMPT_COUNT),
            outcome));
  }

  @Override
  public List<ClaimedDelivery> claimDeliveries(
      String workerId, ComputationKind kind, int limit, Duration lease, Instant now) {
    Bson claimable =
        Filters.and(
            Filters.eq(KIND, kind.value()),
            Filters.lte(AVAILABLE_AT, now),
            Filters.or(Filters.eq(CLAIMED_UNTIL, null), Filters.lte(CLAIMED_UNTIL, now)));
    Bson claim =
        Updates.combine(
            Updates.set(CLAIMED_BY, workerId), Updates.set(CLAIMED_UNTIL, now.plus(lease)));
    FindOneAndUpdateOptions oldestFirst =
        new FindOneAndUpdateOptions()
            .sort(Sorts.ascending(AVAILABLE_AT))
            .returnDocument(ReturnDocument.AFTER);
    List<ClaimedDelivery> claimed = new ArrayList<>();
    while (claimed.size() < limit) {
      // One document per round trip, each a compare-and-set: the first claimer whose update
      // matches owns the document, and the filter no longer matches for everyone else.
      Document row = outbox.findOneAndUpdate(claimable, claim, oldestFirst);
      if (row == null) {
        break;
      }
      claimed.add(
          new ClaimedDelivery(
              new DeliveryId(uuid(row.getString(ID))),
              new CompletionDelivery(
                  new ComputationId(uuid(row.getString(COMPUTATION_ID))),
                  new ComputationKind(row.getString(KIND)),
                  new ContinuationId(uuid(row.getString(CONTINUATION_ID))),
                  bytes(row.get(CONTINUATION_PAYLOAD, Binary.class)),
                  readOutcome(row.get(OUTCOME, Document.class)),
                  row.get(SUBMITTED_AT, Instant.class),
                  row.get(COMPLETED_AT, Instant.class)),
              row.getInteger(ATTEMPT_COUNT)));
    }
    return claimed;
  }

  @Override
  public void acknowledgeDelivery(DeliveryId deliveryId) {
    outbox.deleteOne(Filters.eq(ID, id(deliveryId.value())));
  }

  @Override
  public void releaseDelivery(DeliveryId deliveryId, Instant retryAt) {
    outbox.updateOne(
        Filters.eq(ID, id(deliveryId.value())),
        Updates.combine(
            Updates.set(CLAIMED_BY, null),
            Updates.set(CLAIMED_UNTIL, null),
            Updates.set(AVAILABLE_AT, retryAt),
            Updates.inc(ATTEMPT_COUNT, 1)));
  }

  @Override
  public List<Computation> findExpired(ComputationKind kind, Instant now, int limit) {
    List<Computation> expired = new ArrayList<>();
    computations
        .find(Filters.and(Filters.eq(KIND, kind.value()), Filters.lte(DEADLINE_AT, now)))
        .sort(Sorts.ascending(DEADLINE_AT))
        .limit(limit)
        .forEach(row -> expired.add(pendingComputation(row)));
    return expired;
  }

  @Override
  public void extendDeadline(ComputationId computationId, Instant newDeadline, int attemptCount) {
    computations.updateOne(
        Filters.eq(ID, id(computationId.value())),
        Updates.combine(
            Updates.set(DEADLINE_AT, newDeadline),
            Updates.set(ATTEMPT_COUNT, attemptCount),
            Updates.currentDate(LAST_UPDATED_AT)));
  }

  @Override
  public int purgeResults(ComputationKind kind, Instant olderThan, int limit) {
    List<String> ids = new ArrayList<>();
    results
        .find(Filters.and(Filters.eq(KIND, kind.value()), Filters.lt(COMPLETED_AT, olderThan)))
        .sort(Sorts.ascending(COMPLETED_AT))
        .limit(limit)
        .projection(new Document(ID, 1))
        .forEach(row -> ids.add(row.getString(ID)));
    if (ids.isEmpty()) {
      return 0;
    }
    return (int) results.deleteMany(Filters.in(ID, ids)).getDeletedCount();
  }

  private static Document continuationDocument(
      String computationId, StoredContinuation continuation, Instant createdAt) {
    return new Document(ID, id(continuation.id().value()))
        .append(COMPUTATION_ID, computationId)
        .append(PAYLOAD, new Binary(continuation.payload()))
        .append(CREATED_AT, createdAt);
  }

  private static Computation pendingComputation(Document row) {
    return new Computation(
        new ComputationId(uuid(row.getString(ID))),
        new ComputationKind(row.getString(KIND)),
        ComputationStatus.PENDING,
        row.get(SUBMITTED_AT, Instant.class),
        row.get(DEADLINE_AT, Instant.class),
        bytes(row.get(DISPATCH_PAYLOAD, Binary.class)),
        row.getInteger(ATTEMPT_COUNT),
        null);
  }
}
```

- [ ] **Step 7: Write `package-info.java`**

```java
/**
 * MongoDB persistence for Continuum: {@link org.jwcarman.continuum.mongo.MongoContinuumRepository}
 * over a plain {@code MongoClient}, certified on MongoDB 5.0+ replica sets.
 */
package org.jwcarman.continuum.mongo;
```

- [ ] **Step 8: Run the TCK to verify it passes**

Run: `mvn spotless:apply -q && mvn -q -pl continuum-mongo -am install -DskipTests && mvn -pl continuum-mongo verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `MongoContinuumTckIT` — 26 tests, 0 failures, 0 errors (7 Typed_clients, 2 Purging, 2 Expiry, 4 Claiming, 2 Racing, 4 Registration, 5 Lifecycle). BUILD SUCCESS.

- [ ] **Step 9: Run the analyzer and license check on the module**

Run: `mvn -P ci,license -pl continuum-mongo verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS with no "Used undeclared" / "Unused declared" lines. If `bson` or `mongodb-driver-core` is reported unused, remove that declaration; if reported used-undeclared, it stays.

- [ ] **Step 10: Commit**

```bash
git add pom.xml continuum-bom/pom.xml continuum-mongo
git commit -m "feat(mongo): MongoContinuumRepository, certified by the TCK on mongo:8.2

Four collections mirror the JDBC tables; every multi-document operation is a
transaction; the outbox claim is a per-document findOneAndUpdate compare-and-set
in place of SKIP LOCKED. Instants are BSON date read back as Instant through a
BsonTypeClassMap — java.util.Date appears nowhere.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi"
```

---

### Task 2: `ensureIndexes()`

**Files:**
- Modify: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/MongoContinuumRepository.java`
- Create: `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/EnsureIndexesIT.java`

**Interfaces:**
- Produces: `public void ensureIndexes()` on `MongoContinuumRepository` — idempotent, safe to call on every startup; Task 4's auto-configuration calls it.

- [ ] **Step 1: Write the failing test**

```java
package org.jwcarman.continuum.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EnsureIndexesIT {

  private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.2");
  private static final MongoClient CLIENT;
  private static final String DATABASE = "continuum_indexes";

  static {
    MONGO.start();
    CLIENT = MongoClients.create(MONGO.getConnectionString());
  }

  private MongoDatabase database;
  private MongoContinuumRepository repository;

  @BeforeEach
  void setUp() {
    database = CLIENT.getDatabase(DATABASE);
    database.drop();
    repository = new MongoContinuumRepository(CLIENT, DATABASE);
  }

  private List<String> indexNames(String collection) {
    List<String> names = new ArrayList<>();
    for (Document index : database.getCollection(collection).listIndexes()) {
      names.add(index.getString("name"));
    }
    return names;
  }

  @Test
  void creates_the_four_query_indexes() {
    repository.ensureIndexes();

    assertThat(indexNames(Documents.COMPUTATIONS)).contains("kind_1_deadlineAt_1");
    assertThat(indexNames(Documents.CONTINUATIONS)).contains("computationId_1");
    assertThat(indexNames(Documents.RESULTS)).contains("kind_1_completedAt_1");
    assertThat(indexNames(Documents.OUTBOX)).contains("kind_1_availableAt_1");
  }

  @Test
  void is_idempotent() {
    repository.ensureIndexes();

    assertThatCode(repository::ensureIndexes).doesNotThrowAnyException();
    // _id plus exactly one query index per collection, however many times it runs
    assertThat(indexNames(Documents.OUTBOX)).hasSize(2);
  }

  @Test
  void construction_creates_nothing() {
    assertThat(database.listCollectionNames()).isEmpty();
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `mvn -pl continuum-mongo verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=EnsureIndexesIT`
Expected: compilation failure — `ensureIndexes()` undefined.

- [ ] **Step 3: Implement**

Add to `MongoContinuumRepository` (imports: `com.mongodb.client.model.Indexes`):

```java
  /**
   * Creates the four indexes the query paths rely on, if they do not already exist. Idempotent and
   * cheap — safe on every startup. Never called by the repository itself: like the JDBC schema,
   * indexes are the application's to manage, and this is the helper for doing so. The Spring Boot
   * auto-configuration calls it unless {@code continuum.mongo.ensure-indexes=false}.
   */
  public void ensureIndexes() {
    computations.createIndex(Indexes.ascending(KIND, DEADLINE_AT));
    continuations.createIndex(Indexes.ascending(COMPUTATION_ID));
    results.createIndex(Indexes.ascending(KIND, COMPLETED_AT));
    outbox.createIndex(Indexes.ascending(KIND, AVAILABLE_AT));
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn spotless:apply -q && mvn -pl continuum-mongo verify -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `EnsureIndexesIT` 3/3 and `MongoContinuumTckIT` 26/26 pass.

- [ ] **Step 5: Commit**

```bash
git add continuum-mongo
git commit -m "feat(mongo): ensureIndexes() — idempotent creation of the four query indexes

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi"
```

---

### Task 3: Topology guard

**Files:**
- Create: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/TopologyGuard.java`
- Modify: `continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/MongoContinuumRepository.java`
- Create: `continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/TopologyGuardTest.java`
- Modify: `continuum-mongo/pom.xml` — add `mockito-core` (test). Check first whether the parent already declares Mockito for every module (`pom.xml` `<dependencies>` block, around line 240: `org.mockito:mockito-core` with test scope). It does — no change needed.

**Interfaces:**
- Consumes: `MongoContinuumRepository(MongoClient, String)` from Task 1.
- Produces: `public static MongoContinuumRepository assumeMongoDb(MongoClient client, String databaseName)`; package-private `TopologyGuard.verify(MongoDatabase)` throwing `ContinuumPersistenceException`.

Detection signals, in the order they are checked:

| Signal | Source | Verdict |
|---|---|---|
| `buildInfo.ferretdb` present | `buildInfo` | FerretDB — refused |
| any host in `hello.hosts` / `hello.me` ends with `.docdb.amazonaws.com` or `.docdb-elastic.amazonaws.com` | `hello` | Amazon DocumentDB — refused |
| any host ends with `.cosmos.azure.com` | `hello` | Azure Cosmos DB — refused |
| `buildInfo.version` major < 5 | `buildInfo` | too old — refused |
| `hello.msg == "isdbgrid"` | `hello` | mongos — permitted |
| `hello.setName` present | `hello` | replica set — permitted |
| otherwise | | standalone — refused |

The impostor hostnames are the best signal available: DocumentDB and Cosmos report genuine-looking `buildInfo` versions. False negatives fall through to the version/topology checks and, at worst, to the escape hatch.

- [ ] **Step 1: Write the failing tests**

`continuum-mongo/src/test/java/org/jwcarman/continuum/mongo/TopologyGuardTest.java`:

```java
package org.jwcarman.continuum.mongo;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * The guard turns "the first complete() fails with an opaque driver error" into a refusal on first
 * use that names what was found and how to fix it. These tests script the {@code buildInfo} and
 * {@code hello} replies the way real servers were observed to answer.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TopologyGuardTest {

  private static final Document EMPTY_HELLO = new Document("ok", 1.0);

  /** A client whose database answers buildInfo/hello as scripted and returns no documents. */
  private static MongoClient server(Document buildInfo, Document hello) {
    MongoClient client = mock();
    MongoDatabase database = mock();
    MongoCollection<Document> collection = mock();
    FindIterable<Document> nothing = mock();
    when(client.getDatabase(anyString())).thenReturn(database);
    when(database.withCodecRegistry(any())).thenReturn(database);
    when(database.getCollection(anyString())).thenReturn(collection);
    when(database.runCommand(new Document("buildInfo", 1))).thenReturn(buildInfo);
    when(database.runCommand(new Document("hello", 1))).thenReturn(hello);
    when(collection.find(any(Bson.class))).thenReturn(nothing);
    when(nothing.first()).thenReturn(null);
    return client;
  }

  private static Document buildInfo(String version) {
    return new Document("version", version).append("ok", 1.0);
  }

  private static Document replicaSet(String... hosts) {
    return new Document("setName", "rs0")
        .append("hosts", List.of(hosts))
        .append("me", hosts[0])
        .append("ok", 1.0);
  }

  private static void anyOperation(MongoContinuumRepository repository) {
    repository.findComputation(ComputationId.random());
  }

  @Nested
  class Permitting {
    @Test
    void a_replica_set_on_mongodb_8_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("8.2.12"), replicaSet("db1:27017", "db2:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void a_single_node_replica_set_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("7.0.40"), replicaSet("localhost:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mongos_passes() {
      var hello = new Document("msg", "isdbgrid").append("ok", 1.0);
      var repository = new MongoContinuumRepository(server(buildInfo("8.2.12"), hello), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void mongodb_5_0_is_the_floor_and_passes() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("5.0.31"), replicaSet("localhost:27017")), "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
    }

    @Test
    void detection_runs_once_per_repository() {
      MongoClient client = server(buildInfo("8.2.12"), replicaSet("localhost:27017"));
      var repository = new MongoContinuumRepository(client, "app");

      anyOperation(repository);
      anyOperation(repository);

      verify(client.getDatabase("app")).runCommand(new Document("hello", 1));
    }
  }

  @Nested
  class Refusing {
    @Test
    void a_standalone_server_is_refused_with_the_fix() {
      var repository = new MongoContinuumRepository(server(buildInfo("8.2.12"), EMPTY_HELLO), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("MongoDB 8.2.12 standalone")
          .withMessageContaining("--replSet")
          .withMessageContaining("assumeMongoDb");
    }

    @Test
    void mongodb_before_5_0_is_refused() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("4.4.29"), replicaSet("localhost:27017")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("MongoDB 4.4.29")
          .withMessageContaining("5.0+");
    }

    @Test
    void amazon_documentdb_is_refused_by_name() {
      var repository =
          new MongoContinuumRepository(
              server(
                  buildInfo("5.0.0"),
                  replicaSet("cluster.cluster-abc.us-east-1.docdb.amazonaws.com:27017")),
              "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("Amazon DocumentDB")
          .withMessageContaining("reports as MongoDB 5.0.0")
          .withMessageContaining("not certified");
    }

    @Test
    void azure_cosmos_db_is_refused_by_name() {
      var repository =
          new MongoContinuumRepository(
              server(buildInfo("7.0.0"), replicaSet("acct.mongo.cosmos.azure.com:10255")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("Azure Cosmos DB")
          .withMessageContaining("reports as MongoDB 7.0.0");
    }

    @Test
    void ferretdb_is_refused_by_name() {
      var buildInfo =
          buildInfo("7.0.42").append("ferretdb", new Document("version", "v2.1.0"));
      var repository =
          new MongoContinuumRepository(server(buildInfo, replicaSet("localhost:27017")), "app");

      assertThatExceptionOfType(ContinuumPersistenceException.class)
          .isThrownBy(() -> anyOperation(repository))
          .withMessageContaining("FerretDB v2.1.0")
          .withMessageContaining("reports as MongoDB 7.0.42");
    }
  }

  @Nested
  class The_escape_hatch {
    @Test
    void assume_mongodb_never_runs_a_command() {
      MongoClient client = server(buildInfo("8.2.12"), EMPTY_HELLO);
      var repository = MongoContinuumRepository.assumeMongoDb(client, "app");

      assertThatCode(() -> anyOperation(repository)).doesNotThrowAnyException();
      verify(client.getDatabase("app"), never()).runCommand(any(Bson.class));
    }
  }

  @Nested
  class Construction {
    @Test
    void opens_no_connection_and_runs_no_command() {
      MongoClient client = server(buildInfo("8.2.12"), EMPTY_HELLO);

      new MongoContinuumRepository(client, "app");

      verify(client.getDatabase("app"), never()).runCommand(any(Bson.class));
      verify(client, never()).startSession();
    }
  }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -pl continuum-mongo test -Dtest=TopologyGuardTest`
Expected: compilation failure — `assumeMongoDb` undefined (and `Refusing` tests would fail with no exception once it compiles).

- [ ] **Step 3: Write `TopologyGuard`**

`continuum-mongo/src/main/java/org/jwcarman/continuum/mongo/TopologyGuard.java`:

```java
package org.jwcarman.continuum.mongo;

import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.bson.Document;
import org.jwcarman.continuum.spi.ContinuumPersistenceException;

/**
 * Refuses, on first use, any server that cannot meet the TCK: standalone (no multi-document
 * transactions), MongoDB before 5.0, and the wire-compatible impostors — Amazon DocumentDB, Azure
 * Cosmos DB, FerretDB — which report genuine-looking versions and are refused by name until
 * certified. The message names what was found and how to fix it.
 */
final class TopologyGuard {

  private static final int MINIMUM_MAJOR = 5;
  private static final String ESCAPE_HATCH =
      " An operator who knows better can bypass detection with"
          + " MongoContinuumRepository.assumeMongoDb(client, databaseName).";

  private TopologyGuard() {}

  static void verify(MongoDatabase database) {
    Document buildInfo = database.runCommand(new Document("buildInfo", 1));
    String version = buildInfo.getString("version");
    Document ferret = buildInfo.get("ferretdb", Document.class);
    if (ferret != null) {
      throw refuse(
          "FerretDB " + ferret.getString("version") + " (reports as MongoDB " + version + ")");
    }
    Document hello = database.runCommand(new Document("hello", 1));
    for (String host : hosts(hello)) {
      String lower = host.toLowerCase(Locale.ROOT);
      if (lower.contains(".docdb.amazonaws.com")
          || lower.contains(".docdb-elastic.amazonaws.com")) {
        throw refuse("Amazon DocumentDB (reports as MongoDB " + version + ")");
      }
      if (lower.contains(".cosmos.azure.com")) {
        throw refuse("Azure Cosmos DB (reports as MongoDB " + version + ")");
      }
    }
    if (major(version) < MINIMUM_MAJOR) {
      throw new ContinuumPersistenceException(
          "unsupported database platform: MongoDB "
              + version
              + "; continuum needs MongoDB 5.0+."
              + ESCAPE_HATCH);
    }
    boolean mongos = "isdbgrid".equals(hello.getString("msg"));
    boolean replicaSet = hello.containsKey("setName");
    if (!mongos && !replicaSet) {
      throw new ContinuumPersistenceException(
          "unsupported database topology: MongoDB "
              + version
              + " standalone; continuum needs a replica set for multi-document transactions,"
              + " and a single node started with --replSet (then rs.initiate()) is enough."
              + ESCAPE_HATCH);
    }
  }

  private static ContinuumPersistenceException refuse(String detected) {
    return new ContinuumPersistenceException(
        "unsupported database platform: "
            + detected
            + "; not certified — see the persistence guide."
            + ESCAPE_HATCH);
  }

  private static List<String> hosts(Document hello) {
    List<String> hosts = new ArrayList<>();
    List<String> listed = hello.getList("hosts", String.class);
    if (listed != null) {
      hosts.addAll(listed);
    }
    String me = hello.getString("me");
    if (me != null) {
      hosts.add(me);
    }
    return hosts;
  }

  private static int major(String version) {
    int dot = version.indexOf('.');
    return Integer.parseInt(dot < 0 ? version : version.substring(0, dot));
  }
}
```

- [ ] **Step 4: Wire the guard into the repository**

In `MongoContinuumRepository`:

1. Add imports `java.util.concurrent.atomic.AtomicBoolean`.
2. Add fields and replace the public constructor with a private one plus two factories:

```java
  private final MongoDatabase database;
  private final AtomicBoolean verified;

  /**
   * Creates a repository over the named database. Opens no connection; on first use it verifies the
   * server is a MongoDB 5.0+ replica set (or mongos) and refuses anything else by name.
   *
   * @param client the application's client — it owns pooling and credentials
   * @param databaseName the database holding the four continuum collections
   */
  public MongoContinuumRepository(MongoClient client, String databaseName) {
    this(client, databaseName, false);
  }

  /**
   * Bypasses topology detection for an operator who knows better — for example a platform the
   * guard refuses by name that has been certified privately. Accepts the silent-failure risk the
   * guard exists to remove.
   *
   * @param client the application's client
   * @param databaseName the database holding the four continuum collections
   * @return a repository that never runs {@code buildInfo}/{@code hello}
   */
  public static MongoContinuumRepository assumeMongoDb(MongoClient client, String databaseName) {
    return new MongoContinuumRepository(client, databaseName, true);
  }

  private MongoContinuumRepository(MongoClient client, String databaseName, boolean assumed) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.database =
        client
            .getDatabase(Objects.requireNonNull(databaseName, "databaseName must not be null"))
            .withCodecRegistry(Documents.codecRegistry());
    this.computations = database.getCollection(COMPUTATIONS);
    this.continuations = database.getCollection(CONTINUATIONS);
    this.results = database.getCollection(RESULTS);
    this.outbox = database.getCollection(OUTBOX);
    this.verified = new AtomicBoolean(assumed);
  }

  // Benign race: concurrent first uses verify the same server and reach the same verdict.
  private void verifyTopology() {
    if (!verified.get()) {
      TopologyGuard.verify(database);
      verified.set(true);
    }
  }
```

3. Call `verifyTopology();` as the first statement of every public SPI method (`createComputation`, `registerContinuation`, `complete`, `findComputation`, `claimDeliveries`, `acknowledgeDelivery`, `releaseDelivery`, `findExpired`, `extendDeadline`, `purgeResults`) and of `ensureIndexes()`. It is not called in the constructor.

- [ ] **Step 5: Run the unit tests and the IT suites**

Run: `mvn spotless:apply -q && mvn -pl continuum-mongo verify`
Expected: `TopologyGuardTest` 12/12; `MongoContinuumTckIT` 26/26 (now through the guard — the container is a genuine single-node replica set); `EnsureIndexesIT` 3/3. BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add continuum-mongo
git commit -m "feat(mongo): topology guard — refuse standalone, pre-5.0, and impostors by name

Runs buildInfo and hello once on first use, never at construction. Standalone
servers have no multi-document transactions, so the message says which flag to
add; DocumentDB, Cosmos DB and FerretDB report genuine-looking versions and are
refused until certified. assumeMongoDb bypasses detection.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi"
```

---

### Task 4: Spring Boot auto-configuration and persistence selection

**Files:**
- Modify: `continuum-autoconfigure/pom.xml` — add `continuum-mongo` (optional) and `mongodb-driver-sync` (optional; imported by the auto-configuration class)
- Create: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/PersistenceType.java`
- Create: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/OnPersistenceTypeCondition.java`
- Create: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/ConditionalOnPersistenceType.java`
- Create: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/MongoContinuumProperties.java`
- Create: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/MongoContinuumAutoConfiguration.java`
- Modify: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/JdbcContinuumAutoConfiguration.java` — add `@ConditionalOnPersistenceType(PersistenceType.JDBC)`, update javadoc
- Modify: `continuum-autoconfigure/src/main/java/org/jwcarman/continuum/autoconfigure/ContinuumAutoConfiguration.java` — warning text mentions `continuum-mongo`
- Modify: `continuum-autoconfigure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` — add `MongoContinuumAutoConfiguration`
- Create: `continuum-autoconfigure/src/test/java/org/jwcarman/continuum/autoconfigure/MongoContinuumAutoConfigurationTest.java`

**Interfaces:**
- Consumes: `MongoContinuumRepository(MongoClient, String)`, `ensureIndexes()` from Tasks 1–2.
- Produces: properties `continuum.persistence.type`, `continuum.mongo.database`, `continuum.mongo.ensure-indexes`.

Selection rule (the Spring Session `store-type` precedent): if `continuum.persistence.type` is set, exactly that provider matches. If absent, a provider matches when it is the *only* candidate — a candidate being "its module class is on the classpath and its client bean (`DataSource` / `MongoClient`) is defined". Two candidates and no property → `IllegalStateException` naming the property and both candidates, so startup fails rather than guessing which store holds durable state. `memory` matches nothing, leaving the in-memory fallback.

- [ ] **Step 1: Declare the dependencies**

In `continuum-autoconfigure/pom.xml`, after the `continuum-jdbc` optional dependency:

```xml
        <dependency>
            <groupId>org.jwcarman.continuum</groupId>
            <artifactId>continuum-mongo</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
            <optional>true</optional>
        </dependency>
```

Also add `spring-boot` (the `@ConfigurationProperties` annotation lives in `org.springframework.boot.context.properties`) if the analyzer reports it used-undeclared; it is already exempted as used-undeclared in the parent's analyzer config (`org.springframework.boot:spring-boot`), so likely nothing to add.

- [ ] **Step 2: Write the failing selection tests**

`continuum-autoconfigure/src/test/java/org/jwcarman/continuum/autoconfigure/MongoContinuumAutoConfigurationTest.java`:

```java
package org.jwcarman.continuum.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.mongodb.client.MongoClient;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.jdbc.JdbcContinuumRepository;
import org.jwcarman.continuum.memory.InMemoryContinuumRepository;
import org.jwcarman.continuum.mongo.MongoContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class MongoContinuumAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  JdbcContinuumAutoConfiguration.class,
                  MongoContinuumAutoConfiguration.class,
                  ContinuumAutoConfiguration.class));

  @Configuration(proxyBeanMethods = false)
  static class MongoClientConfiguration {
    // Deep stubs so getDatabase(...).getCollection(...).createIndex(...) all return mocks.
    static final MongoClient CLIENT = mock(MongoClient.class, RETURNS_DEEP_STUBS);

    @Bean
    MongoClient mongoClient() {
      return CLIENT;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DataSourceConfiguration {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  @Nested
  class Auto_detection {
    @Test
    void uses_mongo_when_continuum_mongo_and_a_mongo_client_are_present() {
      runner
          .withUserConfiguration(MongoClientConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(MongoContinuumRepository.class));
    }

    @Test
    void falls_back_to_memory_when_continuum_mongo_is_not_on_the_classpath() {
      runner
          .withClassLoader(new FilteredClassLoader(MongoContinuumRepository.class))
          .withUserConfiguration(MongoClientConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(InMemoryContinuumRepository.class));
    }

    @Test
    void jdbc_alone_still_selects_jdbc() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class)
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(JdbcContinuumRepository.class));
    }

    @Test
    void both_candidates_without_the_property_fail_startup_naming_the_property() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .run(
              context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .rootCause()
                    .hasMessageContaining("continuum.persistence.type")
                    .hasMessageContaining("jdbc")
                    .hasMessageContaining("mongo");
              });
    }
  }

  @Nested
  class Explicit_type {
    @Test
    void mongo_wins_over_jdbc_when_selected() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=mongo")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(MongoContinuumRepository.class));
    }

    @Test
    void jdbc_wins_over_mongo_when_selected() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=jdbc")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(JdbcContinuumRepository.class));
    }

    @Test
    void memory_disables_both_durable_providers() {
      runner
          .withUserConfiguration(DataSourceConfiguration.class, MongoClientConfiguration.class)
          .withPropertyValues("continuum.persistence.type=memory")
          .run(
              context ->
                  assertThat(context.getBean(ContinuumRepository.class))
                      .isInstanceOf(InMemoryContinuumRepository.class));
    }
  }

  @Nested
  class Mongo_properties {
    /** A client whose database and collections are plain mocks, so index calls can be verified. */
    private static MongoClient client(MongoDatabase database) {
      MongoClient client = mock();
      MongoCollection<Document> collection = mock();
      when(client.getDatabase(anyString())).thenReturn(database);
      when(database.withCodecRegistry(any())).thenReturn(database);
      when(database.getCollection(anyString())).thenReturn(collection);
      return client;
    }

    private ApplicationContextRunner mongoOnly(MongoClient client) {
      return new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  MongoContinuumAutoConfiguration.class, ContinuumAutoConfiguration.class))
          .withBean(MongoClient.class, () -> client);
    }

    @Test
    void database_name_defaults_to_boots_property() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues("spring.mongodb.database=orders", "continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("orders"));
    }

    @Test
    void continuum_mongo_database_overrides_boots_property() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues(
              "spring.mongodb.database=orders",
              "continuum.mongo.database=continuum",
              "continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("continuum"));
    }

    @Test
    void the_driver_default_database_is_the_last_resort() {
      MongoClient client = client(mock());
      mongoOnly(client)
          .withPropertyValues("continuum.mongo.ensure-indexes=false")
          .run(context -> verify(client).getDatabase("test"));
    }

    @Test
    void indexes_are_ensured_at_startup_by_default() {
      MongoDatabase database = mock();
      MongoClient client = client(database);
      // The guard runs before ensureIndexes(); script a replica set so it passes.
      when(database.runCommand(new Document("buildInfo", 1)))
          .thenReturn(new Document("version", "8.2.12"));
      when(database.runCommand(new Document("hello", 1)))
          .thenReturn(new Document("setName", "rs0"));
      mongoOnly(client)
          .run(
              context ->
                  verify(database.getCollection("continuum_outbox"))
                      .createIndex(any(Bson.class)));
    }

    @Test
    void ensure_indexes_false_touches_nothing_at_startup() {
      MongoDatabase database = mock();
      MongoClient client = client(database);
      mongoOnly(client)
          .withPropertyValues("continuum.mongo.ensure-indexes=false")
          .run(
              context -> {
                assertThat(context.getBean(ContinuumRepository.class))
                    .isInstanceOf(MongoContinuumRepository.class);
                verify(database, never()).runCommand(any(Bson.class));
                verify(database.getCollection("continuum_outbox"), never())
                    .createIndex(any(Bson.class));
              });
    }
  }
}
```

Additional imports for the class: `static org.mockito.ArgumentMatchers.any`, `static org.mockito.ArgumentMatchers.anyString`, `static org.mockito.Mockito.never`, `static org.mockito.Mockito.when`, `com.mongodb.client.MongoCollection`, `com.mongodb.client.MongoDatabase`, `org.bson.Document`, `org.bson.conversions.Bson`. `RETURNS_DEEP_STUBS` is then unused in `MongoClientConfiguration` — use `client(mock())`-style plain mocks there too (`MongoClientConfiguration.CLIENT` only needs `getDatabase(...)`/`getCollection(...)` to return mocks, and `ensure-indexes` defaults to true, so that configuration must also script `buildInfo`/`hello` as in `indexes_are_ensured_at_startup_by_default`, or the `Auto_detection`/`Explicit_type` tests must set `continuum.mongo.ensure-indexes=false` via `withPropertyValues` — do the latter; it keeps those tests about selection only).

- [ ] **Step 3: Run to verify failure**

Run: `mvn -pl continuum-autoconfigure test -Dtest=MongoContinuumAutoConfigurationTest`
Expected: compilation failure — `MongoContinuumAutoConfiguration` undefined.

- [ ] **Step 4: Write `PersistenceType`, the condition, and the annotation**

`PersistenceType.java`:

```java
package org.jwcarman.continuum.autoconfigure;

/** The value space of {@code continuum.persistence.type}. */
public enum PersistenceType {
  /** {@code continuum-jdbc} over the application's {@code DataSource}. */
  JDBC,
  /** {@code continuum-mongo} over the application's {@code MongoClient}. */
  MONGO,
  /** {@code continuum-memory}: no durability; the fallback when nothing else is configured. */
  MEMORY
}
```

`ConditionalOnPersistenceType.java`:

```java
package org.jwcarman.continuum.autoconfigure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/**
 * Matches when {@code continuum.persistence.type} selects this provider, or — when the property is
 * absent — when this provider is the only candidate present. Two candidates and no property fail
 * startup rather than guess.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Conditional(OnPersistenceTypeCondition.class)
public @interface ConditionalOnPersistenceType {
  /**
   * The provider this configuration contributes.
   *
   * @return the provider type
   */
  PersistenceType value();
}
```

`OnPersistenceTypeCondition.java`:

```java
package org.jwcarman.continuum.autoconfigure;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;

/**
 * The selection rule, in the shape of Spring Session's {@code store-type}: an explicit property
 * wins; otherwise the single candidate wins; two candidates fail startup naming the property.
 */
final class OnPersistenceTypeCondition extends SpringBootCondition {

  static final String PROPERTY = "continuum.persistence.type";

  private static final String JDBC_REPOSITORY = "org.jwcarman.continuum.jdbc.JdbcContinuumRepository";
  private static final String MONGO_REPOSITORY = "org.jwcarman.continuum.mongo.MongoContinuumRepository";
  private static final String DATA_SOURCE = "javax.sql.DataSource";
  private static final String MONGO_CLIENT = "com.mongodb.client.MongoClient";

  @Override
  public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
    PersistenceType wanted =
        PersistenceType.valueOf(
            String.valueOf(
                metadata
                    .getAnnotationAttributes(ConditionalOnPersistenceType.class.getName())
                    .get("value")));
    ConditionMessage.Builder message = ConditionMessage.forCondition("ContinuumPersistence");
    String configured = context.getEnvironment().getProperty(PROPERTY);
    if (configured != null) {
      PersistenceType selected = PersistenceType.valueOf(configured.toUpperCase(Locale.ROOT));
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
```

- [ ] **Step 5: Write the properties and the auto-configuration**

`MongoContinuumProperties.java`:

```java
package org.jwcarman.continuum.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code continuum.mongo.*}. */
@ConfigurationProperties("continuum.mongo")
public class MongoContinuumProperties {

  /**
   * The database holding the continuum collections. Defaults to Boot's {@code
   * spring.mongodb.database} (or the older {@code spring.data.mongodb.database}), then {@code test}
   * — the driver's own default.
   */
  private String database;

  /** Whether to call {@code ensureIndexes()} at startup. */
  private boolean ensureIndexes = true;

  /** Instantiated by Spring's binder. */
  public MongoContinuumProperties() {
    // bound reflectively
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public boolean isEnsureIndexes() {
    return ensureIndexes;
  }

  public void setEnsureIndexes(boolean ensureIndexes) {
    this.ensureIndexes = ensureIndexes;
  }
}
```

(Javadoc every public getter/setter with one line — the release javadoc build runs doclint; e.g. `/** @return the database name, or null to derive it from Boot's Mongo properties */`.)

`MongoContinuumAutoConfiguration.java`:

```java
package org.jwcarman.continuum.autoconfigure;

import com.mongodb.client.MongoClient;
import org.jwcarman.continuum.mongo.MongoContinuumRepository;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Configures durable MongoDB persistence (certified: MongoDB 5.0+ replica sets) when {@code
 * continuum-mongo} is on the classpath, the application defines a {@link MongoClient}, and {@code
 * continuum.persistence.type} either selects {@code mongo} or is absent with no competing JDBC
 * candidate.
 */
@AutoConfiguration(
    before = ContinuumAutoConfiguration.class,
    // String names: Boot's Mongo auto-configuration lives in an optional module. First name is
    // Spring Boot 4.x; second is the Boot 3.x package. Unknown names are ignored.
    afterName = {
      "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration",
      "org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration"
    })
@ConditionalOnClass(MongoContinuumRepository.class)
@ConditionalOnBean(MongoClient.class)
@ConditionalOnPersistenceType(PersistenceType.MONGO)
@EnableConfigurationProperties(MongoContinuumProperties.class)
public class MongoContinuumAutoConfiguration {

  private static final String DEFAULT_DATABASE = "test";

  /** Instantiated by Spring Boot's auto-configuration machinery, not by application code. */
  public MongoContinuumAutoConfiguration() {
    // Spring instantiates this class reflectively; nothing to initialize.
  }

  /**
   * Contributes MongoDB persistence over the application's {@link MongoClient}, unless a {@link
   * ContinuumRepository} is already defined, and ensures the query indexes unless {@code
   * continuum.mongo.ensure-indexes=false}.
   *
   * @param client the application's client; it owns pooling and credentials
   * @param properties {@code continuum.mongo.*}
   * @param environment for Boot's own Mongo database property as the default name
   * @return a {@link MongoContinuumRepository} bound to that client
   */
  @Bean
  @ConditionalOnMissingBean(ContinuumRepository.class)
  public ContinuumRepository mongoContinuumRepository(
      MongoClient client, MongoContinuumProperties properties, Environment environment) {
    MongoContinuumRepository repository =
        new MongoContinuumRepository(client, databaseName(properties, environment));
    if (properties.isEnsureIndexes()) {
      repository.ensureIndexes();
    }
    return repository;
  }

  private static String databaseName(MongoContinuumProperties properties, Environment environment) {
    if (properties.getDatabase() != null) {
      return properties.getDatabase();
    }
    String boot4 = environment.getProperty("spring.mongodb.database");
    if (boot4 != null) {
      return boot4;
    }
    String boot3 = environment.getProperty("spring.data.mongodb.database");
    return boot3 != null ? boot3 : DEFAULT_DATABASE;
  }
}
```

Note `ensureIndexes()` runs the topology guard first (Task 3), so with `ensure-indexes=true` the guard runs at startup — a deliberate consequence: a misconfigured topology fails the application at boot, which is when you want to hear about it. The persistence guide says so.

- [ ] **Step 6: Gate the JDBC auto-configuration and register the import**

In `JdbcContinuumAutoConfiguration`, add `@ConditionalOnPersistenceType(PersistenceType.JDBC)` below `@ConditionalOnBean(DataSource.class)` and extend the class javadoc: "…and {@code continuum.persistence.type} either selects {@code jdbc} or is absent with no competing MongoDB candidate."

In `ContinuumAutoConfiguration`, change the warning text to: `"... Add continuum-jdbc and a DataSource, or continuum-mongo and a MongoClient (or define your own ContinuumRepository bean) for durability."` and update the test in `ContinuumAutoConfigurationTest` that asserts on the message if it matches the full sentence (it matches `"Computations will NOT survive restarts"` and `"defaulting to the in-memory repository"` — both unchanged).

Append to `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:

```
org.jwcarman.continuum.autoconfigure.MongoContinuumAutoConfiguration
```

- [ ] **Step 7: Run the autoconfigure tests**

Run: `mvn spotless:apply -q && mvn -q -pl continuum-mongo install -DskipTests && mvn -pl continuum-autoconfigure verify`
Expected: `MongoContinuumAutoConfigurationTest` all pass; `ContinuumAutoConfigurationTest` and `AutoConfigurationImportsTest` still pass (the imports test has H2 + spring-boot-jdbc on the classpath and no `MongoClient` bean, so JDBC remains the single candidate).

- [ ] **Step 8: Commit**

```bash
git add continuum-autoconfigure
git commit -m "feat(autoconfigure): MongoDB auto-configuration and continuum.persistence.type

Spring Session's store-type rule: an explicit property wins, the single
candidate wins, two candidates fail startup naming the property. Boot's Mongo
database property is the default name; indexes are ensured at startup unless
continuum.mongo.ensure-indexes=false.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi"
```

---

### Task 5: Documentation, CHANGELOG, full gate

**Files:**
- Modify: `docs/guides/persistence.md` — new `## MongoDB (`continuum-mongo`)` section between the JDBC schema section and `## In-memory`
- Modify: `docs/guides/spring-boot.md` — selection rules and properties
- Modify: `README.md` — module table row; the "with `continuum-jdbc` on the classpath…" sentence near line 101
- Modify: `CHANGELOG.md` — Unreleased entry
- Modify: `docs/superpowers/specs/2026-08-25-mongo-persistence-design.md` — property default reads `spring.mongodb.database` (Boot 4), falling back to `spring.data.mongodb.database`

- [ ] **Step 1: persistence.md**

Insert before `## In-memory (`continuum-memory`)`:

```markdown
## MongoDB (`continuum-mongo`)

`MongoContinuumRepository` runs over a plain `MongoClient` and a database name —
no Spring Data, driver `mongodb-driver-sync` only. Four collections mirror the
JDBC tables (`continuum_computation`, `continuum_continuation`,
`continuum_result`, `continuum_outbox`); every operation that touches more than
one document is a transaction; the outbox claim is a per-document
`findOneAndUpdate` compare-and-set, which does what `SKIP LOCKED` does in SQL
with no locking clause at all.

| Platform | Status |
|---|---|
| MongoDB 5.0+ replica set (any size, one node is enough) or sharded cluster | **Certified** — full TCK on `mongo:8.2`, every build |
| MongoDB standalone (no `--replSet`) | **Refused** — no multi-document transactions |
| MongoDB < 5.0 | **Refused** |
| Amazon DocumentDB, Azure Cosmos DB (Mongo API), FerretDB | **Refused by name** — not certified |

!!! warning "A replica set is required"
    Standalone `mongod` has no multi-document transactions, and the ownership
    transfer in `complete()` — delete the pending document, insert the result,
    insert the deliveries — must be atomic or a crash between steps loses
    deliveries silently. A **single node** started with `--replSet rs0` and
    initiated once (`rs.initiate()`) is all it takes; Atlas, Testcontainers and
    Boot's docker-compose support all give you one. On first use the repository
    runs `buildInfo` and `hello` once and refuses anything else, naming what it
    found and the fix. `MongoContinuumRepository.assumeMongoDb(client, name)`
    bypasses detection for an operator who knows better.

**Precision.** Instants are stored as BSON `date`: millisecond precision, where
the JDBC platforms keep microseconds. Nothing in the model needs sub-millisecond
time; `submittedAt`/`completedAt` read back truncated to the millisecond.

**Indexes — yours, but we help.** Collections appear on first write; the only
schema is four indexes, which `ensureIndexes()` creates idempotently:

| Collection | Index | Serves |
|---|---|---|
| `continuum_computation` | `{kind: 1, deadlineAt: 1}` | `findExpired` |
| `continuum_continuation` | `{computationId: 1}` | fan-out on `complete` |
| `continuum_result` | `{kind: 1, completedAt: 1}` | `purgeResults` |
| `continuum_outbox` | `{kind: 1, availableAt: 1}` | `claimDeliveries` |

Call it once at startup, or let the Spring Boot auto-configuration do so
(`continuum.mongo.ensure-indexes`, default `true`). The repository never calls
it on its own.

**Claim cost.** Each claimed delivery is one round trip (`findOneAndUpdate`),
so a batch of *n* is *n* round trips rather than one query. At pump batch sizes
this is immaterial; it is the price of a claim that needs no lock.
```

- [ ] **Step 2: spring-boot.md**

Replace the numbered selection list with:

```markdown
A `Continuum` bean is auto-configured. Repository selection:

1. An application-defined `ContinuumRepository` bean always wins.
2. `continuum.persistence.type` — `jdbc`, `mongo` or `memory` — selects
   explicitly when set.
3. Otherwise the single available durable provider is used:
    - `continuum-jdbc` on the classpath **and** a `DataSource` bean → durable
      persistence on a certified platform — PostgreSQL 9.5+, MySQL 8+,
      MariaDB 10.6+, Oracle 23ai+, or SQL Server 2012+
      (`JdbcContinuumRepository`).
    - `continuum-mongo` on the classpath **and** a `MongoClient` bean → MongoDB
      5.0+ replica sets (`MongoContinuumRepository`).

    Ordering against Boot's own `DataSourceAutoConfiguration` and
    `MongoAutoConfiguration` is handled, so Boot-auto-configured clients count.
    If **both** are available and the property is unset, startup fails naming
    the property — the same rule Spring Session applies to `store-type`,
    because guessing which store holds durable state is worse than asking.
4. Otherwise the starter falls back to the **in-memory repository and logs a
   warning** — computations will not survive restarts. Fine for tests; not
   for production.

MongoDB properties:

| Property | Default | Meaning |
|---|---|---|
| `continuum.mongo.database` | `spring.mongodb.database` (Boot 4) / `spring.data.mongodb.database` (Boot 3), then `test` | database holding the continuum collections |
| `continuum.mongo.ensure-indexes` | `true` | call `ensureIndexes()` at startup — which also runs the topology check, so a standalone server fails the application at boot rather than at the first completion |
```

- [ ] **Step 3: README.md**

Module table: add after the `continuum-jdbc` row:

```markdown
| `continuum-mongo` | MongoDB persistence — certified on 5.0+ replica sets |
```

Near line 101, extend the sentence: "…with `continuum-jdbc` on the classpath and a `DataSource` defined you get durable JDBC persistence; with `continuum-mongo` and a `MongoClient`, MongoDB; …" keeping the existing wording for the fallback.

- [ ] **Step 4: CHANGELOG.md**

Under `## [Unreleased]`:

```markdown
### Added

- **`continuum-mongo`: MongoDB persistence, certified.** `MongoContinuumRepository`
  over a plain `MongoClient` passes the full TCK, concurrency suites included,
  on MongoDB 8.2 every build; MongoDB 5.0+ replica sets are supported. Four
  collections mirror the JDBC tables; every multi-document operation is a
  transaction; the outbox claim is a per-document `findOneAndUpdate`
  compare-and-set — what `SKIP LOCKED` does in SQL, with no locking clause.
  Instants are BSON `date` (millisecond precision). `ensureIndexes()` creates
  the four query indexes idempotently. On first use the repository refuses, by
  name and with the fix in the message, what cannot meet the TCK: standalone
  servers (no multi-document transactions — one node with `--replSet` is
  enough), MongoDB before 5.0, and Amazon DocumentDB, Azure Cosmos DB and
  FerretDB until certified. `assumeMongoDb` bypasses detection.
- **`continuum.persistence.type`** (`jdbc` | `mongo` | `memory`) in the Spring
  Boot auto-configuration. Unnecessary with one durable provider present; with
  both `continuum-jdbc`+`DataSource` and `continuum-mongo`+`MongoClient`
  available and the property unset, startup fails naming it — Spring
  Session's `store-type` rule. Plus `continuum.mongo.database` and
  `continuum.mongo.ensure-indexes`.
```

- [ ] **Step 5: Spec touch-up**

In the spec's Spring Boot table, change the `continuum.mongo.database` default cell to: `` `spring.mongodb.database` (Boot 4), then `spring.data.mongodb.database` (Boot 3), then `test` ``.

- [ ] **Step 6: mkdocs strict**

Run: `/private/tmp/claude-501/-Users-jcarman-IdeaProjects-continuum/a19d2551-de11-49c9-b742-a9be509ac863/scratchpad/mkvenv/bin/mkdocs build --strict -d /private/tmp/claude-501/-Users-jcarman-IdeaProjects-continuum/a19d2551-de11-49c9-b742-a9be509ac863/scratchpad/site`
Expected: `Documentation built`, no WARNING/ERROR lines. (If the venv is gone: `python3 -m venv <scratchpad>/mkvenv && <scratchpad>/mkvenv/bin/pip install -q mkdocs-material` first.)

- [ ] **Step 7: Full gate**

Run: `mvn spotless:apply -q && mvn clean && mvn -P ci,license verify && mvn -P release javadoc:jar -DskipTests`
Expected: BUILD SUCCESS; nine container-backed TCK suites (eight JDBC + Mongo) plus `EnsureIndexesIT`, `OutboxFailureInjectionIT`, `CorruptOutcomeIT` green; analyzer silent; javadoc zero warnings.

- [ ] **Step 8: Commit and push**

```bash
git add -A
git commit -m "docs(mongo): persistence and Spring Boot guides, README, CHANGELOG for continuum-mongo

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PDjudEYAVcpjSPWrKFjLXi"
git push origin main
```

Then watch CI: `gh run watch $(gh run list --branch main --limit 1 --json databaseId --jq '.[0].databaseId') --exit-status`, and confirm the Sonar gate stays OK: `curl -s "https://sonarcloud.io/api/qualitygates/project_status?projectKey=jwcarman_continuum"`.

---

## Self-review

**Spec coverage.** Client contract → Task 1. Driver → Task 1 pom. Timestamps/`Instant` codec → Task 1 `Documents`. Storage layout, outcome sub-document, UUID strings → Task 1. Semantics (all ten operations, bookkeeping from SPI instants, `$currentDate`) → Task 1 (Step 6 note fixes the one `Instant.now()`). Indexes + `ensureIndexes()` → Task 2. Guard, escape hatch, unit tests with scripted commands → Task 3. Certification via detecting constructor, `mongo:8.2` → Task 1 (guard added in Task 3 makes it "through the guard"). Boot: auto-configuration, three properties, Session-style selection → Task 4. Docs/README/CHANGELOG → Task 5. BOM and dependencyManagement → Task 1. Out of scope items: none planned, as intended.

**Placeholder scan.** None — every code step carries its code.

**Type consistency.** `MongoContinuumRepository(MongoClient, String)`, `assumeMongoDb(MongoClient, String)`, `ensureIndexes()`, `TopologyGuard.verify(MongoDatabase)`, `PersistenceType.{JDBC,MONGO,MEMORY}`, `ConditionalOnPersistenceType(value)`, `MongoContinuumProperties.{getDatabase,isEnsureIndexes}`, collection constants `Documents.{COMPUTATIONS,CONTINUATIONS,RESULTS,OUTBOX}` — used identically across tasks.
