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
import com.mongodb.client.model.Indexes;
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
