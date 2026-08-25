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
