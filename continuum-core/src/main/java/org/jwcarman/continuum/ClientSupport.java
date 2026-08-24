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
package org.jwcarman.continuum;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.continuum.api.Backoff;
import org.jwcarman.continuum.api.BatchSize;
import org.jwcarman.continuum.api.CompletionDelivery;
import org.jwcarman.continuum.api.CompletionResult;
import org.jwcarman.continuum.api.Computation;
import org.jwcarman.continuum.api.ComputationId;
import org.jwcarman.continuum.api.ComputationKind;
import org.jwcarman.continuum.api.ComputationRequest;
import org.jwcarman.continuum.api.ExpiryKind;
import org.jwcarman.continuum.api.Lease;
import org.jwcarman.continuum.api.Outcome;
import org.jwcarman.continuum.api.RegistrationResult;
import org.jwcarman.continuum.api.ResultTtl;
import org.jwcarman.continuum.api.TypedDelivery;
import org.jwcarman.continuum.api.TypedOutcome;
import org.jwcarman.continuum.api.TypedRegistration;
import org.jwcarman.continuum.spi.ClaimedDelivery;
import org.jwcarman.continuum.spi.ContinuumRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClientSupport<R, C> {

  static final Lease DEFAULT_LEASE = Lease.ofSeconds(30);
  static final Backoff DEFAULT_BACKOFF = Backoff.ofSeconds(30);

  private static final Logger log = LoggerFactory.getLogger(ClientSupport.class);
  private static final String BATCH_SIZE_NULL_MESSAGE = "batchSize must not be null";

  /**
   * Renders a duration the way an operator reads it — "1 day, 5 hours, 6 mins, 23 secs" — for the
   * diagnostic prose on an expired outcome. Zero-valued parts are omitted, so a short wait reads
   * "45 secs" rather than "0 days, 0 hours, 0 mins, 45 secs". The JDK has no such formatter;
   * ISO-8601 ({@code PT29H6M23S}) is precise but not what belongs in a log line or a result row.
   */
  static String describeElapsed(Duration elapsed) {
    if (elapsed.isNegative() || elapsed.isZero()) {
      return "0 secs";
    }
    StringJoiner parts = new StringJoiner(", ");
    appendPart(parts, elapsed.toDaysPart(), "day");
    appendPart(parts, elapsed.toHoursPart(), "hour");
    appendPart(parts, elapsed.toMinutesPart(), "min");
    appendPart(parts, elapsed.toSecondsPart(), "sec");
    return parts.length() == 0 ? "less than a second" : parts.toString();
  }

  private static void appendPart(StringJoiner parts, long value, String unit) {
    if (value > 0) {
      parts.add(value + " " + unit + (value == 1 ? "" : "s"));
    }
  }

  private final Continuum continuum;
  private final ComputationKind kind;
  private final Codec<R> resultCodec;
  private final Codec<C> continuationCodec;
  private final Duration deadline;
  // pid@hostname — so an operator reading claimed_by can identify the process holding a lease
  private final String workerId = ManagementFactory.getRuntimeMXBean().getName();

  ClientSupport(
      Continuum continuum,
      ComputationKind kind,
      Codec<R> resultCodec,
      Codec<C> continuationCodec,
      Duration deadline) {
    this.continuum = continuum;
    this.kind = kind;
    this.resultCodec = resultCodec;
    this.continuationCodec = continuationCodec;
    this.deadline = deadline;
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
    return continuum.create(
        new ComputationRequest(
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
      case RegistrationResult.Registered(var continuationId) ->
          new TypedRegistration.Registered<>(continuationId);
      case RegistrationResult.Resolved(var outcome) ->
          new TypedRegistration.Resolved<>(decode(outcome));
    };
  }

  TypedOutcome<R> decode(Outcome outcome) {
    return switch (outcome) {
      case Outcome.Success(byte[] payload) ->
          new TypedOutcome.Success<>(resultCodec.decode(payload));
      case Outcome.Failure(String message) -> new TypedOutcome.Failure<>(message);
      case Outcome.Expired(ExpiryKind expiryKind, String message) ->
          new TypedOutcome.Expired<>(expiryKind, message);
    };
  }

  int deliverResults(
      BatchSize batchSize, Lease lease, Backoff backoff, Consumer<TypedDelivery<C, R>> consumer) {
    Objects.requireNonNull(batchSize, BATCH_SIZE_NULL_MESSAGE);
    Objects.requireNonNull(lease, "lease must not be null");
    Objects.requireNonNull(backoff, "backoff must not be null");
    Objects.requireNonNull(consumer, "consumer must not be null");
    ContinuumRepository repository = continuum.repository();
    List<ClaimedDelivery> claimed =
        repository.claimDeliveries(workerId, kind, batchSize.value(), lease.value(), now());
    int delivered = 0;
    for (ClaimedDelivery claim : claimed) {
      try {
        consumer.accept(decodeDelivery(claim));
        repository.acknowledgeDelivery(claim.id());
        delivered++;
      } catch (RuntimeException e) {
        log.warn("delivery {} failed; releasing for retry", claim.id().value(), e);
        repository.releaseDelivery(claim.id(), now().plus(backoff.value()));
      }
    }
    return delivered;
  }

  private TypedDelivery<C, R> decodeDelivery(ClaimedDelivery claim) {
    CompletionDelivery delivery = claim.delivery();
    return new TypedDelivery<>(
        delivery.computationId(),
        delivery.continuationId(),
        continuationCodec.decode(delivery.continuationPayload()),
        decode(delivery.outcome()),
        delivery.submittedAt(),
        delivery.completedAt(),
        claim.deliveryAttempt());
  }

  int purgeExpiredResults(BatchSize batchSize, ResultTtl ttl) {
    Objects.requireNonNull(batchSize, BATCH_SIZE_NULL_MESSAGE);
    Objects.requireNonNull(ttl, "ttl must not be null");
    return continuum.repository().purgeResults(kind, now().minus(ttl.value()), batchSize.value());
  }

  void failExpired(Computation computation, ExpiryKind expiryKind, String message) {
    continuum.repository().complete(computation.id(), Outcome.expired(expiryKind, message), now());
  }

  List<Computation> findExpired(BatchSize batchSize) {
    Objects.requireNonNull(batchSize, BATCH_SIZE_NULL_MESSAGE);
    return continuum.repository().findExpired(kind, now(), batchSize.value());
  }
}
