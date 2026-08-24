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

  record ClientSettings(Duration deadline, Duration lease, Duration backoff, String workerId) {}

  private static final Logger log = LoggerFactory.getLogger(ClientSupport.class);

  private final Continuum continuum;
  private final ComputationKind kind;
  private final Codec<R> resultCodec;
  private final Codec<C> continuationCodec;
  private final ClientSettings settings;

  ClientSupport(
      Continuum continuum,
      ComputationKind kind,
      Codec<R> resultCodec,
      Codec<C> continuationCodec,
      ClientSettings settings) {
    this.continuum = continuum;
    this.kind = kind;
    this.resultCodec = resultCodec;
    this.continuationCodec = continuationCodec;
    this.settings = settings;
  }

  ComputationKind kind() {
    return kind;
  }

  Continuum continuum() {
    return continuum;
  }

  Duration deadline() {
    return settings.deadline();
  }

  Instant now() {
    return continuum.instants().instant();
  }

  Computation create(C continuation, byte[] dispatchPayload, Duration deadlineOverride) {
    Objects.requireNonNull(continuation, "continuation must not be null");
    Duration effective = deadlineOverride != null ? deadlineOverride : settings.deadline();
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
      case Outcome.Success success ->
          new TypedOutcome.Success<>(resultCodec.decode(success.payload()));
      case Outcome.Failure failure -> new TypedOutcome.Failure<>(failure.message());
      case Outcome.Expired expired -> new TypedOutcome.Expired<>(expired.kind(), expired.message());
    };
  }

  int deliverResults(int batchSize, BiConsumer<C, TypedOutcome<R>> consumer) {
    Objects.requireNonNull(consumer, "consumer must not be null");
    ContinuumRepository repository = continuum.repository();
    List<ClaimedDelivery> claimed =
        repository.claimDeliveries(settings.workerId(), kind, batchSize, settings.lease(), now());
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
        repository.releaseDelivery(delivery.id(), now().plus(settings.backoff()));
      }
    }
    return delivered;
  }

  int purgeExpiredResults(int batchSize, Duration ttl) {
    Objects.requireNonNull(ttl, "ttl must not be null");
    return continuum.repository().purgeResults(kind, now().minus(ttl), batchSize);
  }

  void failExpired(Computation computation, ExpiryKind expiryKind, String message) {
    continuum.repository().complete(computation.id(), Outcome.expired(expiryKind, message), now());
  }

  List<Computation> findExpired(int batchSize) {
    return continuum.repository().findExpired(kind, now(), batchSize);
  }
}
