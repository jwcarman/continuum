package org.jwcarman.continuum;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;

final class DefaultRetryableClientConfig<R, C, D> implements RetryableClientConfig<R, C, D> {

  private CodecFactory codecFactory;
  private Codec<R> resultCodec;
  private Codec<C> continuationCodec;
  private Codec<D> dispatchCodec;
  private Duration deadline;
  private Duration lease = Duration.ofSeconds(30);
  private Duration backoff = Duration.ofSeconds(30);
  private String workerId = "worker-" + UUID.randomUUID();

  @Override
  public RetryableClientConfig<R, C, D> codecs(CodecFactory factory) {
    this.codecFactory = Objects.requireNonNull(factory, "factory must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> resultCodec(Codec<R> codec) {
    this.resultCodec = Objects.requireNonNull(codec, "codec must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> continuationCodec(Codec<C> codec) {
    this.continuationCodec = Objects.requireNonNull(codec, "codec must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> dispatchCodec(Codec<D> codec) {
    this.dispatchCodec = Objects.requireNonNull(codec, "codec must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> deadline(Duration deadline) {
    this.deadline = Objects.requireNonNull(deadline, "deadline must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> lease(Duration lease) {
    this.lease = Objects.requireNonNull(lease, "lease must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> backoff(Duration backoff) {
    this.backoff = Objects.requireNonNull(backoff, "backoff must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> workerId(String workerId) {
    this.workerId = Objects.requireNonNull(workerId, "workerId must not be null");
    return this;
  }

  private <T> Codec<T> resolve(Codec<T> explicit, Class<T> type, String role) {
    if (explicit != null) {
      return explicit;
    }
    if (codecFactory != null) {
      return codecFactory.create(type);
    }
    throw new IllegalStateException("no codec configured for " + role + " type " + type.getName());
  }

  ClientSupport<R, C> buildSupport(
      Continuum continuum, ComputationKind kind, Class<R> resultType, Class<C> continuationType) {
    if (deadline == null) {
      throw new IllegalStateException("deadline is required");
    }
    return new ClientSupport<>(
        continuum,
        kind,
        resolve(resultCodec, resultType, "result"),
        resolve(continuationCodec, continuationType, "continuation"),
        deadline,
        lease,
        backoff,
        workerId);
  }

  Codec<D> resolveDispatchCodec(Class<D> dispatchType) {
    return resolve(dispatchCodec, dispatchType, "dispatch");
  }
}
