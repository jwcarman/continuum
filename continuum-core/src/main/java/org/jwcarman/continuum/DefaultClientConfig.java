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

  @Override
  public ClientConfig<R, C> continuationCodec(Codec<C> codec) {
    this.continuationCodec = Objects.requireNonNull(codec, "codec must not be null");
    return this;
  }

  @Override
  public ClientConfig<R, C> deadline(Duration deadline) {
    this.deadline = Objects.requireNonNull(deadline, "deadline must not be null");
    return this;
  }

  @Override
  public ClientConfig<R, C> lease(Duration lease) {
    this.lease = Objects.requireNonNull(lease, "lease must not be null");
    return this;
  }

  @Override
  public ClientConfig<R, C> backoff(Duration backoff) {
    this.backoff = Objects.requireNonNull(backoff, "backoff must not be null");
    return this;
  }

  @Override
  public ClientConfig<R, C> workerId(String workerId) {
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
}
