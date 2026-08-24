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
import java.util.Objects;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;
import org.jwcarman.continuum.api.ComputationKind;

final class DefaultRetryableClientConfig<R, C, D> implements RetryableClientConfig<R, C, D> {

  private static final String CODEC_NULL_MESSAGE = "codec must not be null";

  private CodecFactory codecFactory;
  private Codec<R> resultCodec;
  private Codec<C> continuationCodec;
  private Codec<D> dispatchCodec;
  private Duration deadline;

  @Override
  public RetryableClientConfig<R, C, D> codecs(CodecFactory factory) {
    this.codecFactory = Objects.requireNonNull(factory, "factory must not be null");
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> resultCodec(Codec<R> codec) {
    this.resultCodec = Objects.requireNonNull(codec, CODEC_NULL_MESSAGE);
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> continuationCodec(Codec<C> codec) {
    this.continuationCodec = Objects.requireNonNull(codec, CODEC_NULL_MESSAGE);
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> dispatchCodec(Codec<D> codec) {
    this.dispatchCodec = Objects.requireNonNull(codec, CODEC_NULL_MESSAGE);
    return this;
  }

  @Override
  public RetryableClientConfig<R, C, D> deadline(Duration deadline) {
    this.deadline = Objects.requireNonNull(deadline, "deadline must not be null");
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
        deadline);
  }

  Codec<D> resolveDispatchCodec(Class<D> dispatchType) {
    return resolve(dispatchCodec, dispatchType, "dispatch");
  }
}
