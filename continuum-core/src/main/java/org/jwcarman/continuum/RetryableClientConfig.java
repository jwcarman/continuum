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
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;

/**
 * Creation-time configuration for a retryable client: codecs (including the dispatch codec) and the
 * per-attempt deadline. Fluent, no {@code build()} — the minting factory applies the customizer and
 * builds privately.
 *
 * @param <R> the result type
 * @param <C> the continuation type
 * @param <D> the dispatch type
 */
public interface RetryableClientConfig<R, C, D> {

  /**
   * The factory used to resolve any payload codec not set explicitly.
   *
   * @param factory the codec factory
   * @return this config
   */
  RetryableClientConfig<R, C, D> codecs(CodecFactory factory);

  /**
   * Explicit result codec, overriding factory resolution.
   *
   * @param codec the result codec
   * @return this config
   */
  RetryableClientConfig<R, C, D> resultCodec(Codec<R> codec);

  /**
   * Explicit continuation codec, overriding factory resolution.
   *
   * @param codec the continuation codec
   * @return this config
   */
  RetryableClientConfig<R, C, D> continuationCodec(Codec<C> codec);

  /**
   * Explicit dispatch codec, overriding factory resolution.
   *
   * @param codec the dispatch codec
   * @return this config
   */
  RetryableClientConfig<R, C, D> dispatchCodec(Codec<D> codec);

  /**
   * The per-attempt timeout: {@code create} computes {@code now + deadline}, and retries extend by
   * it unless the retry says otherwise. Required.
   *
   * @param deadline the per-attempt timeout
   * @return this config
   */
  RetryableClientConfig<R, C, D> deadline(Duration deadline);
}
