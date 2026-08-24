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

public interface RetryableClientConfig<R, C, D> {

  RetryableClientConfig<R, C, D> codecs(CodecFactory factory);

  RetryableClientConfig<R, C, D> resultCodec(Codec<R> codec);

  RetryableClientConfig<R, C, D> continuationCodec(Codec<C> codec);

  RetryableClientConfig<R, C, D> dispatchCodec(Codec<D> codec);

  RetryableClientConfig<R, C, D> deadline(Duration deadline);

  RetryableClientConfig<R, C, D> lease(Duration lease);

  RetryableClientConfig<R, C, D> backoff(Duration backoff);

  RetryableClientConfig<R, C, D> workerId(String workerId);
}
