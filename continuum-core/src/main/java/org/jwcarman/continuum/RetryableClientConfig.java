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
