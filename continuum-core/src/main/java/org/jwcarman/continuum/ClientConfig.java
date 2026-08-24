package org.jwcarman.continuum;

import java.time.Duration;
import org.jwcarman.codec.spi.Codec;
import org.jwcarman.codec.spi.CodecFactory;

public interface ClientConfig<R, C> {

  ClientConfig<R, C> codecs(CodecFactory factory);

  ClientConfig<R, C> resultCodec(Codec<R> codec);

  ClientConfig<R, C> continuationCodec(Codec<C> codec);

  ClientConfig<R, C> deadline(Duration deadline);

  ClientConfig<R, C> lease(Duration lease);

  ClientConfig<R, C> backoff(Duration backoff);

  ClientConfig<R, C> workerId(String workerId);
}
