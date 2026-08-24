package org.jwcarman.continuum.memory;

import org.jwcarman.continuum.spi.ContinuumRepository;
import org.jwcarman.continuum.testing.ContinuumTck;

class InMemoryContinuumTckTest extends ContinuumTck {

  @Override
  protected ContinuumRepository createRepository() {
    return new InMemoryContinuumRepository();
  }
}
