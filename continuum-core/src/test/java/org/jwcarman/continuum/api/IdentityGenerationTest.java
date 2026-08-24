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
package org.jwcarman.continuum.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.jwcarman.continuum.spi.DeliveryId;

/**
 * Identities are primary keys, so their generation carries a storage guarantee and not merely a
 * uniqueness one: v7's time ordering is what keeps index inserts local. These tests pin that
 * guarantee, because a silent regression to v4 would still pass every other test in the suite.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class IdentityGenerationTest {

  private static final int SAMPLES = 500;

  private static List<Supplier<UUID>> generators() {
    return List.of(
        () -> ComputationId.random().value(),
        () -> ContinuationId.random().value(),
        () -> DeliveryId.random().value());
  }

  @Test
  void every_identity_type_mints_version_7_not_version_4() {
    for (Supplier<UUID> generator : generators()) {
      assertThat(generator.get().version()).isEqualTo(7);
    }
  }

  @Test
  void successive_identities_are_time_ordered() {
    for (Supplier<UUID> generator : generators()) {
      var ids = IntStream.range(0, SAMPLES).mapToObj(i -> generator.get()).toList();
      // Unsigned comparison: v7 puts the timestamp in the high bits, where the sign bit lives.
      for (int i = 1; i < ids.size(); i++) {
        assertThat(unsignedCompare(ids.get(i - 1), ids.get(i)))
            .as("id %d must not sort after id %d", i - 1, i)
            .isNegative();
      }
    }
  }

  @Test
  void identities_stay_unique_across_concurrent_minting() throws Exception {
    for (Supplier<UUID> generator : generators()) {
      List<Callable<List<UUID>>> tasks =
          IntStream.range(0, 8)
              .<Callable<List<UUID>>>mapToObj(
                  t ->
                      () -> {
                        var minted = new ArrayList<UUID>();
                        for (int i = 0; i < SAMPLES; i++) {
                          minted.add(generator.get());
                        }
                        return minted;
                      })
              .toList();

      Set<UUID> all = new HashSet<>();
      int minted = 0;
      try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
        for (Future<List<UUID>> future : pool.invokeAll(tasks)) {
          var batch = future.get();
          minted += batch.size();
          all.addAll(batch);
        }
      }
      assertThat(all).hasSize(minted);
    }
  }

  private static int unsignedCompare(UUID a, UUID b) {
    int high = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
    return high != 0
        ? high
        : Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
  }
}
