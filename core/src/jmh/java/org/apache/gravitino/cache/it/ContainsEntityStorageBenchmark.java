/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cache.it;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.cache.BenchmarkHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmark for testing the performance of the {@code EntityStore.exists()} method.
 *
 * <p>This benchmark randomly selects an entity from a preloaded list and checks whether it exists
 * in the {@link EntityStore}. It ensures correctness by throwing an exception if the entity is not
 * found.
 *
 * <p>Benchmark results will reflect the performance of metadata existence checks under different
 * entity counts, as controlled by the {@code @Param totalCnt} field from {@link
 * AbstractEntityStorageBenchmark}.
 *
 * <p>This class extends {@link AbstractEntityStorageBenchmark}, which handles store setup,
 * teardown, and test entity generation.
 *
 * @see org.apache.gravitino.EntityStore#exists(org.apache.gravitino.NameIdentifier,
 *     Entity.EntityType)
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ContainsEntityStorageBenchmark extends AbstractEntityStorageBenchmark {

  @Benchmark
  public boolean benchmarkContains() throws IOException {
    int idx = random.nextInt(entities.size());
    Entity sampleEntity = (Entity) entities.get(idx);

    boolean exists =
        store.exists(BenchmarkHelper.getIdentFromEntity(sampleEntity), sampleEntity.type());
    if (!exists) {
      throw new RuntimeException("Entity not found in store");
    }

    return exists;
  }
}
