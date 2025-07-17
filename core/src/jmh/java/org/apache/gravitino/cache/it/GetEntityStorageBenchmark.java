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
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.cache.BenchmarkHelper;
import org.apache.gravitino.meta.ModelEntity;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmark for testing the performance of the {@code EntityStore.get()} method.
 *
 * <p>This benchmark randomly selects a preloaded entity and retrieves it from the {@link
 * EntityStore} using its name identifier and type. It then validates the retrieved entity against
 * the expected one.
 *
 * <p>The test dataset is initialized by {@link AbstractEntityStorageBenchmark}, and the benchmark
 * is designed to simulate typical read-access patterns in metadata-intensive workloads.
 *
 * <p>Benchmark results reflect the efficiency and correctness of metadata retrieval under varying
 * entity counts.
 *
 * @see org.apache.gravitino.EntityStore#get(org.apache.gravitino.NameIdentifier, Entity.EntityType,
 *     Class)
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class GetEntityStorageBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityStorageBenchmark {

  /**
   * Benchmark for getting an entity from the store.
   *
   * @return the entity from store.
   */
  @Benchmark
  public Entity benchmarkGet() {
    int idx = random.nextInt(entities.size());
    Entity sampleEntity = (Entity) entities.get(idx);

    try {
      ModelEntity entityFromStore =
          store.get(
              BenchmarkHelper.getIdentFromEntity(sampleEntity),
              sampleEntity.type(),
              ModelEntity.class);
      return validateEntity((Entity) entities.get(idx), entityFromStore);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
