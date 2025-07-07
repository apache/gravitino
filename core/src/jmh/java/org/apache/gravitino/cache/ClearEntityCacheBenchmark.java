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

package org.apache.gravitino.cache;

import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.meta.ModelEntity;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

/**
 * EntityCacheClearBenchmark benchmarks the performance of the {@link EntityCache#clear()} operation
 * under varying cache sizes.
 *
 * <p>This benchmark repeatedly clears the cache to evaluate the efficiency and overhead of the
 * cache invalidation mechanism, especially under different entity volumes (e.g., 10, 100, 1000).
 *
 * <p>Before each invocation, the cache is pre-populated with a predefined list of entities to
 * ensure the {@code clear()} method operates on a non-empty cache.
 *
 * @param <E> the entity type being tested, must extend {@link Entity} and implement {@link
 *     HasIdentifier}
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class ClearEntityCacheBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityBenchmark {

  @Setup(Level.Invocation)
  @SuppressWarnings("unchecked")
  public void prepareCacheForClear() {
    cache.clear();
    entities.forEach(e -> cache.put((ModelEntity) e));
  }

  @Benchmark
  public void benchmarkClear() {
    cache.clear();
  }
}
