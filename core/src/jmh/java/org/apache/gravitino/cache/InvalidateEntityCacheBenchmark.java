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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.ModelEntity;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

/**
 * EntityCacheInvalidateBenchmark benchmarks the performance of the {@link EntityCache#invalidate}
 * operation for a single entity entry.
 *
 * <p>Before each benchmark invocation, the cache is repopulated with a predefined list of entities
 * to ensure that the invalidation targets a present and consistent entry.
 *
 * <p>The benchmark focuses on the following scenario:
 *
 * <ul>
 *   <li>{@code benchmarkInvalidate}: Invalidates a single {@link Entity} identified by a fixed
 *       {@link NameIdentifier} and {@link Entity.EntityType}.
 * </ul>
 *
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.apache.gravitino.cache.CaffeineEntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class InvalidateEntityCacheBenchmark extends AbstractEntityBenchmark {

  @Setup(Level.Invocation)
  @SuppressWarnings("unchecked")
  public void prepareCacheForClear() {
    entities.forEach(e -> cache.put((ModelEntity) e));
  }

  @Benchmark
  public void benchmarkInvalidate() {
    cache.invalidate(NameIdentifier.of("m1"), Entity.EntityType.METALAKE);
  }
}
