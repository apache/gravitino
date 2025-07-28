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

import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * EntityCacheSizeBenchmark benchmarks the performance and overhead of querying the cache size via
 * {@link EntityCache#size()} under varying data volumes.
 *
 * <p>During setup, both standard entities and relation-based entities are preloaded into the cache
 * using the configured entity count. This ensures that the {@code size()} method operates on a
 * fully populated cache with realistic structure and distribution.
 *
 * <p>The benchmark includes a single method:
 *
 * <ul>
 *   <li>{@code entityCacheSize}: Measures the execution time of retrieving the total number of
 *       cached entries.
 * </ul>
 *
 * @param <E> the type of related entity, extending {@link Entity} and implementing {@link
 *     HasIdentifier}
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class MeasureSizeEntityCacheBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityBenchmark {

  @Override
  @SuppressWarnings("unchecked")
  public void setup() {
    Config config = new Config() {};
    this.cache = new CaffeineEntityCache(config);
    this.entities = BenchmarkHelper.getEntities(totalCnt);
    this.entitiesWithRelations = BenchmarkHelper.getRelationEntities(totalCnt);

    entities.forEach(e -> cache.put((ModelEntity) e));
    entitiesWithRelations.forEach(
        (roleEntity, userList) ->
            cache.put(
                ((RoleEntity) roleEntity).nameIdentifier(),
                ((RoleEntity) roleEntity).type(),
                SupportsRelationOperations.Type.ROLE_USER_REL,
                (List<E>) userList));
  }

  @Benchmark
  public long entityCacheSize() {
    return cache.size();
  }
}
