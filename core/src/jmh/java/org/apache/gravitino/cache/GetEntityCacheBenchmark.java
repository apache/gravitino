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
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * EntityCacheGetBenchmark benchmarks the performance of the {@link EntityCache#getIfPresent}
 * operation for both individual entities and entity relations.
 *
 * <p>This benchmark measures the efficiency of cache lookups under varying data sizes (e.g., 10,
 * 100, 1000 entries), helping evaluate the speed and consistency of the cache's get operations.
 *
 * <p>It includes two benchmark methods:
 *
 * <ul>
 *   <li>{@code benchmarkGet}: Retrieves a {@link ModelEntity} by its identifier and type.
 *   <li>{@code benchmarkGetWithRelations}: Retrieves a list of related entities (e.g., users under
 *       a role) using a relation type and a {@link RoleEntity} as the lookup key.
 * </ul>
 *
 * @param <E> the type of related entity, extending {@link Entity} and implementing {@link
 *     HasIdentifier}
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class GetEntityCacheBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityBenchmark {

  @Benchmark
  public Entity benchmarkGet() {
    int idx = random.nextInt(entities.size());
    ModelEntity sampleEntity = (ModelEntity) entities.get(idx);

    return cache.getIfPresent(sampleEntity.nameIdentifier(), sampleEntity.type()).orElse(null);
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public List<E> benchmarkGetWithRelations() {
    RoleEntity sampleRoleEntity = (RoleEntity) BenchmarkHelper.getRandomKey(entitiesWithRelations);

    Optional<List<E>> relationFromCache =
        cache.getIfPresent(
            SupportsRelationOperations.Type.ROLE_USER_REL,
            sampleRoleEntity.nameIdentifier(),
            sampleRoleEntity.type());

    return relationFromCache.orElse(null);
  }
}
