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

import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * EntityCacheContainsBenchmark benchmarks the performance of the {@link EntityCache#contains}
 * operation for both regular entities and relation-based entries.
 *
 * <p>This benchmark evaluates how efficiently the cache can determine whether a given entity or a
 * relationship entry exists, under different data volumes (e.g., 10, 100, 1000).
 *
 * <p>It includes two benchmark methods:
 *
 * <ul>
 *   <li>{@code benchmarkContains}: Checks the presence of a single {@link ModelEntity} in the
 *       cache.
 *   <li>{@code benchmarkContainsWithRelation}: Checks whether a relation entry (e.g., role-to-user
 *       mapping) exists for a given {@link RoleEntity} and relation type.
 * </ul>
 *
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class ContainsEntityCacheBenchmark extends AbstractEntityBenchmark {

  @Benchmark
  public boolean benchmarkContains() {
    int idx = random.nextInt(entities.size());
    ModelEntity sampleEntity = (ModelEntity) entities.get(idx);

    return cache.contains(sampleEntity.nameIdentifier(), sampleEntity.type());
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public boolean benchmarkContainsWithRelation() {
    RoleEntity sampleRoleEntity = (RoleEntity) BenchmarkHelper.getRandomKey(entitiesWithRelations);

    return cache.contains(
        sampleRoleEntity.nameIdentifier(),
        sampleRoleEntity.type(),
        SupportsRelationOperations.Type.ROLE_USER_REL);
  }
}
