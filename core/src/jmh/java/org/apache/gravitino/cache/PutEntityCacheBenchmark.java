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
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

/**
 * EntityCachePutBenchmark benchmarks the performance of {@link EntityCache#put} operations for both
 * standard entities and relation-based entity groups.
 *
 * <p>Each benchmark invocation starts with a cleared cache to ensure that measurements reflect
 * fresh insertions without side effects from previous state.
 *
 * <p>This benchmark covers two insertion scenarios:
 *
 * <ul>
 *   <li>{@code benchmarkPut}: Inserts a batch of {@link ModelEntity} instances into the cache.
 *   <li>{@code benchmarkPutWithRelation}: Inserts relation mappings between {@link RoleEntity} and
 *       related entities (e.g., users), using a relation type as part of the cache key.
 * </ul>
 *
 * @param <E> the type of related entity, extending {@link Entity} and implementing {@link
 *     HasIdentifier}
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class PutEntityCacheBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityBenchmark {

  @Setup(Level.Invocation)
  public void prepareForCachePut() {
    cache.clear();
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public void benchmarkPut() {
    entities.forEach(e -> cache.put((ModelEntity) e));
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public void benchmarkPutWithRelation() {
    entitiesWithRelations.forEach(
        (role, relationEntities) ->
            cache.put(
                ((RoleEntity) role).nameIdentifier(),
                ((RoleEntity) role).type(),
                SupportsRelationOperations.Type.ROLE_USER_REL,
                (List<E>) relationEntities));
  }
}
