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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.BenchmarkHelper;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmark for testing the performance of listing entities via relation operations in a {@link
 * RelationalEntityStore}.
 *
 * <p>This benchmark simulates a common metadata query pattern: retrieving all related entities
 * (e.g., users) associated with a given entity (e.g., topic) by a specific relation type (such as
 * OWNER_REL).
 *
 * <p>The test randomly selects a preloaded {@link UserEntity}–{@link TopicEntity} pair and invokes
 * {@code listEntitiesByRelation} to fetch related entities from the store. It validates that the
 * result is non-empty to ensure correctness.
 *
 * <p>This benchmark reflects the cost and scalability of relation-based metadata queries under
 * load.
 *
 * @see RelationalEntityStore#listEntitiesByRelation(SupportsRelationOperations.Type,
 *     org.apache.gravitino.NameIdentifier, Entity.EntityType, boolean)
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class ListRelationEntityStorageBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityStorageBenchmark {

  /**
   * Benchmark for list entities by relation operation.
   *
   * @return the entity with relation from store.
   * @throws IOException if there is an error getting the entity from the store.
   */
  @Benchmark
  public List<E> benchmarkGetWithRelation() throws IOException {
    UserEntity user = (UserEntity) BenchmarkHelper.getRandomKey(entitiesWithRelations);
    TopicEntity topic = (TopicEntity) entitiesWithRelations.get(user);

    List<E> entitiesFromStore =
        ((RelationalEntityStore) store)
            .listEntitiesByRelation(
                SupportsRelationOperations.Type.OWNER_REL,
                topic.nameIdentifier(),
                Entity.EntityType.TOPIC,
                true);

    if (entitiesFromStore.isEmpty()) {
      throw new RuntimeException("User list size does not match");
    }

    return entitiesFromStore;
  }
}
