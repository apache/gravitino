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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.BenchmarkHelper;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * A JMH benchmark class that simulates a mixed workload over the EntityStore system, combining
 * multiple core operations in a realistic access ratio.
 *
 * <p>This benchmark is designed to reflect a read-heavy, concurrent usage pattern, which helps
 * evaluate the performance impact of typical usage scenarios, including:
 *
 * <ul>
 *   <li>60% {@code get} operations to retrieve entities by identifier
 *   <li>30% {@code listRelation} operations to query relation-based entities
 *   <li>10% {@code contains} operations to check entity existence
 * </ul>
 *
 * <p>Each benchmark method is annotated with {@code @Group} and {@code @GroupThreads} to simulate
 * mixed operation concurrency under shared state.
 *
 * <p>The benchmark runs under both {@code Throughput} and {@code AverageTime} modes, providing a
 * comprehensive view of system performance in terms of latency and throughput.
 *
 * <p>This benchmark can be used to identify performance bottlenecks, cache efficiency, and
 * concurrency behavior of the EntityStore implementation, especially when caching is enabled or
 * disabled.
 *
 * @param <E> the type of entity under test, which must implement both {@link Entity} and {@link
 *     HasIdentifier}
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public class MixEntityStorageBenchmark<E extends Entity & HasIdentifier>
    extends AbstractEntityStorageBenchmark {
  public static final int GET_ENTITY_RATIO = 6;
  public static final int CONTAINS_RATIO = 1;
  public static final int LIST_RELATION_RATIO = 3;

  private final Random random = ThreadLocalRandom.current();

  /**
   * Test get from store.
   *
   * @return The entity.
   * @throws IOException if an I/O error occurs.
   */
  @Benchmark
  @Group("ops")
  @GroupThreads(GET_ENTITY_RATIO)
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

  /**
   * Test list entities with relation.
   *
   * @return The list of entities.
   * @throws IOException if an I/O error occurs.
   */
  @Benchmark
  @Group("ops")
  @GroupThreads(LIST_RELATION_RATIO)
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

  /**
   * Test exists in store.
   *
   * @return True if the entity exists in the store.
   * @throws IOException if an I/O error occurs.
   */
  @Benchmark
  @Group("ops")
  @GroupThreads(CONTAINS_RATIO)
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
