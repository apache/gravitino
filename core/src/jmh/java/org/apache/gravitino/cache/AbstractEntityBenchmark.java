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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * AbstractEntityBenchmark is a JMH-based benchmark base class designed to evaluate the performance
 * of the {@link EntityCache} implementation in Gravitino.
 *
 * <p>This class sets up a controlled environment where a configurable number of entities and their
 * associated relations are preloaded into the cache before each iteration. It supports benchmarking
 * cache operations such as put, get, and relation-based access under varying data volumes.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Supports benchmarking with different entity counts (e.g., 10, 100, 1000)
 *   <li>Measures both throughput and average execution time using JMH modes
 *   <li>Preloads both simple entities and relation entities (e.g., user-role mappings)
 * </ul>
 *
 * <p>Subclasses should implement benchmark methods to evaluate specific cache operations.
 *
 * @param <E> the type of entity under test, must implement {@link Entity} and {@link HasIdentifier}
 * @see org.apache.gravitino.cache.CaffeineEntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 * @see org.apache.gravitino.cache.BenchmarkHelper
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public abstract class AbstractEntityBenchmark<E extends Entity & HasIdentifier> {
  @Param({"10", "100", "1000"})
  public int totalCnt;

  protected EntityCache cache;
  protected List<ModelEntity> entities;
  protected Map<RoleEntity, List<E>> entitiesWithRelations;
  protected Random random = new Random();

  @Setup(Level.Iteration)
  public void setup() {
    Config config = new Config() {};
    this.cache = new CaffeineEntityCache(config);
    this.entities = BenchmarkHelper.getEntities(totalCnt);
    this.entitiesWithRelations = BenchmarkHelper.getRelationEntities(totalCnt);

    entities.forEach(cache::put);
    entitiesWithRelations.forEach(
        (roleEntity, userList) ->
            cache.put(
                roleEntity.nameIdentifier(),
                roleEntity.type(),
                SupportsRelationOperations.Type.ROLE_USER_REL,
                userList));
  }
}
