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

import org.apache.gravitino.meta.ModelEntity;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * EntityCacheContainsBenchmark benchmarks the performance of the {@link EntityCache#contains}
 * operation for individual entities.
 *
 * <p>This benchmark evaluates how efficiently the cache can determine whether a given entity
 * exists, under different data volumes (e.g., 10, 100, 1000).
 *
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class ContainsEntityCacheBenchmark extends AbstractEntityBenchmark {

  @Benchmark
  public boolean benchmarkContains() {
    int idx = random.nextInt(entities.size());
    ModelEntity sampleEntity = entities.get(idx);

    return cache.contains(sampleEntity.nameIdentifier(), sampleEntity.type());
  }
}
