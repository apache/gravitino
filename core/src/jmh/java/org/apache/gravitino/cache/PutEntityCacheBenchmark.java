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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

/**
 * EntityCachePutBenchmark benchmarks the performance of {@link EntityCache#put} operations for
 * standard entities.
 *
 * <p>Each benchmark invocation starts with a cleared cache to ensure that measurements reflect
 * fresh insertions without side effects from previous state.
 *
 * @see org.apache.gravitino.cache.EntityCache
 * @see org.openjdk.jmh.annotations.Benchmark
 */
public class PutEntityCacheBenchmark extends AbstractEntityBenchmark {

  @Setup(Level.Invocation)
  public void prepareForCachePut() {
    cache.clear();
  }

  @Benchmark
  public void benchmarkPut() {
    entities.forEach(e -> cache.put(e));
  }
}
