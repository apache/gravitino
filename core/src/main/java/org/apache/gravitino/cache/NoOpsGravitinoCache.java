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

import java.util.Optional;

/**
 * A no-op implementation of {@link GravitinoCache} that never caches anything. Useful for testing
 * and for environments where caching is disabled.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class NoOpsGravitinoCache<K, V> implements GravitinoCache<K, V> {

  @Override
  public Optional<V> getIfPresent(K key) {
    return Optional.empty();
  }

  @Override
  public void put(K key, V value) {
    // no-op
  }

  @Override
  public void invalidate(K key) {
    // no-op
  }

  @Override
  public void invalidateAll() {
    // no-op
  }

  @Override
  public void invalidateByPrefix(String prefix) {
    // no-op
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
