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

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

/** Factory class for creating {@link org.apache.gravitino.cache.EntityCache} instances. */
public final class CacheFactory {

  // Register EntityCache's short name to its full qualified class name in the map. So that user
  // doesn't need to specify the full qualified class name when creating an EntityCache instance.
  public static final ImmutableMap<String, String> ENTITY_CACHES =
      ImmutableMap.of("caffeine", CaffeineEntityCache.class.getCanonicalName());

  // Private constructor to prevent instantiation of this factory class.
  private CacheFactory() {}

  /**
   * Creates a new {@link org.apache.gravitino.cache.EntityCache} using the cache type specified in
   * the configuration.
   *
   * @param config The configuration.
   * @return A cache instance
   */
  public static EntityCache getEntityCache(Config config) {
    String name = config.get(Configs.CACHE_IMPLEMENTATION);
    String className = ENTITY_CACHES.getOrDefault(name, name);

    try {
      return (EntityCache)
          Class.forName(className).getDeclaredConstructor(Config.class).newInstance(config);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create and initialize EntityCache: " + name, e);
    }
  }
}
