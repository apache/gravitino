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

package org.apache.gravitino.cache.provider;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.cache.CacheConfig;
import org.apache.gravitino.cache.MetaCache;

/**
 * Interface CacheProvider defines the standard methods for a cache provider to obtain cache
 * instances.
 */
public interface CacheProvider {

  /**
   * Gets the name of the cache provider.
   *
   * @return The name of the cache provider.
   */
  String name();

  /**
   * Gets a {@link MetaCache} instance based on the cache configuration.
   *
   * @param config The configuration information for the cache.
   * @return A cache instance initialized according to the configuration.
   */
  MetaCache getCache(CacheConfig config);

  /**
   * Gets a {@link MetaCache} instance based on the cache configuration and entity store.
   *
   * @param config The configuration information for the cache.
   * @param entityStore The associated entity store.
   * @return A cache instance initialized according to the configuration and entity store.
   */
  MetaCache getCache(CacheConfig config, EntityStore entityStore);
}
