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
import org.apache.gravitino.cache.CaffeineMetaCache;
import org.apache.gravitino.cache.MetaCache;

/**
 * CaffeineCacheProvider is a CacheProvider implementation that uses the Caffeine library for
 * caching. It creates caches with configurations for maximum size and other parameters.
 */
public class CaffeineCacheProvider implements CacheProvider {

  /** {@inheritDoc} */
  @Override
  public String name() {
    return "Caffeine";
  }

  /** {@inheritDoc} */
  @Override
  public MetaCache getCache(CacheConfig config) {
    return CaffeineMetaCache.getInstance(config);
  }

  /** {@inheritDoc} */
  @Override
  public MetaCache getCache(CacheConfig config, EntityStore entityStore) {
    return CaffeineMetaCache.getInstance(config, entityStore);
  }

  /** {@inheritDoc} */
  @Override
  public MetaCache getCache() {
    return CaffeineMetaCache.getInstance();
  }
}
