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

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/** Factory class for creating {@link MetaCache} instances. */
public class CacheFactory {
  /** Map of cache provider names to their implementations. */
  private static final Map<String, CacheProvider> PROVIDERS = new ConcurrentHashMap<>();

  static {
    ServiceLoader<CacheProvider> loader = ServiceLoader.load(CacheProvider.class);
    boolean b = loader.iterator().hasNext();
    System.out.println(b);
    for (CacheProvider provider : loader) {
      PROVIDERS.put(provider.name(), provider);
    }
  }

  /**
   * Returns a {@link MetaCache} instance for the specified cache provider and configuration.
   *
   * <p>The cache provider name is used to look up the corresponding {@link CacheProvider}
   * implementation. The configuration is passed to the provider to create the cache instance.
   *
   * @param name The name of the cache provider.
   * @param config The cache configuration.
   * @return A {@link MetaCache} instance for the specified cache provider and configuration.
   */
  public static MetaCache getMetaCache(String name, CacheConfig config) {
    CacheProvider provider = PROVIDERS.get(name);
    if (provider == null) {
      throw new IllegalArgumentException("No such cache provider: " + name);
    }
    return provider.getCache(config);
  }
}
