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

import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCacheFactory {

  @Test
  void testGetCache() {
    Config config = new Config() {};
    EntityCache entityCache = CacheFactory.getEntityCache(config);
    Assertions.assertInstanceOf(CaffeineEntityCache.class, entityCache);

    entityCache = CacheFactory.getEntityCache(config);
    Assertions.assertInstanceOf(CaffeineEntityCache.class, entityCache);
  }

  @Test
  void testCreateCacheWithInvalidName() {
    Config config = new Config() {};
    config.set(Configs.CACHE_IMPLEMENTATION, "InvalidCacheName");
    Assertions.assertThrows(RuntimeException.class, () -> CacheFactory.getEntityCache(config));
  }
}
