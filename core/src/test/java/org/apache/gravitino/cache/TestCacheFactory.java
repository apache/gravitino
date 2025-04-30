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

import static org.mockito.Mockito.mock;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.cache.provider.CacheFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCacheFactory {
  private EntityStore mockStore;

  @BeforeAll
  void setUp() {
    mockStore = mock(EntityStore.class);
  }

  @Test
  void testCreateCache() {
    String cacheName = "Caffeine";
    MetaCache metaCache = CacheFactory.getMetaCache(cacheName, new CacheConfig(), mockStore);

    Assertions.assertEquals(CaffeineMetaCache.class, metaCache.getClass());

    cacheName = "Snapshot";
    metaCache = CacheFactory.getMetaCache(cacheName, new CacheConfig(), mockStore);
    Assertions.assertEquals(SnapshotMetaCache.class, metaCache.getClass());
  }

  @Test
  void testCreateCacheWithInvalidName() {
    String cacheName = "InvalidCacheName";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CacheFactory.getMetaCache(cacheName, new CacheConfig(), mockStore));
  }
}
