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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.storage.relational.service.CacheInvalidationVersionService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestCacheInvalidationCoordinator {

  private CacheInvalidationCoordinator coordinator;

  @AfterEach
  void tearDown() throws Exception {
    if (coordinator != null) {
      coordinator.close();
    }
  }

  @Test
  void testOnWriteBumpsVersionAndClearsCache() {
    Config config = new Config(false) {};
    CacheInvalidationVersionService versionService =
        Mockito.mock(CacheInvalidationVersionService.class);
    TrackingNoOpsCache cache = new TrackingNoOpsCache(config);

    Mockito.when(versionService.bumpVersion()).thenReturn(2L);

    coordinator = new CacheInvalidationCoordinator(config, cache, versionService);
    coordinator.onWriteSuccess();

    Assertions.assertEquals(1, cache.clearCount.get());
    Mockito.verify(versionService, Mockito.times(1)).bumpVersion();
  }

  @Test
  void testPollDetectsRemoteChangesAndClearsCache() {
    Config config = new Config(false) {};
    config.set(Configs.CACHE_INVALIDATION_POLL_INTERVAL_MS, 20L);

    CacheInvalidationVersionService versionService =
        Mockito.mock(CacheInvalidationVersionService.class);
    TrackingNoOpsCache cache = new TrackingNoOpsCache(config);

    Mockito.when(versionService.currentVersion()).thenReturn(0L, 3L);

    coordinator = new CacheInvalidationCoordinator(config, cache, versionService);
    coordinator.start();

    Awaitility.await().atMost(500, TimeUnit.MILLISECONDS).until(() -> cache.clearCount.get() >= 1);

    Assertions.assertTrue(cache.clearCount.get() >= 1);
  }

  private static class TrackingNoOpsCache extends NoOpsCache {
    private final AtomicInteger clearCount = new AtomicInteger(0);

    TrackingNoOpsCache(Config config) {
      super(config);
    }

    @Override
    public void clear() {
      clearCount.incrementAndGet();
    }

    @Override
    public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
        NameIdentifier ident, Entity.EntityType type) {
      return Optional.empty();
    }

    @Override
    public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
      return false;
    }

    @Override
    public boolean contains(NameIdentifier ident, Entity.EntityType type) {
      return false;
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(E entity) {}

    @Override
    public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {}

    @Override
    public <E extends Entity & HasIdentifier> Optional<java.util.List<E>> getIfPresent(
        SupportsRelationOperations.Type relType,
        NameIdentifier nameIdentifier,
        Entity.EntityType identType) {
      return Optional.empty();
    }

    @Override
    public boolean invalidate(
        NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType) {
      return false;
    }
  }
}
