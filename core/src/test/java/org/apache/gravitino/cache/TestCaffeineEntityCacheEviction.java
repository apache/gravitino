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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.TestUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that Caffeine eviction never waits for an entity segment lock, and that the prefix index
 * still tracks evicted keys correctly.
 *
 * <p>Caffeine runs maintenance and removal notifications inline when its executor rejects a task,
 * and does so while holding its eviction lock. A writer that holds a segment lock can at the same
 * time wait for that eviction lock inside {@code cacheData.put}, so the removal listener must not
 * take a segment lock on the thread Caffeine calls it on.
 */
public class TestCaffeineEntityCacheEviction {

  @Test
  void testEvictionCompletesWhileSegmentLocksAreHeld() throws Exception {
    ExecutorService evictor = Executors.newSingleThreadExecutor();
    try {
      // A direct executor reproduces the rejected-task path: Caffeine performs eviction and calls
      // the removal listener inline, under its eviction lock.
      CaffeineEntityCache cache = new CaffeineEntityCache(countBoundedConfig(1), Runnable::run);
      BaseMetalake first = TestUtil.getTestMetalake(1L, "metalake1", "first");
      BaseMetalake second = TestUtil.getTestMetalake(2L, "metalake2", "second");
      EntityCacheKey firstKey = metalakeKey(first);
      EntityCacheKey secondKey = metalakeKey(second);
      cache.put(first);

      // Hold both segment locks, so whichever entry Caffeine evicts, its listener would block here
      // if it took the segment lock.
      cache.withCacheLock(
          firstKey,
          () ->
              cache.withCacheLock(
                  secondKey,
                  () -> {
                    Future<?> eviction =
                        evictor.submit(
                            () -> {
                              // Bypass the segment lock to model Caffeine evicting on a thread that
                              // does not own the evicted key's segment.
                              cache.getCacheData().put(secondKey, second);
                              cache.getCacheData().cleanUp();
                            });
                    Assertions.assertDoesNotThrow(
                        () -> eviction.get(5, TimeUnit.SECONDS),
                        "eviction must not wait for a segment lock while it holds Caffeine's"
                            + " eviction lock");
                  }));

      Assertions.assertEquals(1, cache.getCacheData().estimatedSize());
      // Inline removal cleans up the index even while the segment locks remain held.
      Awaitility.await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  Assertions.assertEquals(
                      cache.getCacheData().policy().getIfPresentQuietly(firstKey) == null ? 0 : 1,
                      cache.size()));
    } finally {
      evictor.shutdownNow();
    }
  }

  @Test
  void testStaleEvictionDoesNotUnindexReinsertedEntry() {
    // Defer Caffeine's removal notifications so one can arrive after the key was put back.
    Queue<Runnable> deferredNotifications = new ArrayDeque<>();
    CaffeineEntityCache cache =
        new CaffeineEntityCache(countBoundedConfig(10), deferredNotifications::add);
    BaseMetalake metalake = TestUtil.getTestMetalake(1L, "metalake1", "metalake");
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(2L, "catalog1", Namespace.of("metalake1"), "hive", "child");
    EntityCacheKey catalogKey =
        EntityCacheKey.of(catalog.nameIdentifier(), Entity.EntityType.CATALOG);
    cache.put(catalog);
    runAll(deferredNotifications);

    // Evict everything; the notification for the catalog stays queued.
    cache.getCacheData().policy().eviction().get().setMaximum(0);
    cache.getCacheData().cleanUp();
    Assertions.assertNull(cache.getCacheData().policy().getIfPresentQuietly(catalogKey));
    cache.getCacheData().policy().eviction().get().setMaximum(10);

    // The catalog is cached again before the stale notification is processed.
    cache.put(catalog);
    runAll(deferredNotifications);

    Assertions.assertEquals(1, cache.size(), "the re-cached catalog must stay indexed");
    // A cascading invalidation finds descendants only through the index.
    cache.put(metalake);
    cache.invalidate(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
    Assertions.assertNull(
        cache.getCacheData().policy().getIfPresentQuietly(catalogKey),
        "invalidating the metalake must also drop its cached catalog");
  }

  @Test
  void testHeldSegmentDoesNotAccumulateEvictedKeys() throws Exception {
    CaffeineEntityCache cache = new CaffeineEntityCache(countBoundedConfig(64), Runnable::run);
    BaseMetalake held = TestUtil.getTestMetalake(1L, "held", "slow read");
    EntityCacheKey heldKey = metalakeKey(held);
    SegmentedLock locks = (SegmentedLock) FieldUtils.readField(cache, "segmentedLock", true);
    Lock heldSegment = locks.getSegmentLock(heldKey);
    cache.put(held);

    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      cache.withCacheLock(
          heldKey,
          () -> {
            // The worker evicts the held key on a different thread, so a callback cannot
            // reacquire this segment reentrantly. It must finish before this thread releases it.
            Future<?> eviction =
                writer.submit(
                    () -> {
                      cache.getCacheData().policy().eviction().get().setMaximum(0);
                      cache.getCacheData().cleanUp();
                      Assertions.assertEquals(0, cache.size());
                      cache.getCacheData().policy().eviction().get().setMaximum(64);
                      int inserted = 0;
                      for (long id = 100L; inserted < 2_000; id++) {
                        BaseMetalake entity =
                            TestUtil.getTestMetalake(id, "entry" + id, "eviction");
                        // The put itself must not wait for the segment deliberately held here.
                        if (locks.getSegmentLock(metalakeKey(entity)) == heldSegment) {
                          continue;
                        }
                        cache.put(entity);
                        cache.getCacheData().cleanUp();
                        Assertions.assertEquals(cache.getCacheData().asMap().size(), cache.size());
                        Assertions.assertTrue(
                            cache.size() <= 64, "evicted keys must not accumulate");
                        inserted++;
                      }
                    });
            Assertions.assertDoesNotThrow(
                () -> eviction.get(10, TimeUnit.SECONDS),
                "eviction and unrelated puts must finish while the segment remains locked");
          });
    } finally {
      writer.shutdownNow();
      Assertions.assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void testIndexRemovalPreservesLegacyCaffeineSubclassOverride() {
    List<EntityCacheKey> removedKeys = new ArrayList<>();
    CaffeineEntityCache cache =
        new CaffeineEntityCache(countBoundedConfig(1), Runnable::run) {
          /** {@inheritDoc} */
          @Deprecated
          @Override
          protected void invalidateExpiredItem(EntityCacheKey key) {
            removedKeys.add(key);
            super.invalidateExpiredItem(key);
          }
        };
    BaseMetalake entity = TestUtil.getTestMetalake(1L, "metalake", "legacy hook");
    EntityCacheKey key = metalakeKey(entity);
    cache.put(entity);
    cache.getCacheData().policy().eviction().get().setMaximum(0);
    cache.getCacheData().cleanUp();
    Assertions.assertEquals(List.of(key), removedKeys, "eviction must reach the legacy override");

    removedKeys.clear();
    cache.getCacheData().policy().eviction().get().setMaximum(1);
    cache.put(entity);
    cache.invalidate(entity.nameIdentifier(), entity.type());
    Assertions.assertEquals(List.of(key), removedKeys, "explicit invalidation must reach it too");
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testOversizedEntryRemovedBeforeIndexingWithInlineMaintenance() {
    Config config = new Config() {};
    config.set(Configs.CACHE_STATS_ENABLED, false);
    CaffeineEntityCache cache = new CaffeineEntityCache(config, Runnable::run);
    cache.getCacheData().policy().eviction().get().setMaximum(EntityCacheWeigher.SCHEMA_WEIGHT - 1);
    SchemaEntity schema = TestUtil.getTestSchemaEntity();
    EntityCacheKey key = EntityCacheKey.of(schema.nameIdentifier(), schema.type());

    cache.put(schema);

    Assertions.assertNull(cache.getCacheData().policy().getIfPresentQuietly(key));
    Assertions.assertEquals(0, cache.size(), "an entry already rejected must not be indexed");
  }

  @Test
  void testOversizedEntryRemainsIndexedUntilDeferredMaintenance() {
    Config config = new Config() {};
    config.set(Configs.CACHE_STATS_ENABLED, false);
    Queue<Runnable> maintenance = new ArrayDeque<>();
    CaffeineEntityCache cache = new CaffeineEntityCache(config, maintenance::add);
    cache.getCacheData().policy().eviction().get().setMaximum(EntityCacheWeigher.SCHEMA_WEIGHT - 1);
    runAll(maintenance);
    SchemaEntity schema = TestUtil.getTestSchemaEntity();
    EntityCacheKey key = EntityCacheKey.of(schema.nameIdentifier(), schema.type());

    cache.put(schema);

    Assertions.assertNotNull(cache.getCacheData().policy().getIfPresentQuietly(key));
    Assertions.assertEquals(1, cache.size(), "the presence check does not enforce admission");
    runAll(maintenance);
    Assertions.assertNull(cache.getCacheData().policy().getIfPresentQuietly(key));
    Assertions.assertEquals(0, cache.size(), "the SIZE notification must remove the index entry");
  }

  @Test
  void testClearBeforeStaleRemovalPreservesReinsertedEntry() {
    Queue<Runnable> deferredNotifications = new ArrayDeque<>();
    CaffeineEntityCache cache =
        new CaffeineEntityCache(countBoundedConfig(1), deferredNotifications::add);
    BaseMetalake entity = TestUtil.getTestMetalake(1L, "metalake", "reinserted");
    EntityCacheKey key = metalakeKey(entity);
    cache.put(entity);
    runAll(deferredNotifications);
    cache.getCacheData().policy().eviction().get().setMaximum(0);
    cache.getCacheData().cleanUp();
    cache.clear();
    cache.getCacheData().policy().eviction().get().setMaximum(1);
    cache.put(entity);
    runAll(deferredNotifications);

    Assertions.assertEquals(1, cache.size());
    Assertions.assertNotNull(cache.getCacheData().policy().getIfPresentQuietly(key));
    cache.invalidate(entity.nameIdentifier(), entity.type());
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testConcurrentEvictionUnderCountBoundCompletesAndKeepsIndexConsistent() throws Exception {
    int writers = 8;
    int putsPerWriter = 5_000;
    // The production executors: Caffeine's bounded cleanup pool runs rejected work inline. These
    // are
    // static, so if this deadlock regresses the shared cleanup thread stays stuck for this JVM and
    // later cache tests in the same fork can time out too; the timeout below reports it first.
    CaffeineEntityCache cache = new CaffeineEntityCache(countBoundedConfig(64));
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<?>> results = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        int writer = w;
        results.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < putsPerWriter; i++) {
                    long id = (long) writer * putsPerWriter + i;
                    cache.put(TestUtil.getTestMetalake(id, "metalake" + id, "stress"));
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> result : results) {
        Assertions.assertDoesNotThrow(
            () -> result.get(60, TimeUnit.SECONDS), "concurrent puts must not deadlock");
      }
    } finally {
      pool.shutdownNow();
    }

    cache.getCacheData().cleanUp();
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              cache.getCacheData().cleanUp();
              Assertions.assertEquals(cache.getCacheData().asMap().size(), cache.size());
            });
    Assertions.assertTrue(cache.size() <= 64);
  }

  private static void runAll(Queue<Runnable> tasks) {
    Runnable task;
    while ((task = tasks.poll()) != null) {
      task.run();
    }
  }

  private static EntityCacheKey metalakeKey(BaseMetalake metalake) {
    return EntityCacheKey.of(metalake.nameIdentifier(), Entity.EntityType.METALAKE);
  }

  private static Config countBoundedConfig(int maxEntries) {
    Config config = new Config() {};
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    config.set(Configs.CACHE_MAX_ENTRIES, maxEntries);
    config.set(Configs.CACHE_STATS_ENABLED, false);
    return config;
  }
}
