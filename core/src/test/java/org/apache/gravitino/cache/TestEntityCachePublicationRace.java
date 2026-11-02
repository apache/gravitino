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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.RelationalBackend;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Deterministic regression probes for cache invalidation races. */
public class TestEntityCachePublicationRace {
  @Test
  void testPointReadDoesNotReacquireSpiLockForPublication() throws Exception {
    NonReentrantCache cache = new NonReentrantCache();
    RelationalEntityStore store = new RelationalEntityStore();
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    setField(store, RelationalEntityStore.class, "cache", cache);
    setField(store, RelationalEntityStore.class, "backend", backend);
    TableEntity entity = table("non_reentrant");
    Mockito.when(backend.get(entity.nameIdentifier(), EntityType.TABLE)).thenReturn(entity);
    Assertions.assertSame(
        entity, store.get(entity.nameIdentifier(), EntityType.TABLE, TableEntity.class));
    Assertions.assertEquals(1, cache.acquisitions);
    Mockito.verify(backend).get(entity.nameIdentifier(), EntityType.TABLE);
  }

  @Test
  void testParentDropMustFenceAnInflightChildPointRead() throws Exception {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    RelationalEntityStore store = new RelationalEntityStore();
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    setField(store, RelationalEntityStore.class, "cache", cache);
    setField(store, RelationalEntityStore.class, "backend", backend);
    NameIdentifier parent = NameIdentifier.of("lake", "catalog", "schema");
    Field lockField = CaffeineEntityCache.class.getDeclaredField("segmentedLock");
    lockField.setAccessible(true);
    SegmentedLock locks = (SegmentedLock) lockField.get(cache);
    // Enum identity hash codes differ across JVMs. Select keys on distinct real segments instead
    // of assuming fixed names always exercise the ancestor/child race.
    TableEntity old =
        IntStream.range(0, 1000)
            .mapToObj(i -> table("table" + i))
            .filter(
                t ->
                    locks.getSegmentLock(EntityCacheKey.of(parent, EntityType.SCHEMA))
                        != locks.getSegmentLock(
                            EntityCacheKey.of(t.nameIdentifier(), EntityType.TABLE)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No distinct child cache segment"));
    Assertions.assertNotSame(
        locks.getSegmentLock(EntityCacheKey.of(parent, EntityType.SCHEMA)),
        locks.getSegmentLock(EntityCacheKey.of(old.nameIdentifier(), EntityType.TABLE)));
    CountDownLatch readOld = new CountDownLatch(1);
    CountDownLatch finishRead = new CountDownLatch(1);
    Mockito.when(backend.get(old.nameIdentifier(), EntityType.TABLE))
        .thenAnswer(
            invocation -> {
              readOld.countDown();
              Assertions.assertTrue(finishRead.await(10, TimeUnit.SECONDS));
              return old;
            });
    ExecutorService readers = Executors.newSingleThreadExecutor();
    try {
      Future<TableEntity> read =
          readers.submit(
              () -> store.get(old.nameIdentifier(), EntityType.TABLE, TableEntity.class));
      Assertions.assertTrue(readOld.await(10, TimeUnit.SECONDS));
      Mockito.when(backend.delete(parent, EntityType.SCHEMA, true)).thenReturn(true);
      Assertions.assertTrue(store.delete(parent, EntityType.SCHEMA, true));
      finishRead.countDown();
      read.get(10, TimeUnit.SECONDS);
      Assertions.assertFalse(
          cache.contains(old.nameIdentifier(), EntityType.TABLE),
          "A child loaded before the committed parent drop must not survive its invalidation");
    } finally {
      finishRead.countDown();
      readers.shutdownNow();
      try {
        Assertions.assertTrue(readers.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        cache.clear();
      }
    }
  }

  @Test
  void testParentDropFencesDelayedInsertPublication() throws Exception {
    CaffeineEntityCache cache = new CaffeineEntityCache(new Config() {});
    RelationalEntityStore store = new RelationalEntityStore();
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    setField(store, RelationalEntityStore.class, "cache", cache);
    setField(store, RelationalEntityStore.class, "backend", backend);
    TableEntity entity = table("inserted");
    NameIdentifier parent = NameIdentifier.of("lake", "catalog", "schema");
    CountDownLatch insertionCompleted = new CountDownLatch(1);
    CountDownLatch returnFromInsert = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              insertionCompleted.countDown();
              Assertions.assertTrue(returnFromInsert.await(10, TimeUnit.SECONDS));
              return null;
            })
        .when(backend)
        .insert(entity, false);
    Mockito.when(backend.delete(parent, EntityType.SCHEMA, true)).thenReturn(true);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      Future<?> write =
          writer.submit(
              () -> {
                store.put(entity, false);
                return null;
              });
      Assertions.assertTrue(insertionCompleted.await(10, TimeUnit.SECONDS));
      Assertions.assertTrue(store.delete(parent, EntityType.SCHEMA, true));
      returnFromInsert.countDown();
      write.get(10, TimeUnit.SECONDS);
      Assertions.assertFalse(cache.contains(entity.nameIdentifier(), EntityType.TABLE));
    } finally {
      returnFromInsert.countDown();
      writer.shutdownNow();
      try {
        Assertions.assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        cache.clear();
      }
    }
  }

  @Test
  void testInvalidationDuringPublicationRemovesJustCachedValue() throws Exception {
    PublicationHookCache cache = new PublicationHookCache();
    RelationalEntityStore store = new RelationalEntityStore();
    RelationalBackend backend = Mockito.mock(RelationalBackend.class);
    setField(store, RelationalEntityStore.class, "cache", cache);
    setField(store, RelationalEntityStore.class, "backend", backend);
    NameIdentifier parent = NameIdentifier.of("lake", "catalog", "schema");
    Field lockField = CaffeineEntityCache.class.getDeclaredField("segmentedLock");
    lockField.setAccessible(true);
    SegmentedLock locks = (SegmentedLock) lockField.get(cache);
    TableEntity entity =
        IntStream.range(0, 1000)
            .mapToObj(i -> table("published" + i))
            .filter(
                t ->
                    locks.getSegmentLock(EntityCacheKey.of(parent, EntityType.SCHEMA))
                        != locks.getSegmentLock(
                            EntityCacheKey.of(t.nameIdentifier(), EntityType.TABLE)))
            .findFirst()
            .orElseThrow();
    Mockito.when(backend.get(entity.nameIdentifier(), EntityType.TABLE)).thenReturn(entity);
    Mockito.when(backend.delete(parent, EntityType.SCHEMA, true)).thenReturn(true);
    cache.beforePut =
        () -> {
          try {
            Assertions.assertTrue(store.delete(parent, EntityType.SCHEMA, true));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
    try {
      Assertions.assertSame(
          entity, store.get(entity.nameIdentifier(), EntityType.TABLE, TableEntity.class));
      Assertions.assertFalse(cache.contains(entity.nameIdentifier(), EntityType.TABLE));
    } finally {
      cache.clear();
    }
  }

  private static TableEntity table(String name) {
    return TableEntity.builder()
        .withId(123L)
        .withName(name)
        .withNamespace(Namespace.of("lake", "catalog", "schema"))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }

  private static void setField(Object instance, Class<?> clazz, String name, Object value)
      throws Exception {
    Field field = clazz.getDeclaredField(name);
    field.setAccessible(true);
    field.set(instance, value);
  }

  private static class NonReentrantCache extends NoOpsCache {
    private final Semaphore lock = new Semaphore(1);
    private int acquisitions;

    private NonReentrantCache() {
      super(new Config() {});
    }

    /** {@inheritDoc} */
    @Override
    public <T, E extends Exception> T withCacheLock(
        EntityCacheKey key, ThrowingSupplier<T, E> action) throws E {
      // Fail immediately rather than hanging the suite when the caller tries to reenter.
      Assertions.assertTrue(lock.tryAcquire(), "EntityCache SPI lock was acquired recursively");
      acquisitions++;
      try {
        return action.get();
      } finally {
        lock.release();
      }
    }

    /** {@inheritDoc} */
    @Override
    public <E extends Exception> void withCacheLock(EntityCacheKey key, ThrowingRunnable<E> action)
        throws E {
      withCacheLock(
          key,
          () -> {
            action.run();
            return null;
          });
    }
  }

  private static class PublicationHookCache extends CaffeineEntityCache {
    private Runnable beforePut = () -> {};

    private PublicationHookCache() {
      super(new Config() {});
    }

    /** {@inheritDoc} */
    @Override
    protected <E extends Entity & HasIdentifier> void doPut(E entity) {
      beforePut.run();
      super.doPut(entity);
    }
  }
}
