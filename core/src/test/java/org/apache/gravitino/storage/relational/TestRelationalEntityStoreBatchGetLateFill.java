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
package org.apache.gravitino.storage.relational;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Verifies that {@link RelationalEntityStore#batchGet} cannot write an entity back into the cache
 * after that entity was invalidated while the backend read was in flight (the late-fill race).
 *
 * <p>The tests trigger invalidation during the backend read or during the cache write, so both
 * sides of the write-back check are covered without relying on thread timing.
 */
public class TestRelationalEntityStoreBatchGetLateFill {

  private static final Namespace SCHEMA_NS = Namespace.of("metalake", "catalog", "schema");

  private RelationalEntityStore store;
  private RelationalBackend backend;
  private CaffeineEntityCache cache;

  @BeforeEach
  void setUp() throws IllegalAccessException {
    store = new RelationalEntityStore();
    backend = Mockito.mock(RelationalBackend.class);
    cache = Mockito.spy(new CaffeineEntityCache(new Config() {}));
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", cache, true);
  }

  private static EntityChangeRecord dropRecord(NameIdentifier ident, Entity.EntityType type) {
    return new EntityChangeRecord(
        1L,
        ident.namespace().level(0),
        type.name(),
        EntityChangeLogNameIdentifierCodec.encode(ident),
        OperateType.DROP,
        0L);
  }

  @Test
  void testBatchGetWritesBackWhenNoInvalidationHappens() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE))).thenReturn(List.of(table));

    List<TableEntity> result =
        store.batchGet(List.of(table.nameIdentifier()), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertEquals(List.of(table), result);
    Assertions.assertTrue(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testBatchGetSkipsWriteBackWhenChangeLogInvalidatesDuringBackendRead() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    EntityChangeLogListener poller = store.newCacheChangeLogListener();
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE)))
        .thenAnswer(
            invocation -> {
              poller.onEntityChange(List.of(dropRecord(ident, Entity.EntityType.TABLE)));
              return List.of(table);
            });

    List<TableEntity> result =
        store.batchGet(List.of(ident), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertEquals(List.of(table), result);
    Assertions.assertFalse(
        cache.contains(ident, Entity.EntityType.TABLE),
        "a value invalidated during the backend read must not be written back");
  }

  @Test
  void testBatchGetSkipsWriteBackWhenLocalDeleteInvalidatesDuringBackendRead() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE)))
        .thenAnswer(
            invocation -> {
              store.delete(ident, Entity.EntityType.TABLE, false);
              return List.of(table);
            });

    store.batchGet(List.of(ident), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testBatchGetSkipsWriteBackWhenUnrelatedKeyInvalidatesDuringBackendRead() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier unrelatedIdent = NameIdentifier.of(SCHEMA_NS, "t2");
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE)))
        .thenAnswer(
            invocation -> {
              store.delete(unrelatedIdent, Entity.EntityType.TABLE, false);
              return List.of(table);
            });

    store.batchGet(List.of(table.nameIdentifier()), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testBatchGetDirectClearDuringBackendRead() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE)))
        .thenAnswer(
            invocation -> {
              store.clearCache();
              return List.of(table);
            });

    store.batchGet(List.of(ident), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testBatchGetRemovesValueWrittenAfterInvalidationDuringPut() throws IllegalAccessException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    RecordingCache recordingCache = new RecordingCache();
    // Advance the epoch from inside the write-back, while this key's cache lock is held. Invalidate
    // an unrelated entity so this entry is removed only by the post-put epoch check. A whole-cache
    // clear cannot run from inside the segment operation.
    NameIdentifier unrelatedIdent = NameIdentifier.of(SCHEMA_NS, "t2");
    recordingCache.beforePut =
        () -> {
          try {
            store.delete(unrelatedIdent, Entity.EntityType.TABLE, false);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };
    FieldUtils.writeField(store, "cache", recordingCache, true);
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE))).thenReturn(List.of(table));

    List<TableEntity> result =
        store.batchGet(List.of(table.nameIdentifier()), Entity.EntityType.TABLE, TableEntity.class);

    Assertions.assertEquals(List.of(table), result);
    Assertions.assertTrue(recordingCache.beforePutRan);
    Assertions.assertFalse(
        recordingCache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testBatchGetDoesNotLockNonCacheableRole() throws IllegalAccessException {
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build())
            .build();
    RecordingCache recordingCache = new RecordingCache();
    FieldUtils.writeField(store, "cache", recordingCache, true);
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.ROLE))).thenReturn(List.of(role));

    List<RoleEntity> result =
        store.batchGet(List.of(role.nameIdentifier()), Entity.EntityType.ROLE, RoleEntity.class);

    Assertions.assertEquals(List.of(role), result);
    Assertions.assertFalse(recordingCache.cacheLockUsed);
    Assertions.assertTrue(recordingCache.keyChangeHookCalled);
  }

  @Test
  void testBatchGetSkipsWriteBackWhenListenerFallsBackToClearDuringBackendRead() {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    EntityChangeLogListener poller = store.newCacheChangeLogListener();
    // A failed targeted invalidation makes the listener clear the whole cache; that must fence
    // in-flight fills too.
    Mockito.doThrow(new RuntimeException("boom"))
        .when(cache)
        .invalidate(ident, Entity.EntityType.TABLE);
    Mockito.when(backend.batchGet(any(), eq(Entity.EntityType.TABLE)))
        .thenAnswer(
            invocation -> {
              poller.onEntityChange(List.of(dropRecord(ident, Entity.EntityType.TABLE)));
              return List.of(table);
            });

    store.batchGet(List.of(ident), Entity.EntityType.TABLE, TableEntity.class);

    Mockito.verify(cache).clear();
    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.TABLE));
  }

  private static class RecordingCache extends CaffeineEntityCache {
    private boolean cacheLockUsed;
    private boolean keyChangeHookCalled;
    private boolean beforePutRan;
    private Runnable beforePut;

    RecordingCache() {
      super(new Config() {});
    }

    /** {@inheritDoc} */
    @Override
    public <E extends Exception> void withCacheLock(
        EntityCacheKey key, EntityCache.ThrowingRunnable<E> action) throws E {
      cacheLockUsed = true;
      super.withCacheLock(key, action);
    }

    /** {@inheritDoc} */
    @Override
    public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
      keyChangeHookCalled = true;
      super.invalidateOnKeyChange(entity);
    }

    /** {@inheritDoc} */
    @Override
    protected <E extends Entity & HasIdentifier> void doPut(E entity) {
      if (beforePut != null) {
        Runnable action = beforePut;
        beforePut = null;
        action.run();
        beforePutRan = true;
      }
      super.doPut(entity);
    }
  }
}
