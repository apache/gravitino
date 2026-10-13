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
 * Verifies that {@link RelationalEntityStore#put} cannot write an entity back into the cache after
 * that entity was invalidated while the backend insert was in flight (the late-fill race).
 *
 * <p>The tests trigger invalidation during the backend insert or during the cache write, so both
 * sides of the write-back check are covered without relying on thread timing.
 */
public class TestRelationalEntityStorePutLateFill {

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
  void testPutWritesBackWhenNoInvalidationHappens() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    Mockito.doNothing().when(backend).insert(table, false);

    store.put(table, false);

    Assertions.assertTrue(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testPutSkipsWriteBackWhenChangeLogInvalidatesDuringBackendInsert() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    EntityChangeLogListener poller = store.newCacheChangeLogListener();
    Mockito.doAnswer(
            invocation -> {
              poller.onEntityChange(List.of(dropRecord(ident, Entity.EntityType.TABLE)));
              return null;
            })
        .when(backend)
        .insert(table, false);

    store.put(table, false);

    Assertions.assertFalse(
        cache.contains(ident, Entity.EntityType.TABLE),
        "a value invalidated during the backend insert must not be written back");
  }

  @Test
  void testPutSkipsWriteBackWhenLocalDeleteInvalidatesDuringBackendInsert() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    Mockito.doAnswer(
            invocation -> {
              store.delete(ident, Entity.EntityType.TABLE, false);
              return null;
            })
        .when(backend)
        .insert(table, false);

    store.put(table, false);

    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testPutSkipsWriteBackWhenUnrelatedKeyInvalidatesDuringBackendInsert() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier unrelatedIdent = NameIdentifier.of(SCHEMA_NS, "t2");
    Mockito.doAnswer(
            invocation -> {
              store.delete(unrelatedIdent, Entity.EntityType.TABLE, false);
              return null;
            })
        .when(backend)
        .insert(table, false);

    store.put(table, false);

    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testPutDirectClearDuringBackendInsert() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    Mockito.doAnswer(
            invocation -> {
              store.clearCache();
              return null;
            })
        .when(backend)
        .insert(table, false);

    store.put(table, false);

    Assertions.assertFalse(cache.contains(ident, Entity.EntityType.TABLE));
  }

  @Test
  void testPutRemovesValueWrittenAfterInvalidationDuringPut() throws Exception {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    RecordingCache recordingCache = new RecordingCache();
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
    Mockito.doNothing().when(backend).insert(table, false);

    store.put(table, false);

    Assertions.assertTrue(recordingCache.beforePutRan);
    Assertions.assertFalse(
        recordingCache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testPutDoesNotLockNonCacheableRole() throws Exception {
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
    Mockito.doNothing().when(backend).insert(role, false);

    store.put(role, false);

    Assertions.assertFalse(recordingCache.cacheLockUsed);
    Assertions.assertTrue(recordingCache.keyChangeHookCalled);
  }

  @Test
  void testPutSkipsWriteBackWhenListenerFallsBackToClearDuringBackendInsert() throws IOException {
    TableEntity table = TestUtil.getTestTableEntity(1L, "t1", SCHEMA_NS);
    NameIdentifier ident = table.nameIdentifier();
    EntityChangeLogListener poller = store.newCacheChangeLogListener();
    Mockito.doThrow(new RuntimeException("boom"))
        .when(cache)
        .invalidate(ident, Entity.EntityType.TABLE);
    Mockito.doAnswer(
            invocation -> {
              poller.onEntityChange(List.of(dropRecord(ident, Entity.EntityType.TABLE)));
              return null;
            })
        .when(backend)
        .insert(table, false);

    store.put(table, false);

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
