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

import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
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
 * <p>Each race is modelled deterministically by running the invalidation inside the backend stub:
 * the backend has already produced its (now stale) result, and the invalidation lands before {@code
 * batchGet} writes it back.
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
}
