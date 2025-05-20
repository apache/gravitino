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
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCachedEntityStore {

  private EntityStore entityStore;
  private EntityCache cache;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private SchemaEntity entity1;
  private FilesetEntity entity2;
  private SchemaEntity updatedEntity1;

  @BeforeAll
  public void beforeAll() throws IOException {
    ident1 = NameIdentifier.of("metalake", "catalog", "schema");
    ident2 = NameIdentifier.of("metalake", "catalog", "schema", "fileset");
    ident3 = NameIdentifier.of("metalake", "catalog", "schema", "not-exist-model");
    entity1 =
        SchemaEntity.builder()
            .withName("schema")
            .withId(1L)
            .withComment("test schema")
            .withNamespace(Namespace.of("metalake", "catalog"))
            .withProperties(ImmutableMap.of("key1", "value1"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("admin")
                    .withCreateTime(Instant.now())
                    .withLastModifier("admin")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();

    entity2 =
        FilesetEntity.builder()
            .withId(2L)
            .withName("fileset")
            .withComment("test fileset")
            .withNamespace(Namespace.of("metalake", "catalog", "schema"))
            .withFilesetType(Fileset.Type.EXTERNAL)
            .withStorageLocations(ImmutableMap.of("s3", "./data/fileset"))
            .withProperties(ImmutableMap.of("key2", "value2"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("admin")
                    .withCreateTime(Instant.now())
                    .withLastModifier("admin")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();

    updatedEntity1 =
        SchemaEntity.builder()
            .withName("schema")
            .withId(1L)
            .withComment("test schema")
            .withNamespace(Namespace.of("metalake", "catalog"))
            .withProperties(ImmutableMap.of("key1", "value1"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("admin")
                    .withCreateTime(Instant.now())
                    .withLastModifier("admin")
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();

    initMocks();
  }

  @BeforeEach
  public void setUp() {
    clearInvocations(entityStore);
    clearInvocations(cache);
  }

  @Test
  void testGetOperation() throws IOException {
    CachedEntityStore cachedEntityStore = new CachedEntityStore(entityStore, cache);

    SchemaEntity schemaEntity =
        cachedEntityStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    FilesetEntity filesetEntity =
        cachedEntityStore.get(ident2, Entity.EntityType.FILESET, FilesetEntity.class);

    Assertions.assertEquals(entity1, schemaEntity);
    Assertions.assertEquals(entity2, filesetEntity);
    verify(cache).getOrLoad(ident1, Entity.EntityType.SCHEMA);
    verify(cache).getOrLoad(ident2, Entity.EntityType.FILESET);
  }

  @Test
  void testDeleteOperation() throws IOException {
    CachedEntityStore cachedEntityStore = new CachedEntityStore(entityStore, cache);
    boolean deleteNonExists = cachedEntityStore.delete(ident3, Entity.EntityType.MODEL);
    boolean deleteFileset = cachedEntityStore.delete(ident2, Entity.EntityType.FILESET);
    boolean deleteSchema = cachedEntityStore.delete(ident1, Entity.EntityType.SCHEMA);

    Assertions.assertFalse(deleteNonExists);
    Assertions.assertTrue(deleteFileset);
    Assertions.assertTrue(deleteSchema);
    verify(cache).invalidate(ident1, Entity.EntityType.SCHEMA);
    verify(cache).invalidate(ident2, Entity.EntityType.FILESET);
    verify(cache).invalidate(ident3, Entity.EntityType.MODEL);

    verify(entityStore).delete(ident1, Entity.EntityType.SCHEMA, false);
    verify(entityStore).delete(ident2, Entity.EntityType.FILESET, false);
    verify(entityStore).delete(ident3, Entity.EntityType.MODEL, false);
  }

  @Test
  void testPutOperation() throws IOException {
    CachedEntityStore cachedEntityStore = new CachedEntityStore(entityStore, cache);
    doNothing().when(entityStore).put(entity1, false);
    doNothing().when(entityStore).put(entity2, false);

    doNothing().when(cache).put(entity1);
    doNothing().when(cache).put(entity2);

    cachedEntityStore.put(entity1);
    cachedEntityStore.put(entity2);

    verify(cache).put(entity1);
    verify(cache).put(entity2);
    verify(entityStore).put(entity1, false);
    verify(entityStore).put(entity2, false);
  }

  @Test
  void testUpdateOperation() throws IOException {
    CachedEntityStore cachedEntityStore = new CachedEntityStore(entityStore, cache);

    SchemaEntity updatedEntity =
        cachedEntityStore.update(ident1, SchemaEntity.class, Entity.EntityType.SCHEMA, s -> s);
    Assertions.assertEquals(updatedEntity1, updatedEntity);

    verify(cache).invalidate(ident1, Entity.EntityType.SCHEMA);
    verify(entityStore)
        .update(
            eq(ident1), eq(SchemaEntity.class), eq(Entity.EntityType.SCHEMA), any(Function.class));
  }

  private void initMocks() throws IOException {
    entityStore = mock(RelationalEntityStore.class);
    when(entityStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity1);
    when(entityStore.get(ident2, Entity.EntityType.FILESET, FilesetEntity.class))
        .thenReturn(entity2);

    when(entityStore.delete(ident1, Entity.EntityType.SCHEMA, false)).thenReturn(true);
    when(entityStore.delete(ident2, Entity.EntityType.FILESET, false)).thenReturn(true);
    when(entityStore.delete(ident3, Entity.EntityType.MODEL, false)).thenReturn(false);

    when(entityStore.update(
            eq(ident1), eq(SchemaEntity.class), eq(Entity.EntityType.SCHEMA), any(Function.class)))
        .thenReturn(updatedEntity1);

    cache = mock(EntityCache.class);
    when(cache.getOrLoad(ident1, Entity.EntityType.SCHEMA)).thenReturn(entity1);
    when(cache.getOrLoad(ident2, Entity.EntityType.FILESET)).thenReturn(entity2);

    when(cache.invalidate(ident1, Entity.EntityType.SCHEMA)).thenReturn(true);
    when(cache.invalidate(ident2, Entity.EntityType.FILESET)).thenReturn(true);
    when(cache.invalidate(ident3, Entity.EntityType.MODEL)).thenReturn(false);

    when(cache.withCacheLock(any(EntityCache.ThrowingSupplier.class)))
        .then(
            invocationOnMock -> {
              EntityCache.ThrowingSupplier<?, ?> supplier = invocationOnMock.getArgument(0);
              return supplier.get();
            });

    doAnswer(
            invocation -> {
              EntityCache.ThrowingRunnable<?> r = invocation.getArgument(0);
              r.run();
              return null;
            })
        .when(cache)
        .withCacheLock(any(EntityCache.ThrowingRunnable.class));
  }
}
