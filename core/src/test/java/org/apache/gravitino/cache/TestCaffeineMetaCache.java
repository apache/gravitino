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

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCaffeineMetaCache {

  private EntityStore mockStore;
  private CaffeineMetaCache real;
  private CaffeineMetaCache cache;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;
  private NameIdentifier ident7;

  // Test Entities
  private CatalogEntity catalog;
  private SchemaEntity schema1;
  private SchemaEntity schema2;
  private TableEntity table1;
  private TableEntity table2;
  private TableEntity table3;
  private BaseMetalake metalake;
  private Map<NameIdentifier, Long> idMap;

  @BeforeAll
  void init() throws IOException {
    initTestNameIdentifier();
    initTestEntities();
    initMockStore();
  }

  @BeforeEach
  void restoreStore() {
    clearInvocations(mockStore);
    CaffeineMetaCache.resetForTest();
    real = CaffeineMetaCache.getInstance(new CacheConfig(), mockStore);
    cache = spy(real);
    initIdMock();
  }

  @Test
  void testPutAllTypeInCache() throws IOException {
    BaseMetalake testMetalake = TestUtil.getTestMetalake();
    CatalogEntity testCatalogEntity = TestUtil.getTestCatalogEntity();
    SchemaEntity testSchemaEntity = TestUtil.getTestSchemaEntity();
    TableEntity testTableEntity = TestUtil.getTestTableEntity();
    ModelEntity testModelEntity = TestUtil.getTestModelEntity();
    FilesetEntity testFileSetEntity = TestUtil.getTestFileSetEntity();
    TopicEntity testTopicEntity = TestUtil.getTestTopicEntity();
    TagEntity testTagEntity = TestUtil.getTestTagEntity();

    when(mockStore.get(testMetalake.nameIdentifier(), testMetalake.type(), BaseMetalake.class))
        .thenReturn(testMetalake);
    when(mockStore.get(
            testCatalogEntity.nameIdentifier(), testCatalogEntity.type(), CatalogEntity.class))
        .thenReturn(testCatalogEntity);
    when(mockStore.get(
            testSchemaEntity.nameIdentifier(), testSchemaEntity.type(), SchemaEntity.class))
        .thenReturn(testSchemaEntity);
    when(mockStore.get(testTableEntity.nameIdentifier(), testTableEntity.type(), TableEntity.class))
        .thenReturn(testTableEntity);
    when(mockStore.get(testModelEntity.nameIdentifier(), testModelEntity.type(), ModelEntity.class))
        .thenReturn(testModelEntity);
    when(mockStore.get(
            testFileSetEntity.nameIdentifier(), testFileSetEntity.type(), FilesetEntity.class))
        .thenReturn(testFileSetEntity);
    when(mockStore.get(testTopicEntity.nameIdentifier(), testTopicEntity.type(), TopicEntity.class))
        .thenReturn(testTopicEntity);
    when(mockStore.get(testTagEntity.nameIdentifier(), testTagEntity.type(), TagEntity.class))
        .thenReturn(testTagEntity);

    cache.getOrLoadMetadataByName(testMetalake.nameIdentifier(), testMetalake.type());
    cache.getOrLoadMetadataByName(testCatalogEntity.nameIdentifier(), testCatalogEntity.type());
    cache.getOrLoadMetadataByName(testSchemaEntity.nameIdentifier(), testSchemaEntity.type());
    cache.getOrLoadMetadataByName(testTableEntity.nameIdentifier(), testTableEntity.type());
    cache.getOrLoadMetadataByName(testModelEntity.nameIdentifier(), testModelEntity.type());
    cache.getOrLoadMetadataByName(testFileSetEntity.nameIdentifier(), testFileSetEntity.type());
    cache.getOrLoadMetadataByName(testTopicEntity.nameIdentifier(), testTopicEntity.type());
    cache.getOrLoadMetadataByName(testTagEntity.nameIdentifier(), testTagEntity.type());

    Assertions.assertEquals(8, cache.sizeOfCacheData());
    Assertions.assertEquals(8, cache.sizeOfCacheIndex());

    // validate Entities
    Assertions.assertTrue(cache.containsByName(testMetalake.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testCatalogEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testSchemaEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testTableEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testModelEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testFileSetEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testTopicEntity.nameIdentifier()));
    Assertions.assertTrue(cache.containsByName(testTagEntity.nameIdentifier()));
  }

  @Test
  void testLoadFromDBByName() throws IOException {
    Entity loadEntity = cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    verify(mockStore, times(1)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema1, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    verify(mockStore, times(1)).get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema2, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    verify(mockStore, times(1)).get(ident3, Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(table1, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    verify(mockStore, times(1)).get(ident4, Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(table2, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    verify(mockStore, times(1)).get(ident5, Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(table3, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);
    verify(mockStore, times(1)).get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals(catalog, loadEntity);

    loadEntity = cache.getOrLoadMetadataByName(ident7, Entity.EntityType.METALAKE);
    verify(mockStore, times(1)).get(ident7, Entity.EntityType.METALAKE, BaseMetalake.class);
    Assertions.assertEquals(metalake, loadEntity);
  }

  @Test
  void testLoadFromDBById() {
    Entity loadEntity = cache.getOrLoadMetadataById(1L, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(SchemaEntity.class, loadEntity.getClass());
    SchemaEntity schemaEntityLoadFromDB = (SchemaEntity) loadEntity;
    Assertions.assertEquals(schema1, schemaEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(2L, Entity.EntityType.SCHEMA);
    Assertions.assertEquals(SchemaEntity.class, loadEntity.getClass());
    schemaEntityLoadFromDB = (SchemaEntity) loadEntity;
    Assertions.assertEquals(schema2, schemaEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(2L, Entity.EntityType.TABLE);
    Assertions.assertEquals(TableEntity.class, loadEntity.getClass());
    TableEntity tableEntityLoadFromDB = (TableEntity) loadEntity;
    Assertions.assertEquals(table1, tableEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(3L, Entity.EntityType.TABLE);
    Assertions.assertEquals(TableEntity.class, loadEntity.getClass());
    tableEntityLoadFromDB = (TableEntity) loadEntity;
    Assertions.assertEquals(table2, tableEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(4L, Entity.EntityType.TABLE);
    Assertions.assertEquals(TableEntity.class, loadEntity.getClass());
    tableEntityLoadFromDB = (TableEntity) loadEntity;
    Assertions.assertEquals(table3, tableEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(5L, Entity.EntityType.CATALOG);
    Assertions.assertEquals(CatalogEntity.class, loadEntity.getClass());
    CatalogEntity catalogEntityLoadFromDB = (CatalogEntity) loadEntity;
    Assertions.assertEquals(catalog, catalogEntityLoadFromDB);

    loadEntity = cache.getOrLoadMetadataById(6L, Entity.EntityType.METALAKE);
    Assertions.assertEquals(BaseMetalake.class, loadEntity.getClass());
    BaseMetalake metalakeEntityLoadFromDB = (BaseMetalake) loadEntity;
    Assertions.assertEquals(metalake, metalakeEntityLoadFromDB);
  }

  @Test
  void testLoadMetalakeFromCacheByName() throws IOException {
    cache.getOrLoadMetadataByName(ident7, Entity.EntityType.METALAKE);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident7, Entity.EntityType.METALAKE);

    verify(mockStore, times(1)).get(ident7, Entity.EntityType.METALAKE, BaseMetalake.class);
    Assertions.assertEquals(metalake, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident7));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadCatalogFromCacheByName() throws IOException {
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    verify(mockStore, times(1)).get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals(catalog, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident6));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadSchemaFromCacheByName() throws IOException {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);

    verify(mockStore, times(1)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema1, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident1));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadTableFromCacheByName() throws IOException {
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);

    verify(mockStore, times(1)).get(ident3, Entity.EntityType.TABLE, TableEntity.class);
    Assertions.assertEquals(table1, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident3));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadMetalakeFromCacheById() {
    cache.getOrLoadMetadataById(6L, Entity.EntityType.METALAKE);
    Entity loadEntity = cache.getOrLoadMetadataById(6L, Entity.EntityType.METALAKE);

    Assertions.assertEquals(metalake, loadEntity);
    Assertions.assertTrue(cache.containsById(6L, Entity.EntityType.METALAKE));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadCatalogFromCacheById() throws IOException {
    cache.getOrLoadMetadataById(5L, Entity.EntityType.CATALOG);
    Entity loadEntity = cache.getOrLoadMetadataById(5L, Entity.EntityType.CATALOG);

    Assertions.assertEquals(catalog, loadEntity);
    Assertions.assertTrue(cache.containsById(5L, Entity.EntityType.CATALOG));

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadSchemaFromCacheById() throws IOException {
    cache.getOrLoadMetadataById(1L, Entity.EntityType.SCHEMA);
    Entity loadEntity = cache.getOrLoadMetadataById(1L, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(schema1, loadEntity);

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadTableFromCacheById() throws IOException {
    cache.getOrLoadMetadataById(2L, Entity.EntityType.TABLE);
    Entity loadEntity = cache.getOrLoadMetadataById(2L, Entity.EntityType.TABLE);

    Assertions.assertEquals(table1, loadEntity);

    // validate cache size
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testRemoveTableEntityByName() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeByName(ident3);

    // validate cache size
    Assertions.assertFalse(cache.containsByName(ident3));
    Assertions.assertEquals(5, cache.sizeOfCacheData());
    Assertions.assertEquals(5, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident1));
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));
    Assertions.assertTrue(cache.containsByName(ident5));
    Assertions.assertTrue(cache.containsByName(ident6));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident1), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident5), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident6), Entity.EntityType.CATALOG));
  }

  @Test
  void testRemoveTableEntityById() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeById(idMap.get(ident3), Entity.EntityType.TABLE);

    // validate cache size
    Assertions.assertFalse(cache.containsByName(ident3));
    Assertions.assertEquals(5, cache.sizeOfCacheData());
    Assertions.assertEquals(5, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident1));
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));
    Assertions.assertTrue(cache.containsByName(ident5));
    Assertions.assertTrue(cache.containsByName(ident6));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident1), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident5), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident6), Entity.EntityType.CATALOG));
  }

  @Test
  void testRemoveSchemaEntityByName() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeByName(ident1);

    // validate cache size
    Assertions.assertEquals(4, cache.sizeOfCacheData());
    Assertions.assertEquals(4, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));
    Assertions.assertTrue(cache.containsByName(ident5));
    Assertions.assertTrue(cache.containsByName(ident6));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident5), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident6), Entity.EntityType.CATALOG));
  }

  @Test
  void testRemoveSchemaEntityById() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeById(idMap.get(ident1), Entity.EntityType.SCHEMA);

    // validate cache size
    Assertions.assertEquals(4, cache.sizeOfCacheData());
    Assertions.assertEquals(4, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));
    Assertions.assertTrue(cache.containsByName(ident5));
    Assertions.assertTrue(cache.containsByName(ident6));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident5), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.containsById(idMap.get(ident6), Entity.EntityType.CATALOG));
  }

  @Test
  void testRemoveCatalogEntityByName() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeByName(ident6);

    // validate cache
    Assertions.assertEquals(2, cache.sizeOfCacheData());
    Assertions.assertEquals(2, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
  }

  @Test
  void testRemoveCatalogEntityById() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeById(idMap.get(ident6), Entity.EntityType.CATALOG);

    // validate cache
    Assertions.assertEquals(2, cache.sizeOfCacheData());
    Assertions.assertEquals(2, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident2));
    Assertions.assertTrue(cache.containsByName(ident4));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.containsById(idMap.get(ident4), Entity.EntityType.TABLE));
  }

  @Test
  void testRemoveMetalakeEntityByName() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);
    cache.getOrLoadMetadataByName(ident7, Entity.EntityType.METALAKE);

    cache.removeByName(ident7);

    // validate cache
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsByName(ident2));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
  }

  @Test
  void testRemoveMetalakeEntityById() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);
    cache.getOrLoadMetadataByName(ident7, Entity.EntityType.METALAKE);

    cache.removeById(idMap.get(ident7), Entity.EntityType.METALAKE);

    // validate cache
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertTrue(cache.containsById(2L, Entity.EntityType.SCHEMA));

    // validate by id
    Assertions.assertTrue(cache.containsById(idMap.get(ident2), Entity.EntityType.SCHEMA));
  }

  @Test
  void testClearCache() {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.clear();

    // validate cache
    Assertions.assertEquals(0, cache.sizeOfCacheData());
    Assertions.assertEquals(0, cache.sizeOfCacheIndex());

    // validate by name
    Assertions.assertFalse(cache.containsByName(ident1));
    Assertions.assertFalse(cache.containsByName(ident2));
    Assertions.assertFalse(cache.containsByName(ident3));
    Assertions.assertFalse(cache.containsByName(ident4));
    Assertions.assertFalse(cache.containsByName(ident5));
    Assertions.assertFalse(cache.containsByName(ident6));
  }

  @Test
  void testConcurrentGetAndRemove() throws InterruptedException {
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);

    int threadNum = 100;
    ExecutorService exec = Executors.newFixedThreadPool(threadNum);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadNum);

    for (int i = 0; i < threadNum; i++) {
      final boolean remover = (i % 2 == 0);
      exec.execute(
          () -> {
            try {
              start.await();
              if (remover) {
                cache.removeByName(ident1);
              } else {
                cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
              }
            } catch (Exception e) {
              Assertions.fail("throw exception in thread", e);
            } finally {
              done.countDown();
            }
          });
    }

    start.countDown();
    done.await();
    exec.shutdownNow();

    Assertions.assertTrue(
        (cache.sizeOfCacheData() == 1 && cache.sizeOfCacheIndex() == 1)
            || (cache.sizeOfCacheData() == 0 && cache.sizeOfCacheIndex() == 0));
  }

  @Test
  void testRemoveNonExistentEntity() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.removeById(9999L, Entity.EntityType.TABLE));

    NameIdentifier unknownIdent = NameIdentifier.of("metalakeZ", "catalogZ", "schemaZ", "tableZ");
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.removeByName(unknownIdent));
  }

  @Test
  void testLoadEntityThrowException() throws IOException {
    NameIdentifier badIdent = NameIdentifier.of("metalakeX", "catalogX", "schemaX");

    when(mockStore.get(badIdent, Entity.EntityType.SCHEMA, SchemaEntity.class))
        .thenThrow(new IOException("Simulated IO failure"));

    Assertions.assertThrows(
        RuntimeException.class,
        () -> cache.getOrLoadMetadataByName(badIdent, Entity.EntityType.SCHEMA));
  }

  @Test
  void testLoadByNameWithNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.getOrLoadMetadataByName(null, Entity.EntityType.SCHEMA));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getOrLoadMetadataByName(ident1, null));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getOrLoadMetadataById(99999999L, null));
  }

  @Test
  void testRemoveByIdThenReload() {
    long id = idMap.get(ident3);
    Entity.EntityType type = Entity.EntityType.TABLE;

    Entity firstLoad = cache.getOrLoadMetadataById(id, type);
    Assertions.assertEquals(table1, firstLoad);
    Assertions.assertTrue(cache.containsById(id, type));
    Assertions.assertTrue(cache.containsByName(ident3));

    cache.removeById(id, type);
    Assertions.assertFalse(cache.containsById(id, type));
    Assertions.assertFalse(cache.containsByName(ident3));

    Entity secondLoad = cache.getOrLoadMetadataById(id, type);
    Assertions.assertEquals(table1, secondLoad);
    Assertions.assertTrue(cache.containsById(id, type));
    Assertions.assertTrue(cache.containsByName(ident3));
  }

  @Test
  void testCreateEntityKey() {
    MetaCacheKey metaCacheKey1 = MetaCacheKey.of(1L, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(1L, metaCacheKey1.id());
    Assertions.assertEquals(Entity.EntityType.SCHEMA, metaCacheKey1.type());

    MetaCacheKey metaCacheKey2 = MetaCacheKey.of(2L, Entity.EntityType.TABLE);
    Assertions.assertEquals(2L, metaCacheKey2.id());
    Assertions.assertEquals(Entity.EntityType.TABLE, metaCacheKey2.type());

    Assertions.assertThrows(IllegalArgumentException.class, () -> MetaCacheKey.of(1L, null));
  }

  @Test
  void testEntityKeyEqualsAndHashCode() {
    MetaCacheKey metaCacheKey1 = MetaCacheKey.of(1L, Entity.EntityType.SCHEMA);
    MetaCacheKey metaCacheKey2 = MetaCacheKey.of(1L, Entity.EntityType.SCHEMA);
    MetaCacheKey metaCacheKey3 = MetaCacheKey.of(2L, Entity.EntityType.SCHEMA);
    MetaCacheKey metaCacheKey4 = MetaCacheKey.of(2L, Entity.EntityType.TABLE);

    Assertions.assertEquals(metaCacheKey1, metaCacheKey2);
    Assertions.assertNotEquals(metaCacheKey1, metaCacheKey3);
    Assertions.assertNotEquals(metaCacheKey1, metaCacheKey4);
    Assertions.assertNotEquals(metaCacheKey2, metaCacheKey3);
    Assertions.assertNotEquals(metaCacheKey2, metaCacheKey4);
    Assertions.assertNotEquals(metaCacheKey3, metaCacheKey4);

    Assertions.assertEquals(metaCacheKey1.hashCode(), metaCacheKey2.hashCode());
    Assertions.assertNotEquals(metaCacheKey1.hashCode(), metaCacheKey3.hashCode());
    Assertions.assertNotEquals(metaCacheKey1.hashCode(), metaCacheKey4.hashCode());
    Assertions.assertNotEquals(metaCacheKey2.hashCode(), metaCacheKey3.hashCode());
    Assertions.assertNotEquals(metaCacheKey2.hashCode(), metaCacheKey4.hashCode());
    Assertions.assertNotEquals(metaCacheKey3.hashCode(), metaCacheKey4.hashCode());
  }

  private void initTestNameIdentifier() {
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    ident4 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table2");
    ident5 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table3");
    ident6 = NameIdentifier.of("metalake1", "catalog1");
    ident7 = NameIdentifier.of("metalake1");
  }

  private void initTestEntities() {
    idMap = new HashMap<>();
    schema1 =
        TestUtil.getTestSchemaEntity(
            1L, "schema1", Namespace.of("metalake1", "catalog1"), "schema1");
    schema2 =
        TestUtil.getTestSchemaEntity(
            2L, "schema2", Namespace.of("metalake2", "catalog2"), "schema2");

    table1 =
        TestUtil.getTestTableEntity(2L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
    table2 =
        TestUtil.getTestTableEntity(3L, "table2", Namespace.of("metalake1", "catalog2", "schema1"));
    table3 =
        TestUtil.getTestTableEntity(4L, "table3", Namespace.of("metalake1", "catalog1", "schema2"));

    catalog =
        TestUtil.getTestCatalogEntity(
            5L, "catalog1", Namespace.of("metalake1"), "hive", "catalog1");
    metalake = TestUtil.getTestMetalake(6L, "metalake1", "test_metalake1");

    idMap.put(ident1, 1L);
    idMap.put(ident2, 2L);
    idMap.put(ident3, 2L);
    idMap.put(ident4, 3L);
    idMap.put(ident5, 4L);
    idMap.put(ident6, 5L);
    idMap.put(ident7, 6L);
  }

  private void initMockStore() throws IOException {
    mockStore = mock(EntityStore.class);

    when(mockStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(schema1);
    when(mockStore.get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(schema2);

    when(mockStore.get(ident3, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table1);
    when(mockStore.get(ident4, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table2);
    when(mockStore.get(ident5, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table3);

    when(mockStore.get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class)).thenReturn(catalog);
    when(mockStore.get(ident7, Entity.EntityType.METALAKE, BaseMetalake.class))
        .thenReturn(metalake);
  }

  private void initIdMock() {
    doReturn(schema1).when(cache).loadEntityFromDBById(1L, Entity.EntityType.SCHEMA);
    doReturn(schema2).when(cache).loadEntityFromDBById(2L, Entity.EntityType.SCHEMA);
    doReturn(table1).when(cache).loadEntityFromDBById(2L, Entity.EntityType.TABLE);
    doReturn(table2).when(cache).loadEntityFromDBById(3L, Entity.EntityType.TABLE);
    doReturn(table3).when(cache).loadEntityFromDBById(4L, Entity.EntityType.TABLE);
    doReturn(catalog).when(cache).loadEntityFromDBById(5L, Entity.EntityType.CATALOG);
    doReturn(metalake).when(cache).loadEntityFromDBById(6L, Entity.EntityType.METALAKE);
  }
}
