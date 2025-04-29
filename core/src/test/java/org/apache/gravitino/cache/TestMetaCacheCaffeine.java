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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMetaCacheCaffeine {

  // Test NameIdentifiers
  private EntityStore mockStore;
  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;

  // Test Entities
  private CatalogEntity catalog;
  private SchemaEntity schema1;
  private SchemaEntity schema2;
  private TableEntity table1;
  private TableEntity table2;
  private TableEntity table3;
  private Map<NameIdentifier, Long> idMap;

  @BeforeAll
  void init() throws IOException {
    initTestNameIdentifier();
    initTestEntities();
    initMockStore();
  }

  @BeforeEach
  void restoreStore() throws IOException {
    initMockStore();
  }

  @Test
  void testInitMetaCache() {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);

    Assertions.assertEquals(0, cache.sizeOfCacheData());
    Assertions.assertEquals(0, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadFromDBByName() throws IOException {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);

    verify(mockStore).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema1, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident1));
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadFromDBById() throws IOException {
    // TODO implement test
  }

  @Test
  void testLoadSchemaFromCacheByName() throws IOException {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);

    verify(mockStore, times(1)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema1, loadEntity);
    Assertions.assertTrue(cache.containsByName(ident1));
    Assertions.assertEquals(1, cache.sizeOfCacheData());
    Assertions.assertEquals(1, cache.sizeOfCacheIndex());
  }

  @Test
  void testLoadCatalogFromCacheByName() throws IOException {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);
    Entity loadEntity = cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    verify(mockStore, times(1)).get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals(catalog, loadEntity);
  }

  @Test
  void testLoadFromCacheById() throws IOException {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);

    Entity loadEntity = cache.getOrLoadMetadataById(1L, Entity.EntityType.SCHEMA);

    verify(mockStore, times(1)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Assertions.assertEquals(schema1, loadEntity);
  }

  @Test
  void testGetMultipleEntitiesByName() throws IOException {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);

    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);

    Assertions.assertEquals(5, cache.sizeOfCacheData());
    Assertions.assertEquals(5, cache.sizeOfCacheIndex());
    verify(mockStore, times(1)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore, times(1)).get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore, times(1)).get(ident3, Entity.EntityType.TABLE, TableEntity.class);
    verify(mockStore, times(1)).get(ident4, Entity.EntityType.TABLE, TableEntity.class);
    verify(mockStore, times(1)).get(ident5, Entity.EntityType.TABLE, TableEntity.class);
  }

  @Test
  void testGetMultipleEntitiesById() {
    // TODO implement test
  }

  @Test
  void testRemoveTableEntityByName() {
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeByName(ident3);
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
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeById(idMap.get(ident3), Entity.EntityType.TABLE);
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
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeByName(ident1);

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
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
    cache.getOrLoadMetadataByName(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoadMetadataByName(ident3, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident4, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident5, Entity.EntityType.TABLE);
    cache.getOrLoadMetadataByName(ident6, Entity.EntityType.CATALOG);

    cache.removeById(idMap.get(ident1), Entity.EntityType.SCHEMA);

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
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
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
    MetaCacheCaffeine cache = new MetaCacheCaffeine(new CacheConfig(), mockStore);
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

  private TableEntity getTestTableEntity(long id, String name, Namespace namespace) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  private AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  private ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }

  private SchemaEntity getTestSchemaEntity(
      long id, String name, Namespace namespace, String comment) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private CatalogEntity getTestCatalogEntity(
      long id, String name, Namespace namespace, String provider, String comment) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withType(Catalog.Type.RELATIONAL)
        .withProvider(provider)
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  private void initTestNameIdentifier() {
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    ident4 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table2");
    ident5 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table3");
    ident6 = NameIdentifier.of("metalake1", "catalog1");
  }

  private void initTestEntities() {
    idMap = new HashMap<>();
    schema1 = getTestSchemaEntity(1L, "schema1", Namespace.of("metalake1", "catalog1"), "schema1");
    schema2 = getTestSchemaEntity(2L, "schema2", Namespace.of("metalake2", "catalog2"), "schema2");

    table1 = getTestTableEntity(2L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
    table2 = getTestTableEntity(3L, "table2", Namespace.of("metalake1", "catalog2", "schema1"));
    table3 = getTestTableEntity(4L, "table3", Namespace.of("metalake1", "catalog1", "schema2"));

    catalog = getTestCatalogEntity(5L, "catalog1", Namespace.of("metalake1"), "hive", "catalog1");

    idMap.put(ident1, 1L);
    idMap.put(ident2, 2L);
    idMap.put(ident3, 2L);
    idMap.put(ident4, 3L);
    idMap.put(ident5, 4L);
    idMap.put(ident6, 5L);
  }

  private void initMockStore() throws IOException {
    mockStore = mock(EntityStore.class);

    when(mockStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(schema1);
    when(mockStore.get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(schema2);

    when(mockStore.get(ident3, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table1);
    when(mockStore.get(ident4, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table2);
    when(mockStore.get(ident5, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(table3);

    when(mockStore.get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class)).thenReturn(catalog);
  }
}
