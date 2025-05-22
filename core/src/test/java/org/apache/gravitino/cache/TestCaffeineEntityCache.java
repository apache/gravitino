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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCaffeineEntityCache {
  private RelationalEntityStore mockStore;
  private CaffeineEntityCache real;
  private CaffeineEntityCache cache;

  private NameIdentifier ident1;
  private NameIdentifier ident2;
  private NameIdentifier ident3;
  private NameIdentifier ident4;
  private NameIdentifier ident5;
  private NameIdentifier ident6;
  private NameIdentifier ident7;
  private NameIdentifier ident8;
  private NameIdentifier ident9;
  private NameIdentifier ident10;
  private NameIdentifier ident11;
  private NameIdentifier ident12;
  private NameIdentifier ident13;

  // Test Entities
  private SchemaEntity entity1;
  private SchemaEntity entity2;
  private TableEntity entity3;
  private TableEntity entity4;
  private TableEntity entity5;
  private CatalogEntity entity6;
  private BaseMetalake entity7;
  private UserEntity entity8;
  private UserEntity entity9;
  private GroupEntity entity10;
  private GroupEntity entity11;
  private RoleEntity entity12;
  private RoleEntity entity13;

  @BeforeAll
  void init() throws IOException {
    initTestNameIdentifier();
    initTestEntities();
    initMockStore();
  }

  @BeforeEach
  void restoreStore() {
    clearInvocations(mockStore);
    CaffeineEntityCache.resetForTest();
    real = CaffeineEntityCache.getInstance(new CacheConfig(), mockStore);
    cache = spy(real);
  }

  @Test
  void testEquals() {
    Assertions.assertEquals(entity1.nameIdentifier(), ident1);
    Assertions.assertEquals(entity2.nameIdentifier(), ident2);
    Assertions.assertEquals(entity3.nameIdentifier(), ident3);
    Assertions.assertEquals(entity4.nameIdentifier(), ident4);
    Assertions.assertEquals(entity5.nameIdentifier(), ident5);
    Assertions.assertEquals(entity6.nameIdentifier(), ident6);
    Assertions.assertEquals(entity7.nameIdentifier(), ident7);
  }

  @Test
  void testPutAllTypeInCache() {
    BaseMetalake testMetalake = TestUtil.getTestMetalake();
    CatalogEntity testCatalogEntity = TestUtil.getTestCatalogEntity();
    SchemaEntity testSchemaEntity = TestUtil.getTestSchemaEntity();
    TableEntity testTableEntity = TestUtil.getTestTableEntity();
    ModelEntity testModelEntity = TestUtil.getTestModelEntity();
    FilesetEntity testFileSetEntity = TestUtil.getTestFileSetEntity();
    TopicEntity testTopicEntity = TestUtil.getTestTopicEntity();
    TagEntity testTagEntity = TestUtil.getTestTagEntity();
    UserEntity testUserEntity = TestUtil.getTestUserEntity();
    GroupEntity testGroupEntity = TestUtil.getTestGroupEntity();
    RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();
    ModelVersionEntity testModelVersionEntity = TestUtil.getTestModelVersionEntity();

    cache.put(testMetalake);
    cache.put(testCatalogEntity);
    cache.put(testSchemaEntity);
    cache.put(testTableEntity);
    cache.put(testModelEntity);
    cache.put(testFileSetEntity);
    cache.put(testTopicEntity);
    cache.put(testTagEntity);
    cache.put(testUserEntity);
    cache.put(testGroupEntity);
    cache.put(testRoleEntity);
    cache.put(testModelVersionEntity);

    cache.put(testRoleEntity, testGroupEntity, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    cache.put(testRoleEntity, testUserEntity, SupportsRelationOperations.Type.ROLE_USER_REL);

    Assertions.assertEquals(14, cache.size());
    Assertions.assertTrue(
        cache.getIfPresent(testMetalake.nameIdentifier(), Entity.EntityType.METALAKE).isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(testCatalogEntity.nameIdentifier(), Entity.EntityType.CATALOG)
            .isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(testSchemaEntity.nameIdentifier(), Entity.EntityType.SCHEMA)
            .isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testTableEntity.nameIdentifier(), Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testModelEntity.nameIdentifier(), Entity.EntityType.MODEL).isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(testFileSetEntity.nameIdentifier(), Entity.EntityType.FILESET)
            .isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testTopicEntity.nameIdentifier(), Entity.EntityType.TOPIC).isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testTagEntity.nameIdentifier(), Entity.EntityType.TAG).isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testUserEntity.nameIdentifier(), Entity.EntityType.USER).isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testGroupEntity.nameIdentifier(), Entity.EntityType.GROUP).isPresent());
    Assertions.assertTrue(
        cache.getIfPresent(testRoleEntity.nameIdentifier(), Entity.EntityType.ROLE).isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(testModelVersionEntity.nameIdentifier(), Entity.EntityType.MODEL_VERSION)
            .isPresent());

    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                Entity.EntityType.ROLE)
            .isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                testRoleEntity.nameIdentifier(),
                Entity.EntityType.ROLE)
            .isPresent());
  }

  @Test
  void testPutAndGet() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);
    cache.put(entity8);
    cache.put(entity9);
    cache.put(entity10);
    cache.put(entity11);

    cache.put(entity12, entity8, SupportsRelationOperations.Type.ROLE_USER_REL);
    cache.put(entity12, entity9, SupportsRelationOperations.Type.ROLE_USER_REL);

    cache.put(entity13, entity10, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    cache.put(entity13, entity11, SupportsRelationOperations.Type.ROLE_GROUP_REL);

    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident6, Entity.EntityType.CATALOG));
    Assertions.assertTrue(cache.contains(ident7, Entity.EntityType.METALAKE));

    Assertions.assertTrue(cache.contains(ident8, Entity.EntityType.USER));
    Assertions.assertTrue(cache.contains(ident9, Entity.EntityType.USER));
    Assertions.assertTrue(cache.contains(ident10, Entity.EntityType.GROUP));
    Assertions.assertTrue(cache.contains(ident11, Entity.EntityType.GROUP));

    Assertions.assertTrue(
        cache.contains(
            ident12, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertTrue(
        cache.contains(
            ident13, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL));
  }

  @Test
  void testGetIfPresent() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertTrue(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
    Assertions.assertEquals(cache.size(), 3);

    Assertions.assertFalse(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());
  }

  @Test
  void testGetFromCache() throws IOException {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Entity entityFromCache1 = cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
    Entity entityFromCache2 = cache.getOrLoad(ident2, Entity.EntityType.SCHEMA);
    Entity entityFromCache3 = cache.getOrLoad(ident3, Entity.EntityType.TABLE);

    Assertions.assertEquals(entity1, entityFromCache1);
    Assertions.assertEquals(entity2, entityFromCache2);
    Assertions.assertEquals(entity3, entityFromCache3);

    verify(mockStore, never()).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore, never()).get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore, never()).get(ident3, Entity.EntityType.TABLE, TableEntity.class);
  }

  @Test
  void testGetFromStore() throws IOException {
    cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoad(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoad(ident3, Entity.EntityType.TABLE);

    cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
    cache.getOrLoad(ident2, Entity.EntityType.SCHEMA);
    cache.getOrLoad(ident3, Entity.EntityType.TABLE);

    verify(mockStore).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore).get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class);
    verify(mockStore).get(ident3, Entity.EntityType.TABLE, TableEntity.class);
  }

  @Test
  void testContains() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident6, Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(ident7, Entity.EntityType.METALAKE));
  }

  @Test
  void testSize() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertEquals(3, cache.size());
  }

  @Test
  void testClear() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());

    cache.clear();

    Assertions.assertEquals(0, cache.size());
    Assertions.assertFalse(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());
  }

  @Test
  void testInvalidateMetalake() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);
    cache.put(entity8);
    cache.put(entity9);
    cache.put(entity10);
    cache.put(entity11);

    cache.put(entity12, entity8, SupportsRelationOperations.Type.ROLE_USER_REL);
    cache.put(entity12, entity9, SupportsRelationOperations.Type.ROLE_USER_REL);

    cache.put(entity13, entity10, SupportsRelationOperations.Type.ROLE_GROUP_REL);
    cache.put(entity13, entity11, SupportsRelationOperations.Type.ROLE_GROUP_REL);

    Assertions.assertEquals(13, cache.size());

    cache.invalidate(ident7, Entity.EntityType.METALAKE);

    Assertions.assertEquals(4, cache.size());
    Assertions.assertTrue(cache.contains(ident10, Entity.EntityType.GROUP));
    Assertions.assertTrue(cache.contains(ident11, Entity.EntityType.GROUP));
    Assertions.assertTrue(
        cache.contains(
            ident13, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_GROUP_REL));
    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());

    Assertions.assertFalse(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident6, Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(ident7, Entity.EntityType.TABLE));
  }

  @Test
  void testInvalidateCatalog() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident6, Entity.EntityType.CATALOG));
    Assertions.assertTrue(cache.contains(ident7, Entity.EntityType.METALAKE));

    cache.invalidate(ident6, Entity.EntityType.CATALOG);
    Assertions.assertEquals(3, cache.size());

    Assertions.assertTrue(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());

    Assertions.assertFalse(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident6, Entity.EntityType.TABLE).isPresent());
  }

  @Test
  void testInvalidateSchema() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident6, Entity.EntityType.CATALOG));
    Assertions.assertTrue(cache.contains(ident7, Entity.EntityType.METALAKE));

    cache.invalidate(ident1, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(5, cache.size());

    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());

    Assertions.assertFalse(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
  }

  @Test
  void testInvalidateTable() {
    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(ident3, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident4, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident5, Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(ident6, Entity.EntityType.CATALOG));
    Assertions.assertTrue(cache.contains(ident7, Entity.EntityType.METALAKE));

    cache.invalidate(ident3, Entity.EntityType.TABLE);

    Assertions.assertEquals(6, cache.size());
    Assertions.assertTrue(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());

    Assertions.assertFalse(cache.contains(ident3, Entity.EntityType.TABLE));
  }

  @Test
  void testRemoveNonExistentEntity() {
    cache.put(entity1);

    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));

    Assertions.assertDoesNotThrow(() -> cache.invalidate(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertDoesNotThrow(() -> cache.invalidate(ident7, Entity.EntityType.METALAKE));
  }

  @Test
  void testLoadEntityThrowException() throws IOException {
    NameIdentifier badIdent = NameIdentifier.of("metalakeX", "catalogX", "schemaX");

    when(mockStore.get(badIdent, Entity.EntityType.SCHEMA, SchemaEntity.class))
        .thenThrow(new IOException("Simulated IO failure"));

    Assertions.assertThrows(
        IOException.class, () -> cache.getOrLoad(badIdent, Entity.EntityType.SCHEMA));
  }

  @Test
  void testLoadByNameWithNull() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getOrLoad(null, Entity.EntityType.SCHEMA));
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.getOrLoad(ident1, null));
  }

  @Test
  void testNullEntityStore() {
    CaffeineEntityCache.resetForTest();
    cache = CaffeineEntityCache.getInstance(new CacheConfig(), null);
    Assertions.assertDoesNotThrow(() -> cache.put(entity1));
    Assertions.assertDoesNotThrow(() -> cache.getIfPresent(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertDoesNotThrow(() -> cache.invalidate(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertDoesNotThrow(() -> cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertDoesNotThrow(() -> cache.size());
    Assertions.assertThrows(
        RuntimeException.class, () -> cache.getOrLoad(ident1, Entity.EntityType.SCHEMA));
  }

  @Test
  void testRemoveThenReload() throws IOException {
    Entity firstLoadEntity = cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
    Assertions.assertEquals(firstLoadEntity, entity1);
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertEquals(1, cache.size());

    cache.invalidate(ident1, Entity.EntityType.SCHEMA);
    Entity secondLoadEntity = cache.getOrLoad(ident1, Entity.EntityType.SCHEMA);
    Assertions.assertEquals(firstLoadEntity, secondLoadEntity);
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));
    Assertions.assertEquals(1, cache.size());

    verify(mockStore, times(2)).get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class);
  }

  private void initMockStore() throws IOException {
    mockStore = mock(RelationalEntityStore.class);

    when(mockStore.get(ident1, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity1);
    when(mockStore.get(ident2, Entity.EntityType.SCHEMA, SchemaEntity.class)).thenReturn(entity2);

    when(mockStore.get(ident3, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity3);
    when(mockStore.get(ident4, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity4);
    when(mockStore.get(ident5, Entity.EntityType.TABLE, TableEntity.class)).thenReturn(entity5);

    when(mockStore.get(ident6, Entity.EntityType.CATALOG, CatalogEntity.class)).thenReturn(entity6);
    when(mockStore.get(ident7, Entity.EntityType.METALAKE, BaseMetalake.class)).thenReturn(entity7);

    when(mockStore.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_USER_REL, ident12, Entity.EntityType.ROLE))
        .thenReturn(ImmutableList.of(entity8, entity9));
    when(mockStore.listEntitiesByRelation(
            SupportsRelationOperations.Type.ROLE_GROUP_REL, ident13, Entity.EntityType.ROLE))
        .thenReturn(ImmutableList.of(entity10, entity11));
  }

  private void initTestNameIdentifier() {
    ident1 = NameIdentifier.of("metalake1", "catalog1", "schema1");
    ident2 = NameIdentifier.of("metalake2", "catalog2", "schema2");
    ident3 = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    ident4 = NameIdentifier.of("metalake1", "catalog2", "schema1", "table2");
    ident5 = NameIdentifier.of("metalake1", "catalog1", "schema2", "table3");
    ident6 = NameIdentifier.of("metalake1", "catalog1");
    ident7 = NameIdentifier.of("metalake1");

    ident8 = NameIdentifierUtil.ofUser("metalake1", "user1");
    ident9 = NameIdentifierUtil.ofUser("metalake1", "user2");

    ident10 = NameIdentifierUtil.ofGroup("metalake2", "group1");
    ident11 = NameIdentifierUtil.ofGroup("metalake2", "group2");

    ident12 = NameIdentifierUtil.ofRole("metalake1", "role1");
    ident13 = NameIdentifierUtil.ofRole("metalake2", "role2");
  }

  private void initTestEntities() {
    entity1 =
        TestUtil.getTestSchemaEntity(
            1L, "schema1", Namespace.of("metalake1", "catalog1"), "test_schema1");
    entity2 =
        TestUtil.getTestSchemaEntity(
            2L, "schema2", Namespace.of("metalake2", "catalog2"), "test_schema2");
    entity3 =
        TestUtil.getTestTableEntity(3L, "table1", Namespace.of("metalake1", "catalog1", "schema1"));
    entity4 =
        TestUtil.getTestTableEntity(4L, "table2", Namespace.of("metalake1", "catalog2", "schema1"));
    entity5 =
        TestUtil.getTestTableEntity(5L, "table3", Namespace.of("metalake1", "catalog1", "schema2"));
    entity6 =
        TestUtil.getTestCatalogEntity(
            6L, "catalog1", Namespace.of("metalake1"), "hive", "test_catalog");
    entity7 = TestUtil.getTestMetalake(7L, "metalake1", "test_metalake1");

    entity8 = TestUtil.getTestUserEntity(8L, "user1", "metalake1", ImmutableList.of(12L));
    entity9 = TestUtil.getTestUserEntity(9L, "user2", "metalake1", ImmutableList.of(12L));

    entity10 = TestUtil.getTestGroupEntity(10L, "group1", "metalake2", ImmutableList.of("role2"));
    entity11 = TestUtil.getTestGroupEntity(11L, "group2", "metalake2", ImmutableList.of("role2"));

    entity12 = TestUtil.getTestRoleEntity(12L, "role1", "metalake1");
    entity13 = TestUtil.getTestRoleEntity(13L, "role2", "metalake2");
  }
}
