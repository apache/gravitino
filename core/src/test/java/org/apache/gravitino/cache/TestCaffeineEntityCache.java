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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mockStatic;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
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
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.MockedStatic;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestCaffeineEntityCache {
  // Test Entities.
  private static SchemaEntity entity1;
  private static SchemaEntity entity2;
  private static TableEntity entity3;
  private static TableEntity entity4;
  private static TableEntity entity5;
  private static CatalogEntity entity6;
  private static BaseMetalake entity7;
  private static UserEntity entity8;
  private static UserEntity entity9;
  private static GroupEntity entity10;
  private static GroupEntity entity11;
  private static RoleEntity entity12;
  private static RoleEntity entity13;

  private static Object getCacheDataFrom(EntityCache cache) {
    try {
      Object object = FieldUtils.readDeclaredField(cache, "cacheData", true);
      if (object instanceof Cache) {
        return object;
      } else {
        throw new RuntimeException("Unexpected cache data type: " + object.getClass());
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeAll
  static void initTestEntities() {
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

  @Test
  void testEnableStats() {
    Config config = new Config() {};
    config.set(Configs.CACHE_STATS_ENABLED, true);
    EntityCache cache = new CaffeineEntityCache(config);

    Assertions.assertDoesNotThrow(() -> cache.put(entity1));
  }

  @Test
  void testPutAllTypeInCache() {
    EntityCache cache = getNormalCache();

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

    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity));
    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(testUserEntity));

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
  void testPutSameIdentifierEntities() {
    EntityCache cache = getNormalCache();

    UserEntity testUserEntity = TestUtil.getTestUserEntity();
    TableEntity testTableEntity =
        TestUtil.getTestTableEntity(
            12L, "test_user", Namespace.of("test_metalake", "system", "user"));

    cache.put(testUserEntity);
    cache.put(testTableEntity);

    Assertions.assertEquals(2, cache.size());
    Assertions.assertTrue(
        cache.contains(testTableEntity.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(testUserEntity.nameIdentifier(), Entity.EntityType.USER));
  }

  @Test
  void testPutAndGet() {
    EntityCache cache = getNormalCache();

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

    cache.put(
        entity12.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(entity8, entity9));
    cache.put(
        entity13.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(entity10, entity11));

    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertTrue(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertTrue(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertTrue(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), entity7.type()));

    Assertions.assertTrue(cache.contains(entity8.nameIdentifier(), entity8.type()));
    Assertions.assertTrue(cache.contains(entity9.nameIdentifier(), entity9.type()));
    Assertions.assertTrue(cache.contains(entity10.nameIdentifier(), entity10.type()));
    Assertions.assertTrue(cache.contains(entity11.nameIdentifier(), entity11.type()));

    Assertions.assertTrue(
        cache.contains(
            entity12.nameIdentifier(),
            entity12.type(),
            SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertTrue(
        cache.contains(
            entity13.nameIdentifier(),
            entity13.type(),
            SupportsRelationOperations.Type.ROLE_GROUP_REL));
  }

  @Test
  void testGetIfPresent() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(
        entity12.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(entity8, entity9));

    Assertions.assertTrue(cache.getIfPresent(entity1.nameIdentifier(), entity1.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity3.nameIdentifier(), entity3.type()).isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                entity12.nameIdentifier(),
                entity12.type())
            .isPresent());
    Assertions.assertEquals(cache.size(), 4);

    Assertions.assertFalse(
        cache.getIfPresent(entity4.nameIdentifier(), entity4.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity5.nameIdentifier(), entity5.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity6.nameIdentifier(), entity6.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity7.nameIdentifier(), entity7.type()).isPresent());
  }

  @Test
  void testContains() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertFalse(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertFalse(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertFalse(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertFalse(cache.contains(entity7.nameIdentifier(), entity7.type()));
  }

  @Test
  void testSize() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertEquals(3, cache.size());
  }

  @Test
  void testClear() {
    EntityCache cache = getNormalCache();
    Assertions.assertDoesNotThrow(cache::clear);

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);
    cache.put(
        entity12.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(entity8, entity9));
    cache.put(
        entity13.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(entity10, entity11));

    Assertions.assertEquals(9, cache.size());

    cache.clear();

    Assertions.assertEquals(0, cache.size());
    Assertions.assertFalse(
        cache.getIfPresent(entity1.nameIdentifier(), entity1.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity3.nameIdentifier(), entity3.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity4.nameIdentifier(), entity4.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity5.nameIdentifier(), entity5.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity6.nameIdentifier(), entity6.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity7.nameIdentifier(), entity7.type()).isPresent());
    Assertions.assertFalse(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                entity12.nameIdentifier(),
                entity12.type())
            .isPresent());
    Assertions.assertFalse(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                entity13.nameIdentifier(),
                entity13.type())
            .isPresent());
  }

  @Test
  void testInvalidateMetalake() {
    EntityCache cache = getNormalCache();

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

    cache.put(
        entity12.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(entity8, entity9));
    cache.put(
        entity13.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(entity10, entity11));

    Assertions.assertEquals(13, cache.size());

    cache.invalidate(entity7.nameIdentifier(), entity7.type());

    Assertions.assertEquals(4, cache.size());
    Assertions.assertTrue(cache.contains(entity10.nameIdentifier(), entity10.type()));
    Assertions.assertTrue(cache.contains(entity11.nameIdentifier(), entity11.type()));
    Assertions.assertTrue(
        cache.contains(
            entity13.nameIdentifier(),
            entity13.type(),
            SupportsRelationOperations.Type.ROLE_GROUP_REL));
    Assertions.assertTrue(cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());

    Assertions.assertFalse(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertFalse(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertFalse(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertFalse(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertFalse(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertFalse(cache.contains(entity7.nameIdentifier(), entity7.type()));
  }

  @Test
  void testInvalidateCatalog() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertTrue(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertTrue(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertTrue(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), entity7.type()));

    cache.invalidate(entity6.nameIdentifier(), entity6.type());
    Assertions.assertEquals(3, cache.size());

    Assertions.assertTrue(cache.getIfPresent(entity7.nameIdentifier(), entity7.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity4.nameIdentifier(), entity4.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());

    Assertions.assertFalse(
        cache.getIfPresent(entity1.nameIdentifier(), entity1.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity3.nameIdentifier(), entity3.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity5.nameIdentifier(), entity5.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity6.nameIdentifier(), entity6.type()).isPresent());
  }

  @Test
  void testInvalidateSchema() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertTrue(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertTrue(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertTrue(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), entity7.type()));

    cache.invalidate(entity1.nameIdentifier(), entity1.type());

    Assertions.assertEquals(5, cache.size());

    Assertions.assertTrue(cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity4.nameIdentifier(), entity4.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity5.nameIdentifier(), entity5.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity6.nameIdentifier(), entity6.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity7.nameIdentifier(), entity7.type()).isPresent());

    Assertions.assertFalse(
        cache.getIfPresent(entity1.nameIdentifier(), entity1.type()).isPresent());
    Assertions.assertFalse(
        cache.getIfPresent(entity3.nameIdentifier(), entity3.type()).isPresent());
  }

  @Test
  void testInvalidateTable() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(entity4);
    cache.put(entity5);
    cache.put(entity6);
    cache.put(entity7);

    Assertions.assertEquals(7, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertTrue(cache.contains(entity4.nameIdentifier(), entity4.type()));
    Assertions.assertTrue(cache.contains(entity5.nameIdentifier(), entity5.type()));
    Assertions.assertTrue(cache.contains(entity6.nameIdentifier(), entity6.type()));
    Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), entity7.type()));

    cache.invalidate(entity3.nameIdentifier(), entity3.type());

    Assertions.assertEquals(6, cache.size());
    Assertions.assertTrue(cache.getIfPresent(entity1.nameIdentifier(), entity1.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity2.nameIdentifier(), entity2.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity4.nameIdentifier(), entity4.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity5.nameIdentifier(), entity5.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity6.nameIdentifier(), entity6.type()).isPresent());
    Assertions.assertTrue(cache.getIfPresent(entity7.nameIdentifier(), entity7.type()).isPresent());

    Assertions.assertFalse(cache.contains(entity3.nameIdentifier(), entity3.type()));
  }

  @Test
  void testPutRelationalEntitiesWithMerge() {
    EntityCache cache = getNormalCache();
    RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();
    GroupEntity testGroupEntity1 =
        TestUtil.getTestGroupEntity(
            20L, "group1", "test_metalake", ImmutableList.of(testRoleEntity.name()));
    GroupEntity testGroupEntity2 =
        TestUtil.getTestGroupEntity(
            21L, "group1", "test_metalake", ImmutableList.of(testRoleEntity.name()));

    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity1));
    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity2));

    Assertions.assertTrue(
        cache.contains(
            testRoleEntity.nameIdentifier(),
            testRoleEntity.type(),
            SupportsRelationOperations.Type.ROLE_GROUP_REL));
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .isPresent());
    List<? extends Entity> entities =
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .get();
    Assertions.assertEquals(2, entities.size());
    Assertions.assertEquals(ImmutableList.of(testGroupEntity1, testGroupEntity2), entities);
  }

  @Test
  void testInvalidateOnKeyChange() {
    ModelEntity testModelEntity = TestUtil.getTestModelEntity();
    ModelVersionEntity testModelVersionEntity =
        TestUtil.getTestModelVersionEntity(
            testModelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "s3://test/path"),
            ImmutableMap.of(),
            "test model version",
            ImmutableList.of("alias1", "alias2"));

    EntityCache cache = getNormalCache();
    cache.put(testModelEntity);
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(testModelEntity.nameIdentifier(), testModelEntity.type()));

    cache.put(testModelVersionEntity);
    Assertions.assertEquals(1, cache.size());
    Assertions.assertFalse(
        cache.contains(testModelEntity.nameIdentifier(), testModelEntity.type()));
    Assertions.assertTrue(
        cache.contains(testModelVersionEntity.nameIdentifier(), testModelVersionEntity.type()));
  }

  @Test
  void testPutSameRelationalEntities() {
    EntityCache cache = getNormalCache();
    RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();
    GroupEntity testGroupEntity =
        TestUtil.getTestGroupEntity(
            20L, "group1", "test_metalake", ImmutableList.of(testRoleEntity.name()));

    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity));
    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity));

    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(
        cache.contains(
            testRoleEntity.nameIdentifier(),
            testRoleEntity.type(),
            SupportsRelationOperations.Type.ROLE_GROUP_REL));
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .isPresent());

    List<? extends Entity> entities =
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .get();
    Assertions.assertEquals(1, entities.size());
    Assertions.assertEquals(testGroupEntity, entities.get(0));
  }

  @Test
  void testPutRelationalEntitiesWithEmptyList() {
    EntityCache cache = getNormalCache();
    RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();

    Assertions.assertDoesNotThrow(
        () ->
            cache.put(
                testRoleEntity.nameIdentifier(),
                Entity.EntityType.ROLE,
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                ImmutableList.of()));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testPutRelationalEntitiesWithDifferentOrderButDeduplicated() {
    EntityCache cache = getNormalCache();
    RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();
    GroupEntity testGroupEntity1 =
        TestUtil.getTestGroupEntity(
            20L, "group1", "test_metalake", ImmutableList.of(testRoleEntity.name()));
    GroupEntity testGroupEntity2 =
        TestUtil.getTestGroupEntity(
            21L, "group1", "test_metalake", ImmutableList.of(testRoleEntity.name()));

    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity1, testGroupEntity2));
    cache.put(
        testRoleEntity.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(testGroupEntity2, testGroupEntity1));

    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .isPresent());

    List<? extends Entity> entities =
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                testRoleEntity.nameIdentifier(),
                testRoleEntity.type())
            .get();

    Assertions.assertEquals(
        Sets.newHashSet(testGroupEntity1, testGroupEntity2), Sets.newHashSet(entities));
  }

  @Test
  void testInvalidateRelationKeyAndRelatedEntities() {
    EntityCache cache = getNormalCache();
    RoleEntity role = TestUtil.getTestRoleEntity();
    GroupEntity group = TestUtil.getTestGroupEntity();
    UserEntity user = TestUtil.getTestUserEntity();

    cache.put(
        role.nameIdentifier(),
        role.type(),
        SupportsRelationOperations.Type.ROLE_GROUP_REL,
        ImmutableList.of(group));
    cache.put(
        role.nameIdentifier(),
        role.type(),
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(user));
    cache.put(role);

    cache.invalidate(role.nameIdentifier(), role.type());

    Assertions.assertFalse(
        cache.contains(
            role.nameIdentifier(), role.type(), SupportsRelationOperations.Type.ROLE_GROUP_REL));
    Assertions.assertFalse(
        cache.contains(
            role.nameIdentifier(), role.type(), SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertFalse(cache.contains(role.nameIdentifier(), role.type()));
  }

  @Test
  void testRemoveNonExistentEntity() {
    EntityCache cache = getNormalCache();

    cache.put(entity1);

    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), entity1.type()));

    Assertions.assertDoesNotThrow(() -> cache.invalidate(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertFalse(cache.invalidate(entity2.nameIdentifier(), entity2.type()));

    Assertions.assertDoesNotThrow(() -> cache.invalidate(entity7.nameIdentifier(), entity7.type()));
    Assertions.assertFalse(cache.invalidate(entity7.nameIdentifier(), entity7.type()));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testExpireByTime() {
    long expireTime = 1_000L;

    Config config = new Config() {};
    config.set(Configs.CACHE_EXPIRATION_TIME, expireTime);
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);

    EntityCache cache = new CaffeineEntityCache(config);
    cache.put(entity1);

    await()
        .atMost(3_000, MILLISECONDS)
        .pollInterval(100, MILLISECONDS)
        .until(
            () -> {
              cache.put(entity2);
              Cache<EntityCacheKey, List<Entity>> internalCache =
                  (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);
              internalCache.cleanUp();
              return !cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA);
            });

    Assertions.assertFalse(cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @Test
  void testExpireByWeightExceedMaxWeight() {
    Config config = new Config() {};
    config.set(Configs.CACHE_WEIGHER_ENABLED, true);

    try (MockedStatic<EntityCacheWeigher> mockedStatic = mockStatic(EntityCacheWeigher.class)) {
      mockedStatic.when(EntityCacheWeigher::getMaxWeight).thenReturn(75L);
      mockedStatic.when(EntityCacheWeigher::getInstance).thenReturn(new EntityCacheWeigher());

      EntityCache cache = new CaffeineEntityCache(config);
      cache.put(entity7);

      Cache<EntityCacheKey, List<Entity>> caffeineObject =
          (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);

      await()
          .atMost(1, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                caffeineObject.cleanUp();
                return !cache.contains(entity7.nameIdentifier(), Entity.EntityType.METALAKE);
              });

      Assertions.assertEquals(0, cache.size());
      Assertions.assertFalse(cache.contains(entity7.nameIdentifier(), Entity.EntityType.METALAKE));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testExpireByWeight() throws InterruptedException {
    try (MockedStatic<EntityCacheWeigher> entityCacheWeigherMocked =
        mockStatic(EntityCacheWeigher.class)) {
      entityCacheWeigherMocked.when(EntityCacheWeigher::getMaxWeight).thenReturn(105L);
      entityCacheWeigherMocked
          .when(EntityCacheWeigher::getInstance)
          .thenReturn(new EntityCacheWeigher());
      Config config = new Config() {};
      config.set(Configs.CACHE_WEIGHER_ENABLED, true);

      EntityCache cache = new CaffeineEntityCache(config);
      cache.put(entity7);
      Assertions.assertEquals(1, cache.size());
      Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), entity7.type()));

      cache.put(entity1);
      cache.put(entity2);
      cache.getIfPresent(entity1.nameIdentifier(), entity1.type());
      cache.getIfPresent(entity2.nameIdentifier(), entity2.type());

      Cache<EntityCacheKey, List<Entity>> caffeineObject =
          (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);
      caffeineObject.cleanUp();
      await()
          .atMost(1500, MILLISECONDS)
          .pollInterval(50, MILLISECONDS)
          .until(() -> !cache.contains(entity7.nameIdentifier(), entity7.type()));
    }
  }

  @Test
  void testExpireBySize() {
    Config config = new Config() {};
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    config.set(Configs.CACHE_EXPIRATION_TIME, 0L);
    config.set(Configs.CACHE_MAX_ENTRIES, 1);
    EntityCache cache = new CaffeineEntityCache(config);

    Cache<EntityCacheKey, List<Entity>> caffeineObject =
        (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);

    cache.put(entity1);
    caffeineObject.cleanUp();
    await()
        .atMost(500, MILLISECONDS)
        .until(() -> cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertEquals(1, cache.size());

    cache.put(entity2);
    caffeineObject.cleanUp();
    await()
        .atMost(500, MILLISECONDS)
        .until(() -> cache.contains(entity2.nameIdentifier(), entity2.type()));
    Assertions.assertEquals(1, cache.size());

    cache.put(entity3);
    caffeineObject.cleanUp();
    await()
        .atMost(500, MILLISECONDS)
        .until(() -> cache.contains(entity3.nameIdentifier(), entity3.type()));
    Assertions.assertEquals(1, cache.size());
  }

  @Test
  void testWeightCalculation() {
    int metalakeWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(entity7.nameIdentifier(), entity7.type()),
                ImmutableList.of(entity7));
    Assertions.assertEquals(100, metalakeWeight);

    int catalogWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(entity6.nameIdentifier(), entity6.type()),
                ImmutableList.of(entity6));
    Assertions.assertEquals(75, catalogWeight);

    int schemaWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(entity1.nameIdentifier(), entity1.type()),
                ImmutableList.of(entity1));
    Assertions.assertEquals(50, schemaWeight);

    int tableWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(entity3.nameIdentifier(), entity3.type()),
                ImmutableList.of(entity3));
    Assertions.assertEquals(15, tableWeight);

    int multiUserWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(
                    entity12.nameIdentifier(),
                    entity12.type(),
                    SupportsRelationOperations.Type.ROLE_USER_REL),
                ImmutableList.of(entity8, entity9));

    Assertions.assertEquals(30, multiUserWeight);
  }

  @Test
  void testGetIfPresentWithNull() {
    EntityCache cache = getNormalCache();
    cache.put(entity1);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getIfPresent(null, Entity.EntityType.SCHEMA));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getIfPresent(entity1.nameIdentifier(), null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.getIfPresent(null, entity12.nameIdentifier(), entity12.type()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, null, Entity.EntityType.ROLE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, entity12.nameIdentifier(), null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, entity12.nameIdentifier(), null));
  }

  @Test
  void testContainsWithNull() {
    EntityCache cache = getNormalCache();

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.contains(null, Entity.EntityType.SCHEMA));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.contains(entity7.nameIdentifier(), null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.contains(
                null, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.contains(
                entity12.nameIdentifier(), null, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.contains(entity12.nameIdentifier(), entity12.type(), null));
  }

  @Test
  void testInvalidateWithNull() {
    EntityCache cache = getNormalCache();

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.invalidate(null, Entity.EntityType.CATALOG));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.invalidate(entity7.nameIdentifier(), null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.invalidate(
                null, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.invalidate(
                entity12.nameIdentifier(), null, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.invalidate(entity12.nameIdentifier(), entity12.type(), null));
  }

  @Test
  void testPutWithNull() {
    EntityCache cache = getNormalCache();

    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.put(null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.put(
                null,
                Entity.EntityType.ROLE,
                SupportsRelationOperations.Type.ROLE_USER_REL,
                ImmutableList.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.put(
                entity12.nameIdentifier(),
                null,
                SupportsRelationOperations.Type.ROLE_USER_REL,
                ImmutableList.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.put(entity12.nameIdentifier(), entity12.type(), null, ImmutableList.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.put(
                entity12.nameIdentifier(),
                Entity.EntityType.ROLE,
                SupportsRelationOperations.Type.ROLE_USER_REL,
                null));
  }

  private EntityCache getNormalCache() {
    Config config = new Config() {};
    config.set(Configs.CACHE_EXPIRATION_TIME, 0L);
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    config.set(Configs.CACHE_MAX_ENTRIES, 1000000);

    return new CaffeineEntityCache(config);
  }
}
