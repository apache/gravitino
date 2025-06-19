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

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
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
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCaffeineEntityCache {
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
  void init() {
    initTestNameIdentifier();
    initTestEntities();
  }

  @Test
  void testPutAllTypeInCache() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);
    cache.put(
        entity12.nameIdentifier(),
        Entity.EntityType.ROLE,
        SupportsRelationOperations.Type.ROLE_USER_REL,
        ImmutableList.of(entity8, entity9));

    Assertions.assertTrue(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertTrue(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
    Assertions.assertTrue(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, ident12, Entity.EntityType.ROLE)
            .isPresent());
    Assertions.assertEquals(cache.size(), 4);

    Assertions.assertFalse(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());
  }

  @Test
  void testContains() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    cache.put(entity1);
    cache.put(entity2);
    cache.put(entity3);

    Assertions.assertEquals(3, cache.size());
  }

  @Test
  void testClear() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    Assertions.assertFalse(cache.getIfPresent(ident1, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident2, Entity.EntityType.SCHEMA).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident3, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident4, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident5, Entity.EntityType.TABLE).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident6, Entity.EntityType.CATALOG).isPresent());
    Assertions.assertFalse(cache.getIfPresent(ident7, Entity.EntityType.METALAKE).isPresent());
    Assertions.assertFalse(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, ident12, Entity.EntityType.ROLE)
            .isPresent());
    Assertions.assertFalse(
        cache
            .getIfPresent(
                SupportsRelationOperations.Type.ROLE_GROUP_REL, ident13, Entity.EntityType.ROLE)
            .isPresent());
  }

  @Test
  void testInvalidateMetalake() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    cache.put(entity1);

    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(ident1, Entity.EntityType.SCHEMA));

    Assertions.assertDoesNotThrow(() -> cache.invalidate(ident2, Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.invalidate(ident2, Entity.EntityType.SCHEMA));

    Assertions.assertDoesNotThrow(() -> cache.invalidate(ident7, Entity.EntityType.METALAKE));
    Assertions.assertFalse(cache.invalidate(ident7, Entity.EntityType.METALAKE));
  }

  @Test
  void testExpireByTime() throws InterruptedException {
    int waitTime = 3_000;
    long expireTime = 1_000L;

    Config config = new Config() {};
    config.set(Configs.CACHE_EXPIRATION_TIME, expireTime);
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    EntityCache cache = new CaffeineEntityCache(config);

    cache.put(entity1);
    Thread.sleep(waitTime);
    cache.put(entity2);

    Cache<EntityCacheKey, List<Entity>> caffeineObject =
        (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);
    caffeineObject.cleanUp();

    Assertions.assertEquals(1, cache.size());
    Assertions.assertFalse(cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @Test
  void testExpireByWeightExceedMaxWeight() {
    Config config = new Config() {};
    long oldMaxWeight = EntityCacheWeigher.MAX_WEIGHT;
    EntityCacheWeigher.MAX_WEIGHT = 75;
    config.set(Configs.CACHE_WEIGHER_ENABLED, true);
    EntityCache cache = new CaffeineEntityCache(config);

    cache.put(entity7);
    Cache<EntityCacheKey, List<Entity>> caffeineObject =
        (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);
    caffeineObject.cleanUp();

    Assertions.assertEquals(0, cache.size());
    Assertions.assertFalse(cache.contains(entity7.nameIdentifier(), Entity.EntityType.METALAKE));

    EntityCacheWeigher.MAX_WEIGHT = oldMaxWeight;
  }

  @Test
  void testExpireByWeight() {
    Config config = new Config() {};
    long maxWeight = EntityCacheWeigher.MAX_WEIGHT;
    EntityCacheWeigher.MAX_WEIGHT = 100;
    config.set(Configs.CACHE_WEIGHER_ENABLED, true);
    EntityCache cache = new CaffeineEntityCache(config);

    cache.put(entity7);
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(entity7.nameIdentifier(), Entity.EntityType.METALAKE));

    cache.put(entity1);
    cache.put(entity2);
    Cache<EntityCacheKey, List<Entity>> caffeineObject =
        (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);
    caffeineObject.cleanUp();

    Assertions.assertEquals(2, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), Entity.EntityType.SCHEMA));

    EntityCacheWeigher.MAX_WEIGHT = maxWeight;
  }

  @Test
  void testExpireBySize() {
    Config config = new Config() {};
    config.set(Configs.CACHE_WEIGHER_ENABLED, false);
    config.set(Configs.CACHE_MAX_ENTRIES, 1);
    EntityCache cache = new CaffeineEntityCache(config);

    Cache<EntityCacheKey, List<Entity>> caffeineObject =
        (Cache<EntityCacheKey, List<Entity>>) getCacheDataFrom(cache);

    cache.put(entity1);
    caffeineObject.cleanUp();
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(entity1.nameIdentifier(), Entity.EntityType.SCHEMA));

    cache.put(entity2);
    caffeineObject.cleanUp();
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(entity2.nameIdentifier(), Entity.EntityType.SCHEMA));

    cache.put(entity3);
    caffeineObject.cleanUp();
    Assertions.assertEquals(1, cache.size());
    Assertions.assertTrue(cache.contains(entity3.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testWeightCalculation() {
    int metalakeWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(ident7, Entity.EntityType.METALAKE), ImmutableList.of(entity7));
    Assertions.assertEquals(100, metalakeWeight);

    int catalogWeight =
        EntityCacheWeigher.getInstance()
            .weigh(EntityCacheKey.of(ident6, Entity.EntityType.CATALOG), ImmutableList.of(entity6));
    Assertions.assertEquals(75, catalogWeight);

    int schemaWeight =
        EntityCacheWeigher.getInstance()
            .weigh(EntityCacheKey.of(ident1, Entity.EntityType.SCHEMA), ImmutableList.of(entity1));
    Assertions.assertEquals(50, schemaWeight);

    int tableWeight =
        EntityCacheWeigher.getInstance()
            .weigh(EntityCacheKey.of(ident3, Entity.EntityType.TABLE), ImmutableList.of(entity3));
    Assertions.assertEquals(15, tableWeight);

    int multiUserWeight =
        EntityCacheWeigher.getInstance()
            .weigh(
                EntityCacheKey.of(
                    ident12, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL),
                ImmutableList.of(entity8, entity9));

    Assertions.assertEquals(30, multiUserWeight);
  }

  @Test
  void testGetIfPresentWithNull() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});
    cache.put(entity1);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.getIfPresent(null, Entity.EntityType.SCHEMA));
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.getIfPresent(ident1, null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.getIfPresent(null, ident12, Entity.EntityType.ROLE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.getIfPresent(
                SupportsRelationOperations.Type.ROLE_USER_REL, null, Entity.EntityType.ROLE));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.getIfPresent(SupportsRelationOperations.Type.ROLE_USER_REL, ident12, null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.getIfPresent(SupportsRelationOperations.Type.ROLE_USER_REL, ident12, null));
  }

  @Test
  void testContainsWithNull() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.contains(null, Entity.EntityType.SCHEMA));
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.contains(ident7, null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.contains(
                null, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.contains(ident12, null, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.contains(ident12, Entity.EntityType.ROLE, null));
  }

  @Test
  void testInvalidateWithNull() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> cache.invalidate(null, Entity.EntityType.CATALOG));
    Assertions.assertThrows(IllegalArgumentException.class, () -> cache.invalidate(ident7, null));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.invalidate(
                null, Entity.EntityType.ROLE, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.invalidate(ident12, null, SupportsRelationOperations.Type.ROLE_USER_REL));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.invalidate(ident12, Entity.EntityType.ROLE, null));
  }

  @Test
  void testPutWithNull() {
    EntityCache cache = new CaffeineEntityCache(new Config() {});

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
                ident12, null, SupportsRelationOperations.Type.ROLE_USER_REL, ImmutableList.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> cache.put(ident12, Entity.EntityType.ROLE, null, ImmutableList.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            cache.put(
                ident12,
                Entity.EntityType.ROLE,
                SupportsRelationOperations.Type.ROLE_USER_REL,
                null));
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
