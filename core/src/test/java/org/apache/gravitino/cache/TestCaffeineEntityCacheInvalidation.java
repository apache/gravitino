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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests hierarchical invalidation and cacheability rules of {@link CaffeineEntityCache}. */
public class TestCaffeineEntityCacheInvalidation {

  private CaffeineEntityCache cache;

  @BeforeEach
  void setUp() {
    cache = new CaffeineEntityCache(new Config() {});
  }

  @Test
  void testInvalidateCatalogCascadesToChildren() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    SchemaEntity schema =
        TestUtil.getTestSchemaEntity(2L, "schema1", Namespace.of("metalake", "catalog1"), "cmt");
    TableEntity table =
        TestUtil.getTestTableEntity(3L, "table1", Namespace.of("metalake", "catalog1", "schema1"));

    cache.put(catalog);
    cache.put(schema);
    cache.put(table);
    Assertions.assertEquals(3, cache.size());

    cache.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG);

    Assertions.assertFalse(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testInvalidateSchemaKeepsParentCatalog() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    SchemaEntity schema =
        TestUtil.getTestSchemaEntity(2L, "schema1", Namespace.of("metalake", "catalog1"), "cmt");
    TableEntity table =
        TestUtil.getTestTableEntity(3L, "table1", Namespace.of("metalake", "catalog1", "schema1"));

    cache.put(catalog);
    cache.put(schema);
    cache.put(table);

    cache.invalidate(schema.nameIdentifier(), Entity.EntityType.SCHEMA);

    Assertions.assertTrue(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertFalse(cache.contains(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  void testInvalidateDoesNotEvictSiblingsWithSharedNamePrefix() {
    CatalogEntity catalog1 =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    CatalogEntity catalog10 =
        TestUtil.getTestCatalogEntity(2L, "catalog10", Namespace.of("metalake"), "hive", "cmt");

    cache.put(catalog1);
    cache.put(catalog10);

    cache.invalidate(catalog1.nameIdentifier(), Entity.EntityType.CATALOG);

    Assertions.assertFalse(cache.contains(catalog1.nameIdentifier(), Entity.EntityType.CATALOG));
    Assertions.assertTrue(cache.contains(catalog10.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  void testRoleUserGroupAreNotCached() {
    RoleEntity role = TestUtil.getTestRoleEntity();
    UserEntity user = TestUtil.getTestUserEntity();
    GroupEntity group = TestUtil.getTestGroupEntity();

    cache.put(role);
    cache.put(user);
    cache.put(group);

    Assertions.assertEquals(0, cache.size());
    Assertions.assertFalse(cache.contains(role.nameIdentifier(), Entity.EntityType.ROLE));
    Assertions.assertFalse(cache.contains(user.nameIdentifier(), Entity.EntityType.USER));
    Assertions.assertFalse(cache.contains(group.nameIdentifier(), Entity.EntityType.GROUP));
    Assertions.assertTrue(
        cache.getIfPresent(role.nameIdentifier(), Entity.EntityType.ROLE).isEmpty());
  }

  @Test
  void testPutModelVersionInvalidatesModel() {
    ModelEntity model = TestUtil.getTestModelEntity(1L, "model1", Namespace.of("m1", "c1", "s1"));
    cache.put(model);
    Assertions.assertTrue(cache.contains(model.nameIdentifier(), Entity.EntityType.MODEL));

    ModelVersionEntity version =
        TestUtil.getTestModelVersionEntity(
            model.nameIdentifier(),
            1,
            ImmutableMap.of("unknown", "uri"),
            ImmutableMap.of(),
            "cmt",
            ImmutableList.of());
    cache.put(version);

    Assertions.assertFalse(cache.contains(model.nameIdentifier(), Entity.EntityType.MODEL));
  }

  @Test
  void testGetIfPresentReturnsCachedEntity() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    cache.put(catalog);

    Assertions.assertEquals(
        catalog,
        cache.getIfPresent(catalog.nameIdentifier(), Entity.EntityType.CATALOG).orElse(null));
    Assertions.assertTrue(
        cache.getIfPresent(catalog.nameIdentifier(), Entity.EntityType.SCHEMA).isEmpty());
  }

  @Test
  void testClearResetsSizeAndIndex() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    cache.put(catalog);
    Assertions.assertEquals(1, cache.size());

    cache.clear();

    Assertions.assertEquals(0, cache.size());
    Assertions.assertFalse(cache.contains(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }
}
