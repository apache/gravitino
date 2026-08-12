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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests hierarchical invalidation and cacheability rules of {@link CaffeineEntityCache}. */
public class TestCaffeineEntityCacheInvalidation {

  private static final Namespace CATALOG_NS = Namespace.of("metalake", "catalog1");

  private CaffeineEntityCache cache;

  @BeforeEach
  void setUp() {
    cache = new CaffeineEntityCache(new Config() {});
  }

  /**
   * Joins nested schema levels the way they reach the cache. The cache sits above the storage
   * layer, so nested schema names still carry the configured external separator.
   */
  private static String hierarchicalName(String... levels) {
    return String.join(HierarchicalSchemaUtil.schemaSeparator(), levels);
  }

  private static Namespace schemaNamespace(String schemaName) {
    return Namespace.of(CATALOG_NS.level(0), CATALOG_NS.level(1), schemaName);
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
  void testInvalidateHierarchicalSchemaCascadesToNestedSchemas() {
    // A HierarchicalSchema nests inside a single name level, joined by the schema separator, so
    // "raw:events:2024" is a child of "raw:events" without adding a NameIdentifier level.
    String parentName = hierarchicalName("raw", "events");
    String childName = hierarchicalName("raw", "events", "2024");

    SchemaEntity parent = TestUtil.getTestSchemaEntity(2L, parentName, CATALOG_NS, "cmt");
    SchemaEntity child = TestUtil.getTestSchemaEntity(3L, childName, CATALOG_NS, "cmt");
    TableEntity tableInParent =
        TestUtil.getTestTableEntity(4L, "t_parent", schemaNamespace(parentName));
    TableEntity tableInChild =
        TestUtil.getTestTableEntity(5L, "t_child", schemaNamespace(childName));

    cache.put(parent);
    cache.put(child);
    cache.put(tableInParent);
    cache.put(tableInChild);
    Assertions.assertEquals(4, cache.size());

    cache.invalidate(parent.nameIdentifier(), Entity.EntityType.SCHEMA);

    Assertions.assertFalse(cache.contains(parent.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(tableInParent.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(cache.contains(child.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(tableInChild.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testInvalidateHierarchicalSchemaCascadesToAnyDepth() {
    String level1 = hierarchicalName("raw");
    String level2 = hierarchicalName("raw", "events");
    String level3 = hierarchicalName("raw", "events", "2024");
    String level4 = hierarchicalName("raw", "events", "2024", "q1");

    cache.put(TestUtil.getTestSchemaEntity(2L, level1, CATALOG_NS, "cmt"));
    cache.put(TestUtil.getTestSchemaEntity(3L, level2, CATALOG_NS, "cmt"));
    cache.put(TestUtil.getTestSchemaEntity(4L, level3, CATALOG_NS, "cmt"));
    SchemaEntity deepest = TestUtil.getTestSchemaEntity(5L, level4, CATALOG_NS, "cmt");
    cache.put(deepest);
    TableEntity deepestTable = TestUtil.getTestTableEntity(6L, "t_deep", schemaNamespace(level4));
    cache.put(deepestTable);
    Assertions.assertEquals(5, cache.size());

    cache.invalidate(
        TestUtil.getTestSchemaEntity(2L, level1, CATALOG_NS, "cmt").nameIdentifier(),
        Entity.EntityType.SCHEMA);

    Assertions.assertFalse(cache.contains(deepest.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(deepestTable.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testInvalidateHierarchicalSchemaDoesNotTouchSiblings() {
    // Guards against over-matching: "raw:events2" is a sibling of "raw:events", not a descendant,
    // exactly like the catalog1 / catalog10 case the "." boundary already protects against.
    String target = hierarchicalName("raw", "events");
    String sibling = hierarchicalName("raw", "events2");
    String siblingOfParent = hierarchicalName("raw2", "events");

    SchemaEntity targetSchema = TestUtil.getTestSchemaEntity(2L, target, CATALOG_NS, "cmt");
    SchemaEntity siblingSchema = TestUtil.getTestSchemaEntity(3L, sibling, CATALOG_NS, "cmt");
    SchemaEntity otherBranch = TestUtil.getTestSchemaEntity(4L, siblingOfParent, CATALOG_NS, "cmt");
    TableEntity siblingTable =
        TestUtil.getTestTableEntity(5L, "t_sibling", schemaNamespace(sibling));

    cache.put(targetSchema);
    cache.put(siblingSchema);
    cache.put(otherBranch);
    cache.put(siblingTable);

    cache.invalidate(targetSchema.nameIdentifier(), Entity.EntityType.SCHEMA);

    Assertions.assertFalse(cache.contains(targetSchema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(siblingSchema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(otherBranch.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertTrue(cache.contains(siblingTable.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(3, cache.size());
  }

  @Test
  void testInvalidateCatalogCascadesToHierarchicalSchemas() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(1L, "catalog1", Namespace.of("metalake"), "hive", "cmt");
    String nested = hierarchicalName("raw", "events", "2024");
    SchemaEntity schema = TestUtil.getTestSchemaEntity(2L, nested, CATALOG_NS, "cmt");
    TableEntity table = TestUtil.getTestTableEntity(3L, "t1", schemaNamespace(nested));

    cache.put(catalog);
    cache.put(schema);
    cache.put(table);
    Assertions.assertEquals(3, cache.size());

    cache.invalidate(catalog.nameIdentifier(), Entity.EntityType.CATALOG);

    Assertions.assertFalse(cache.contains(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void testInvalidateHierarchicalSchemaCascadesWithNonDefaultSeparator() throws Exception {
    Config separatorConfig = new Config(false) {};
    separatorConfig.set(Configs.SCHEMA_SEPARATOR, "|");
    Object previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", separatorConfig, true);

    try {
      Assertions.assertEquals("|", HierarchicalSchemaUtil.schemaSeparator());

      String parentName = hierarchicalName("raw", "events");
      String childName = hierarchicalName("raw", "events", "2024");
      String siblingName = hierarchicalName("raw", "events2");

      SchemaEntity parent = TestUtil.getTestSchemaEntity(2L, parentName, CATALOG_NS, "cmt");
      SchemaEntity child = TestUtil.getTestSchemaEntity(3L, childName, CATALOG_NS, "cmt");
      SchemaEntity sibling = TestUtil.getTestSchemaEntity(4L, siblingName, CATALOG_NS, "cmt");
      TableEntity tableInChild =
          TestUtil.getTestTableEntity(5L, "t_child", schemaNamespace(childName));

      cache.put(parent);
      cache.put(child);
      cache.put(sibling);
      cache.put(tableInChild);

      cache.invalidate(parent.nameIdentifier(), Entity.EntityType.SCHEMA);

      Assertions.assertFalse(cache.contains(parent.nameIdentifier(), Entity.EntityType.SCHEMA));
      Assertions.assertFalse(cache.contains(child.nameIdentifier(), Entity.EntityType.SCHEMA));
      Assertions.assertFalse(
          cache.contains(tableInChild.nameIdentifier(), Entity.EntityType.TABLE));
      Assertions.assertTrue(cache.contains(sibling.nameIdentifier(), Entity.EntityType.SCHEMA));
      Assertions.assertEquals(1, cache.size());
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    }
  }

  @Test
  void testInvalidateLeafDoesNotEvictSameNameEntityOfAnotherType() {
    // A cache key is "<identifier>:<type>", and ":" is also the default schema separator. Only a
    // schema can nest, so the schema-separator scan must not run for other types, otherwise
    // invalidating a table would also drop the topic of the same name.
    Namespace schemaNs = schemaNamespace("schema1");
    TableEntity table = TestUtil.getTestTableEntity(2L, "shared_name", schemaNs);
    TopicEntity topic = TestUtil.getTestTopicEntity(3L, "shared_name", schemaNs, "cmt");

    cache.put(table);
    cache.put(topic);

    cache.invalidate(table.nameIdentifier(), Entity.EntityType.TABLE);

    Assertions.assertFalse(cache.contains(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(cache.contains(topic.nameIdentifier(), Entity.EntityType.TOPIC));
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
