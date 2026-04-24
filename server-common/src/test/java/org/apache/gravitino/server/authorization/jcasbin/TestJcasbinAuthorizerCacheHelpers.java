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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for JcasbinAuthorizer static helpers and cached role snapshot classes. */
public class TestJcasbinAuthorizerCacheHelpers {

  // ---------- buildCacheKey ----------

  @Test
  void testBuildCacheKeyMetalake() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    // Metalake is non-leaf → trailing "::"
    Assertions.assertEquals("ml1::ml1::", key);
  }

  @Test
  void testBuildCacheKeyCatalog() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    Assertions.assertEquals("ml1::cat1::", key);
  }

  @Test
  void testBuildCacheKeySchema() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    // Schema is non-leaf → trailing "::"
    Assertions.assertEquals("ml1::cat1::sch1::", key);
  }

  @Test
  void testBuildCacheKeyTable() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    // Table is a leaf → type suffix
    Assertions.assertEquals("ml1::cat1::sch1::tbl1::TABLE", key);
  }

  @Test
  void testBuildCacheKeyFileset() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "fs1"), MetadataObject.Type.FILESET);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    Assertions.assertEquals("ml1::cat1::sch1::fs1::FILESET", key);
  }

  @Test
  void testBuildCacheKeyTopic() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "topic1"), MetadataObject.Type.TOPIC);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    Assertions.assertEquals("ml1::cat1::sch1::topic1::TOPIC", key);
  }

  @Test
  void testBuildCacheKeyModel() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "model1"), MetadataObject.Type.MODEL);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    Assertions.assertEquals("ml1::cat1::sch1::model1::MODEL", key);
  }

  @Test
  void testBuildCacheKeyView() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "v1"), MetadataObject.Type.VIEW);
    String key = JcasbinAuthorizer.buildCacheKey("ml1", obj);
    Assertions.assertEquals("ml1::cat1::sch1::v1::VIEW", key);
  }

  // ---------- isNonLeaf ----------

  @Test
  void testIsNonLeafMetalake() {
    Assertions.assertTrue(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.METALAKE));
  }

  @Test
  void testIsNonLeafCatalog() {
    Assertions.assertTrue(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.CATALOG));
  }

  @Test
  void testIsNonLeafSchema() {
    Assertions.assertTrue(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.SCHEMA));
  }

  @Test
  void testIsLeafTable() {
    Assertions.assertFalse(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.TABLE));
  }

  @Test
  void testIsLeafFileset() {
    Assertions.assertFalse(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.FILESET));
  }

  @Test
  void testIsLeafTopic() {
    Assertions.assertFalse(JcasbinAuthorizer.isNonLeaf(MetadataObject.Type.TOPIC));
  }

  // ---------- Prefix cascade key hierarchy ----------

  @Test
  void testCascadeInvalidationKeyHierarchy() {
    // Dropping a catalog should use a prefix that covers all schemas and tables below it
    MetadataObject catalog =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String catalogKey = JcasbinAuthorizer.buildCacheKey("ml1", catalog);

    MetadataObject schema =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String schemaKey = JcasbinAuthorizer.buildCacheKey("ml1", schema);

    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String tableKey = JcasbinAuthorizer.buildCacheKey("ml1", table);

    // Schema key starts with catalog key prefix
    Assertions.assertTrue(
        schemaKey.startsWith(catalogKey),
        "schema key should start with catalog prefix for cascade invalidation");
    // Table key starts with catalog key prefix
    Assertions.assertTrue(
        tableKey.startsWith(catalogKey),
        "table key should start with catalog prefix for cascade invalidation");
    // Table key starts with schema key prefix
    Assertions.assertTrue(
        tableKey.startsWith(schemaKey),
        "table key should start with schema prefix for cascade invalidation");
  }

  // ---------- CachedUserRoles ----------

  @Test
  void testCachedUserRoles() {
    List<Long> roleIds = Arrays.asList(10L, 20L, 30L);
    CachedUserRoles cur = new CachedUserRoles(1L, 1000L, roleIds);
    Assertions.assertEquals(1L, cur.getUserId());
    Assertions.assertEquals(1000L, cur.getUpdatedAt());
    Assertions.assertEquals(roleIds, cur.getRoleIds());
  }

  // ---------- CachedGroupRoles ----------

  @Test
  void testCachedGroupRoles() {
    List<Long> roleIds = Arrays.asList(100L, 200L);
    CachedGroupRoles cgr = new CachedGroupRoles(5L, 2000L, roleIds);
    Assertions.assertEquals(5L, cgr.getGroupId());
    Assertions.assertEquals(2000L, cgr.getUpdatedAt());
    Assertions.assertEquals(roleIds, cgr.getRoleIds());
  }
}
