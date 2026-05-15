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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link JcasbinAuthorizationLookups} static helpers. */
public class TestJcasbinAuthorizationLookups {

  // ---------- buildCacheKey ----------

  @Test
  void testBuildCacheKeyMetalake() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    Assertions.assertEquals("ml1::", JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeyCatalog() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    Assertions.assertEquals("ml1::cat1::", JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeySchema() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        "ml1::cat1::sch1::", JcasbinAuthorizationLookups.buildCacheKey("ml1", obj));
  }

  @Test
  void testBuildCacheKeyLeafTypesGetTypeSuffix() {
    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        "ml1::cat1::sch1::tbl1::TABLE", JcasbinAuthorizationLookups.buildCacheKey("ml1", table));

    MetadataObject view =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "v1"), MetadataObject.Type.VIEW);
    Assertions.assertEquals(
        "ml1::cat1::sch1::v1::VIEW", JcasbinAuthorizationLookups.buildCacheKey("ml1", view));

    MetadataObject fileset =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "fs1"), MetadataObject.Type.FILESET);
    Assertions.assertEquals(
        "ml1::cat1::sch1::fs1::FILESET", JcasbinAuthorizationLookups.buildCacheKey("ml1", fileset));
  }

  // ---------- isNonLeaf ----------

  @Test
  void testIsNonLeafContainerTypes() {
    Assertions.assertTrue(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.SCHEMA));
  }

  @Test
  void testIsNonLeafLeafTypes() {
    Assertions.assertFalse(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.TABLE));
    Assertions.assertFalse(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.VIEW));
    Assertions.assertFalse(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.FILESET));
    Assertions.assertFalse(JcasbinAuthorizationLookups.isNonLeaf(MetadataObject.Type.TOPIC));
  }

  // ---------- Cascade prefix hierarchy ----------

  @Test
  void testCascadeInvalidationKeyHierarchy() {
    // Dropping a catalog should use a prefix that covers all schemas and tables below it.
    MetadataObject catalog =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String catalogKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", catalog);

    MetadataObject schema =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String schemaKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", schema);

    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String tableKey = JcasbinAuthorizationLookups.buildCacheKey("ml1", table);

    Assertions.assertTrue(schemaKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(schemaKey));
  }
}
