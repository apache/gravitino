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

/** Tests for {@link JcasbinAuthorizationCacheKeys}. */
public class TestJcasbinAuthorizationCacheKeys {

  @Test
  void testMetadataIdCacheKey() {
    MetadataObject metalake =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        key("ml1", "METALAKE", ""),
        JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", metalake));

    MetadataObject catalog =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String catalogKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", catalog);
    Assertions.assertEquals(key("ml1", "CATALOG", "cat1", ""), catalogKey);

    MetadataObject schema =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String schemaKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", schema);
    Assertions.assertEquals(key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", ""), schemaKey);

    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String tableKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", table);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", "TABLE", "tbl1", ""), tableKey);

    MetadataObject column =
        MetadataObjects.of(
            Arrays.asList("cat1", "sch1", "tbl1", "col1"), MetadataObject.Type.COLUMN);
    String columnKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", column);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", "TABLE", "tbl1", "COLUMN", "col1"),
        columnKey);

    MetadataObject view =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.VIEW);
    String viewKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey("ml1", view);
    Assertions.assertEquals(
        key("ml1", "CATALOG", "cat1", "SCHEMA", "sch1", "VIEW", "tbl1"), viewKey);

    Assertions.assertTrue(schemaKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(schemaKey));
    Assertions.assertTrue(columnKey.startsWith(tableKey));
    Assertions.assertFalse(viewKey.startsWith(tableKey));
  }

  @Test
  void testHasNestedMetadataObjects() {
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.TABLE));

    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.VIEW));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.FILESET));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(MetadataObject.Type.COLUMN));
  }

  @Test
  void testPrincipalRoleKeysAreTyped() {
    Assertions.assertEquals(
        key("ml1", JcasbinAuthorizationCacheKeys.USER_ROLE_REL, "alice"),
        JcasbinAuthorizationCacheKeys.userRoleKey("ml1", "alice"));
    Assertions.assertEquals(
        key("ml1", JcasbinAuthorizationCacheKeys.GROUP_ROLE_REL, "admins"),
        JcasbinAuthorizationCacheKeys.groupRoleKey("ml1", "admins"));
  }

  @Test
  void testPrincipalRoleKeysAreDistinct() {
    String userKey = JcasbinAuthorizationCacheKeys.userRoleKey("ml1", "alice");
    String groupKey = JcasbinAuthorizationCacheKeys.groupRoleKey("ml1", "alice");

    Assertions.assertNotEquals(userKey, groupKey);
  }

  private static String key(String... parts) {
    return JcasbinAuthorizationCacheKeys.joinKeyParts(parts);
  }
}
