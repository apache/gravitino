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
  void testMetadataObjectKeyMetalake() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        key("ml1", ""), JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", obj));
  }

  @Test
  void testMetadataObjectKeyCatalog() {
    MetadataObject obj =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        key("ml1", "cat1", ""), JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", obj));
  }

  @Test
  void testMetadataObjectKeySchema() {
    MetadataObject obj =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", ""),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", obj));
  }

  @Test
  void testMetadataObjectKeyLeafTypesGetTypeSuffix() {
    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "tbl1", "TABLE"),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", table));

    MetadataObject view =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "v1"), MetadataObject.Type.VIEW);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "v1", "VIEW"),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", view));

    MetadataObject fileset =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "fs1"), MetadataObject.Type.FILESET);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "fs1", "FILESET"),
        JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", fileset));
  }

  @Test
  void testPrincipalRoleKeysAreTyped() {
    Assertions.assertEquals(
        key("USER", "ml1", "alice"), JcasbinAuthorizationCacheKeys.userRoleKey("ml1", "alice"));
    Assertions.assertEquals(
        key("GROUP", "ml1", "admins"), JcasbinAuthorizationCacheKeys.groupRoleKey("ml1", "admins"));
  }

  @Test
  void testPrincipalRoleKeysAreDistinctFromMetadataKeys() {
    MetadataObject metalake =
        MetadataObjects.of(Collections.singletonList("ml1"), MetadataObject.Type.METALAKE);
    String metalakeKey = JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", metalake);
    String userKey = JcasbinAuthorizationCacheKeys.userRoleKey("ml1", "alice");
    String groupKey = JcasbinAuthorizationCacheKeys.groupRoleKey("ml1", "alice");

    Assertions.assertNotEquals(metalakeKey, userKey);
    Assertions.assertNotEquals(metalakeKey, groupKey);
    Assertions.assertNotEquals(userKey, groupKey);
  }

  @Test
  void testIsMetadataContainerContainerTypes() {
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.SCHEMA));
  }

  @Test
  void testIsMetadataContainerLeafTypes() {
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.TABLE));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.VIEW));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.FILESET));
    Assertions.assertFalse(
        JcasbinAuthorizationCacheKeys.isMetadataContainer(MetadataObject.Type.TOPIC));
  }

  @Test
  void testPrefixInvalidationCoversContainerPath() {
    MetadataObject catalog =
        MetadataObjects.of(Collections.singletonList("cat1"), MetadataObject.Type.CATALOG);
    String catalogKey = JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", catalog);

    MetadataObject schema =
        MetadataObjects.of(Arrays.asList("cat1", "sch1"), MetadataObject.Type.SCHEMA);
    String schemaKey = JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", schema);

    MetadataObject table =
        MetadataObjects.of(Arrays.asList("cat1", "sch1", "tbl1"), MetadataObject.Type.TABLE);
    String tableKey = JcasbinAuthorizationCacheKeys.metadataObjectKey("ml1", table);

    Assertions.assertTrue(schemaKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(catalogKey));
    Assertions.assertTrue(tableKey.startsWith(schemaKey));
  }

  private static String key(String... parts) {
    return String.join(JcasbinAuthorizationCacheKeys.SEPARATOR, parts);
  }
}
