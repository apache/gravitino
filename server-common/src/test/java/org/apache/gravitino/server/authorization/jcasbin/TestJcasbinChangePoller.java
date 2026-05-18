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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link JcasbinChangePoller} static helpers. */
public class TestJcasbinChangePoller {

  @Test
  void testChangeLogFullNameStripsLeadingMetalakeForChildTypes() {
    MetadataObject catalog =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1", MetadataObject.Type.CATALOG);
    Assertions.assertEquals(
        key("ml1", "cat1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", catalog));

    MetadataObject schema =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1", MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", schema));

    MetadataObject table =
        JcasbinChangePoller.metadataObjectFromChangeLog(
            "ml1", "ml1.cat1.sch1.tbl1", MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        key("ml1", "cat1", "sch1", "tbl1", "TABLE"),
        JcasbinAuthorizationLookups.buildCacheKey("ml1", table));
  }

  @Test
  void testChangeLogFullNameForMetalakeKeepsItself() {
    MetadataObject metalake =
        JcasbinChangePoller.metadataObjectFromChangeLog("ml1", "ml1", MetadataObject.Type.METALAKE);
    Assertions.assertEquals(
        key("ml1", ""), JcasbinAuthorizationLookups.buildCacheKey("ml1", metalake));
  }

  private static String key(String... parts) {
    return String.join(HierarchicalSchemaUtil.internalSeparator(), parts);
  }
}
