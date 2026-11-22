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

import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityCacheHierarchy {

  @Test
  void testDescendantPrefixesUseNameLevelBoundary() {
    NameIdentifier catalog1 = NameIdentifier.of("metalake", "catalog1");
    List<String> prefixes =
        EntityCacheHierarchy.descendantPrefixes(catalog1, Entity.EntityType.CATALOG);

    Assertions.assertEquals(1, prefixes.size());
    Assertions.assertTrue(
        EntityCacheHierarchy.isUnderPrefix(
            NameIdentifier.of("metalake", "catalog1", "schema"), prefixes.get(0)));
    Assertions.assertFalse(
        EntityCacheHierarchy.isUnderPrefix(
            NameIdentifier.of("metalake", "catalog10"), prefixes.get(0)));
    Assertions.assertFalse(
        EntityCacheHierarchy.isUnderPrefix(
            NameIdentifier.of("metalake", "catalog1"), prefixes.get(0)));
  }

  @Test
  void testSchemaAddsHierarchicalSeparatorPrefix() {
    NameIdentifier schema = NameIdentifier.of("metalake", "catalog", "schema");
    List<String> prefixes =
        EntityCacheHierarchy.descendantPrefixes(schema, Entity.EntityType.SCHEMA);

    Assertions.assertEquals(2, prefixes.size());
    Assertions.assertEquals(
        EntityCacheHierarchy.indexText(schema) + EntityCacheHierarchy.NAME_LEVEL_BOUNDARY,
        prefixes.get(0));
    Assertions.assertEquals(
        EntityCacheHierarchy.indexText(schema) + HierarchicalSchemaUtil.schemaSeparator(),
        prefixes.get(1));
  }
}
