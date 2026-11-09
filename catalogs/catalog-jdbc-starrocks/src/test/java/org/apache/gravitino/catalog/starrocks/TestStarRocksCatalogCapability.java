/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks;

import org.apache.gravitino.connector.capability.Capability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Existing-name capability regression tests. */
class TestStarRocksCatalogCapability {
  @Test
  void testExistingColumnNamesUseSafeNativeReferences() {
    var capability = new StarRocksCatalogCapability();
    for (String name : new String[] {"native column", "native.column", "col`name", "col\"name"}) {
      Assertions.assertTrue(
          capability.specificationOnExistingName(Capability.Scope.COLUMN, name).supported());
    }
    for (String invalid : new String[] {"", "bad\0name", "*"}) {
      Assertions.assertFalse(
          capability.specificationOnExistingName(Capability.Scope.COLUMN, invalid).supported());
    }
  }

  @Test
  void testExistingQuotedTableNamesDoNotRelaxCreationOrSchemaRules() {
    var capability = new StarRocksCatalogCapability();
    Assertions.assertTrue(
        capability.specificationOnExistingName(Capability.Scope.TABLE, "native table").supported());
    Assertions.assertFalse(
        capability.specificationOnName(Capability.Scope.TABLE, "native.table").supported());
    Assertions.assertFalse(
        capability
            .specificationOnExistingName(Capability.Scope.SCHEMA, "native schema")
            .supported());
    for (String invalid : new String[] {"", "native.table", "bad\0name", "*"}) {
      Assertions.assertFalse(
          capability.specificationOnExistingName(Capability.Scope.TABLE, invalid).supported());
    }
  }
}
