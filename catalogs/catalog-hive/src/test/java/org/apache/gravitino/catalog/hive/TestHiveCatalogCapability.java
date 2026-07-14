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
package org.apache.gravitino.catalog.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestHiveCatalogCapability {

  private HiveCatalogCapability capability;

  @BeforeEach
  void setUp() {
    capability = new HiveCatalogCapability();
  }

  @Test
  void testCaseSensitiveOnSchemaTableColumnIsUnsupported() {
    for (Capability.Scope scope :
        new Capability.Scope[] {
          Capability.Scope.SCHEMA, Capability.Scope.TABLE, Capability.Scope.COLUMN
        }) {
      CapabilityResult result = capability.caseSensitiveOnName(scope);
      assertFalse(result.supported(), "Hive is case insensitive for " + scope);
    }
  }

  @Test
  void testCaseSensitiveOnOtherScopesIsSupported() {
    CapabilityResult result = capability.caseSensitiveOnName(Capability.Scope.FILESET);
    assertTrue(result.supported());
  }

  @Test
  void testNormalizeNameLowercasesSchemaTableColumnNames() {
    // Regression test: normalizeName must honor this instance's own caseSensitiveOnName
    // override rather than falling back to case-sensitive DEFAULT behavior.
    assertEquals("myschema", capability.normalizeName(Capability.Scope.SCHEMA, "MySchema"));
    assertEquals("mytable", capability.normalizeName(Capability.Scope.TABLE, "MyTable"));
    assertEquals("mycolumn", capability.normalizeName(Capability.Scope.COLUMN, "MyColumn"));
  }

  @Test
  void testNormalizeNameKeepsOtherScopeNameCase() {
    assertEquals("MyFileset", capability.normalizeName(Capability.Scope.FILESET, "MyFileset"));
  }

  @Test
  void testNormalizeNameAllowsNullNameToPassThrough() {
    assertNull(capability.normalizeName(Capability.Scope.TABLE, null));
  }
}
