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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogCapability {

  @Test
  public void testSchemaNameSupportsConfiguredSeparator() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "team:sales");
    Assertions.assertTrue(result.supported());
  }

  @Test
  public void testSchemaNameSupportsDeepNesting() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team:sales:reports");
    Assertions.assertTrue(result.supported());
  }

  @Test
  public void testSchemaNameRejectsEmptySegments() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team::sales");
    Assertions.assertFalse(result.supported());
  }

  @Test
  public void testSchemaNameRejectsLeadingSeparator() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, ":sales");
    Assertions.assertFalse(result.supported());
  }

  @Test
  public void testSchemaNameRejectsTrailingSeparator() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "sales:");
    Assertions.assertFalse(result.supported());
  }

  @Test
  public void testFlatSchemaNameAllowed() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "flat");
    Assertions.assertTrue(result.supported());
  }

  @Test
  public void testNonSchemaScopeStillUsesDefaultRules() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.TABLE, "table:name");
    Assertions.assertFalse(result.supported());
  }

  @Test
  public void testDefaultConstructorUsesColonSeparator() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    CapabilityResult result = capability.specificationOnName(Capability.Scope.SCHEMA, "team:sales");
    Assertions.assertTrue(result.supported());
  }

  @Test
  public void testSupportsHierarchicalSchema() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":");

    Assertions.assertTrue(capability.supportsHierarchicalSchema().supported());
  }

  @Test
  public void testHiveBackendFoldsSchemaTableAndViewNamesOnly() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":", true);

    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.SCHEMA).supported());
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.TABLE).supported());
    Assertions.assertFalse(capability.caseSensitiveOnName(Capability.Scope.VIEW).supported());
    Assertions.assertEquals("schema", capability.normalizeName(Capability.Scope.SCHEMA, "Schema"));
    Assertions.assertEquals("table", capability.normalizeName(Capability.Scope.TABLE, "Table"));
    Assertions.assertEquals("view", capability.normalizeName(Capability.Scope.VIEW, "View"));

    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.COLUMN).supported());
    Assertions.assertEquals("Column", capability.normalizeName(Capability.Scope.COLUMN, "Column"));
  }

  @Test
  public void testNonHiveBackendKeepsEntityNameCase() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability(":", false);

    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.SCHEMA).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.TABLE).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.VIEW).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.COLUMN).supported());
    Assertions.assertEquals("Schema", capability.normalizeName(Capability.Scope.SCHEMA, "Schema"));
    Assertions.assertEquals("Table", capability.normalizeName(Capability.Scope.TABLE, "Table"));
    Assertions.assertEquals("View", capability.normalizeName(Capability.Scope.VIEW, "View"));
    Assertions.assertEquals("Column", capability.normalizeName(Capability.Scope.COLUMN, "Column"));
  }

  @Test
  public void testCustomSeparatorSlash() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability("/");

    CapabilityResult validResult =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team/sales");
    Assertions.assertTrue(validResult.supported());

    CapabilityResult invalidResult =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team//sales");
    Assertions.assertFalse(invalidResult.supported());
  }
}
