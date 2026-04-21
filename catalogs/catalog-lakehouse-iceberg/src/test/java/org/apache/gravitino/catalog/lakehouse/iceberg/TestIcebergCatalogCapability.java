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
    IcebergCatalogCapability capability = new IcebergCatalogCapability();

    CapabilityResult result =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team:sales");
    Assertions.assertTrue(result.supported());
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
