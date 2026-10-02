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

import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergCatalogCapability {

  @ParameterizedTest
  @ValueSource(strings = {"hive", "HIVE"})
  public void testHiveBackendNormalizesTableAndSchemaAliases(String backend) {
    Capability capability = catalogCapability(backend);
    NameIdentifier registered =
        NameIdentifier.of("metalake", "iceberg", "default_db", "test_table");
    NameIdentifier alias = NameIdentifier.of("metalake", "iceberg", "DEFAULT_DB", "TEST_TABLE");

    // Both SQL spellings must reach the same registration before the identity fence runs.
    Assertions.assertEquals(
        registered,
        CapabilityHelpers.applyCaseSensitive(alias, Capability.Scope.TABLE, capability));
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "iceberg", "default_db"),
        CapabilityHelpers.applyCaseSensitive(
            NameIdentifier.of("metalake", "iceberg", "DEFAULT_DB"),
            Capability.Scope.SCHEMA,
            capability));
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.COLUMN).supported());
    Assertions.assertEquals(
        "MixedCaseColumn",
        CapabilityHelpers.applyCaseSensitiveOnName(
            Capability.Scope.COLUMN, "MixedCaseColumn", capability));
  }

  @ParameterizedTest
  @ValueSource(strings = {"jdbc", "rest", "memory", "custom"})
  public void testOtherBackendsPreserveCaseSensitiveNames(String backend) {
    Capability capability = catalogCapability(backend);
    NameIdentifier ident = NameIdentifier.of("metalake", "iceberg", "MixedSchema", "MixedTable");

    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.SCHEMA).supported());
    Assertions.assertTrue(capability.caseSensitiveOnName(Capability.Scope.TABLE).supported());
    Assertions.assertEquals(
        ident, CapabilityHelpers.applyCaseSensitive(ident, Capability.Scope.TABLE, capability));
  }

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
  public void testCustomSeparatorSlash() {
    IcebergCatalogCapability capability = new IcebergCatalogCapability("/");

    CapabilityResult validResult =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team/sales");
    Assertions.assertTrue(validResult.supported());

    CapabilityResult invalidResult =
        capability.specificationOnName(Capability.Scope.SCHEMA, "team//sales");
    Assertions.assertFalse(invalidResult.supported());
  }

  private Capability catalogCapability(String backend) {
    Map<String, String> properties = Map.of(IcebergConstants.CATALOG_BACKEND, backend);
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("iceberg")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .withProperties(properties)
            .build();
    return new IcebergCatalog().withCatalogConf(properties).withCatalogEntity(entity).capability();
  }
}
