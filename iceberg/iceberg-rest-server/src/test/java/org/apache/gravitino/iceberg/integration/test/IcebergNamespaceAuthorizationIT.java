/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Iceberg namespace (schema) authorization functionality.
 *
 * <p>These tests verify that the authorization system correctly controls access to schema
 * operations including creation, listing, modification, and deletion. Tests cover both
 * ownership-based and privilege-based authorization models.
 */
@Tag("gravitino-docker-test")
public class IcebergNamespaceAuthorizationIT extends IcebergAuthorizationIT {

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
  }

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    sql("USE rest;");
  }

  @Test
  void testCreateSchemaWithOwner() {
    String namespace = "ns_owner";

    // Catalog ownership enables schema creation
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(Arrays.asList(GRAVITINO_CATALOG_NAME), MetadataObject.Type.CATALOG),
        NORMAL_USER,
        Owner.Type.USER);

    sql("CREATE DATABASE %s", namespace);

    // Verify schema owner is automatically set to the creator
    Optional<Owner> schemaOwner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(schemaOwner.isPresent());
    Assertions.assertEquals(NORMAL_USER, schemaOwner.get().name());

    // Test table creation within the owned schema
    String tableName = "test_table";
    sql("USE %s", namespace);
    sql("CREATE TABLE %s(id int, name string) USING iceberg", tableName);
    sql("DESC TABLE %s", tableName);

    // Verify table ownership is inherited
    Optional<Owner> tableOwner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, namespace, tableName),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(tableOwner.isPresent());
    Assertions.assertEquals(NORMAL_USER, tableOwner.get().name());
  }

  @Test
  void testCreateNamespaceAuthorization() {
    String namespace = "ns_unauthorized";

    // Should fail without proper authorization
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("CREATE DATABASE %s", namespace));

    // Grant CREATE_SCHEMA and USE_CATALOG privileges and verify creation succeeds
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantCreateSchemaRole(GRAVITINO_CATALOG_NAME);

    Assertions.assertDoesNotThrow(() -> sql("CREATE DATABASE %s", namespace));
    Assertions.assertDoesNotThrow(() -> sql("USE %s", namespace));

    // Verify schema owner is automatically set to the creator
    Optional<Owner> schemaOwner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(schemaOwner.isPresent());
    Assertions.assertEquals(NORMAL_USER, schemaOwner.get().name());
  }

  @Test
  void testListNamespaces() {
    String namespace1 = "ns_list_hidden1";
    String namespace2 = "ns_list_hidden2";

    // Create schemas as admin
    catalogClientWithAllPrivilege.asSchemas().createSchema(namespace1, "test", new HashMap<>());
    catalogClientWithAllPrivilege.asSchemas().createSchema(namespace2, "test", new HashMap<>());

    // Normal user should not see unauthorized schemas
    List<Object[]> result = sql("SHOW DATABASES");
    List<String> visibleSchemas = extractSchemaNames(result);

    Assertions.assertFalse(
        visibleSchemas.contains(namespace1),
        "Should not see " + namespace1 + " without USE_SCHEMA privilege");
    Assertions.assertFalse(
        visibleSchemas.contains(namespace2),
        "Should not see " + namespace2 + " without USE_SCHEMA privilege");

    // Without any roles, even SHOW DATABASES should fail
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class, () -> sql("SHOW DATABASES"));
  }

  @Test
  void testListNamespacesWithFiltering() {
    String visibleSchema = "ns_visible";
    String hiddenSchema = "ns_hidden";

    // Create test schemas
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(visibleSchema, "visible schema", new HashMap<>());
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(hiddenSchema, "hidden schema", new HashMap<>());

    // Grant access to only one schema
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(visibleSchema);

    List<Object[]> result = sql("SHOW DATABASES");
    List<String> visibleSchemas = extractSchemaNames(result);

    Assertions.assertTrue(
        visibleSchemas.contains(visibleSchema), "Should see authorized schema: " + visibleSchema);
    Assertions.assertFalse(
        visibleSchemas.contains(hiddenSchema),
        "Should not see unauthorized schema: " + hiddenSchema);
  }

  @Test
  void testUseNamespace() {
    String namespace = "ns_access_test";

    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(namespace, "test schema", new HashMap<>());

    // Access should fail without proper privileges
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class, () -> sql("USE %s", namespace));

    // Grant schema access and verify success
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(namespace);
    Assertions.assertDoesNotThrow(() -> sql("USE %s", namespace));
  }

  @Test
  void testUpdateNamespace() {
    String namespace = "ns_modification";

    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(namespace, "test schema", new HashMap<>());
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);

    // Non-owners cannot modify schemas even with USE_CATALOG privilege
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("ALTER DATABASE %s SET DBPROPERTIES ('key'='value')", namespace));

    // Schema ownership enables modification
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);

    grantUseSchemaRole(namespace);

    Assertions.assertDoesNotThrow(
        () ->
            catalogClientWithAllPrivilege
                .asSchemas()
                .alterSchema(namespace, SchemaChange.setProperty("modified-by", "owner")));
  }

  @Test
  void testDropNamespace() {
    String namespace = "ns_deletion";

    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(namespace, "test schema", new HashMap<>());

    // Set different owner initially
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA),
        SUPER_USER,
        Owner.Type.USER);

    // Non-owner cannot delete schema
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("DROP DATABASE %s", namespace));

    // Transfer ownership to normal user
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);

    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(namespace);

    // Owner can delete schema
    Assertions.assertDoesNotThrow(() -> sql("DROP DATABASE %s", namespace));

    boolean exists = catalogClientWithAllPrivilege.asSchemas().schemaExists(namespace);
    Assertions.assertFalse(exists, "Schema should be deleted");
  }

  @Test
  void testNamespaceExistenceCheck() {
    String namespace = "ns_existence";

    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(namespace, "test schema", new HashMap<>());

    // Schema access requires proper authorization
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class, () -> sql("USE %s", namespace));

    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(namespace);
    Assertions.assertDoesNotThrow(() -> sql("USE %s", namespace));
    Assertions.assertDoesNotThrow(() -> sql("SHOW DATABASES"));
  }

  @Test
  void testRegisterTable() {
    String sourceNamespace = "ns_register_source";
    String destNamespace = "ns_register_dst";
    String sourceTable = "source_table";
    String registeredTable = "registered_table";

    // Create both schemas as admin
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(sourceNamespace, "source schema for registration test", new HashMap<>());
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(destNamespace, "destination schema for registration test", new HashMap<>());

    // Grant source schema ownership to create source table
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, sourceNamespace), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(sourceNamespace);

    // Create source table and extract metadata location
    sql("USE %s", sourceNamespace);
    sql("CREATE TABLE %s(id bigint, data string) USING iceberg", sourceTable);
    sql("INSERT INTO %s VALUES (1, 'test data')", sourceTable);

    List<Object[]> metadataResults =
        sql("SELECT file FROM %s.%s.metadata_log_entries", sourceNamespace, sourceTable);
    String metadataLocation = (String) metadataResults.get(metadataResults.size() - 1)[0];

    // Drop the original table to simulate external registration scenario
    sql("DROP TABLE %s", sourceTable);

    // Reset to limited privileges for testing registration authorization
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);

    // Registration should fail without destination schema ownership
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () ->
            sql(
                "CALL rest.system.register_table(table => '%s.%s', metadata_file=> '%s')",
                destNamespace, registeredTable, metadataLocation));

    // Grant destination schema ownership and verify registration succeeds
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, destNamespace), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    grantUseSchemaRole(destNamespace);

    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "CALL rest.system.register_table(table => '%s.%s', metadata_file=> '%s')",
                destNamespace, registeredTable, metadataLocation));

    // Verify table ownership and accessibility
    Optional<Owner> tableOwner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, destNamespace, registeredTable),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(tableOwner.isPresent());
    Assertions.assertEquals(NORMAL_USER, tableOwner.get().name());

    sql("USE %s", destNamespace);
    Assertions.assertDoesNotThrow(() -> sql("DESC TABLE %s", registeredTable));
  }

  @Test
  void testComplexAuthorizations() {
    String namespace = "ns_complex_auth";

    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(namespace, "test schema", new HashMap<>());

    revokeUserRoles();
    resetMetalakeAndCatalogOwner();

    // Verify operations fail without authorization
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("ALTER DATABASE %s SET DBPROPERTIES ('key1'='value1')", namespace));

    // USE_CATALOG alone is insufficient for schema modification
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("ALTER DATABASE %s SET DBPROPERTIES ('key2'='value2')", namespace));

    // Schema ownership with USE_CATALOG enables modification
    metalakeClientWithAllPrivilege.setOwner(
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);

    grantUseSchemaRole(namespace);
    Assertions.assertDoesNotThrow(() -> sql("USE %s", namespace));
  }

  @Test
  void testAuthorizationException() {
    // Authorization is checked before existence - security best practice
    String nonExistentSchema = "non_existent_schema";
    Assertions.assertThrowsExactly(
        org.apache.iceberg.exceptions.ForbiddenException.class,
        () -> sql("USE %s", nonExistentSchema));

    // Special characters in schema names should work with authorization
    String specialSchema = "schema_with_123_special";
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(specialSchema, "special characters test", new HashMap<>());

    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    grantUseSchemaRole(specialSchema);
    Assertions.assertDoesNotThrow(() -> sql("USE %s", specialSchema));
  }

  /** Grants USE_CATALOG privilege to the normal user for the specified catalog. */
  private void grantUseCatalogRole(String catalogName) {
    String roleName = "useCatalog_" + UUID.randomUUID();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));

    metalakeClientWithAllPrivilege.createRole(
        roleName, new HashMap<>(), ImmutableList.of(catalogObject));
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  /** Grants USE_SCHEMA privilege to the normal user for the specified schema. */
  private void grantUseSchemaRole(String schemaName) {
    String roleName = "useSchema_" + UUID.randomUUID();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(GRAVITINO_CATALOG_NAME, ImmutableList.of());
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schemaName, ImmutableList.of(Privileges.UseSchema.allow()));

    metalakeClientWithAllPrivilege.createRole(
        roleName, new HashMap<>(), ImmutableList.of(schemaObject));
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  /** Grants CREATE_SCHEMA privilege to the normal user for the specified catalog. */
  private void grantCreateSchemaRole(String catalogName) {
    String roleName = "createSchema_" + UUID.randomUUID();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.CreateSchema.allow()));

    metalakeClientWithAllPrivilege.createRole(
        roleName, new HashMap<>(), ImmutableList.of(catalogObject));
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  /** Extracts schema names from SHOW DATABASES result, handling various result formats. */
  private List<String> extractSchemaNames(List<Object[]> result) {
    List<String> schemaNames = new ArrayList<>();
    for (Object[] row : result) {
      if (row.length > 1 && row[1] != null) {
        schemaNames.add((String) row[1]);
      } else if (row.length == 1 && row[0] != null) {
        schemaNames.add((String) row[0]);
      }
    }
    return schemaNames;
  }
}
