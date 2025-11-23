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
import java.util.Set;
import java.util.UUID;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.rel.Table;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class IcebergTableAuthorizationIT extends IcebergAuthorizationIT {

  private static final String SCHEMA_NAME = "schema";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());
  }

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    MetadataObject schemaObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaObject, SUPER_USER, Owner.Type.USER);
    clearTable();
    // Grant user the privilege to use the catalog and schema
    grantUseSchemaRole(SCHEMA_NAME);
    sql("USE %s;", SPARK_CATALOG_NAME);
    sql("USE %s;", SCHEMA_NAME);
  }

  @Test
  void testCreateTable() {
    String tableName = "test_create";
    boolean exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(exists);
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("CREATE TABLE %s(a int)", tableName));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(exists);

    String roleName = grantCreateTableRole(SCHEMA_NAME);
    Assertions.assertDoesNotThrow(() -> sql("CREATE TABLE %s(a int)", tableName));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertTrue(exists);
    Assertions.assertDoesNotThrow(
        () ->
            Assertions.assertDoesNotThrow(() -> sql("CREATE TABLE %s(a int)", "anotherTableName")));

    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());

    Assertions.assertThrowsExactly(
        TableAlreadyExistsException.class, () -> sql("CREATE TABLE %s(a int)", tableName));

    // test create table with schema owner
    revokeRole(roleName);
    setSchemaOwner(NORMAL_USER);
    String tableName2 = "test_create_2";
    Assertions.assertDoesNotThrow(() -> sql("CREATE TABLE %s(a int)", tableName2));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName2));
    Assertions.assertTrue(exists);

    setSchemaOwner(SUPER_USER);
    String tableName3 = "test_create_3";
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("CREATE TABLE %s(a int)", tableName3));

    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName3));
    Assertions.assertFalse(exists);
  }

  @Test
  void testLoadTable() {
    String tableName = "test_load";
    createTable(SCHEMA_NAME, tableName);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DESC TABLE %s", tableName));

    String roleName = grantSelectTableRole(tableName);
    Assertions.assertDoesNotThrow(() -> sql("DESC TABLE %s", tableName));

    revokeRole(roleName);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DESC TABLE %s", tableName));

    setSchemaOwner(NORMAL_USER);
    Assertions.assertDoesNotThrow(() -> sql("DESC TABLE %s", tableName));
    // With ownership: non-existent table returns 404 (AnalysisException)
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("DESC TABLE %s_not_exist", tableName));

    setSchemaOwner(SUPER_USER);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DESC TABLE %s", tableName));
    // Without ownership: non-existent table also returns 403 (ForbiddenException)
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("DESC TABLE %s_not_exist", tableName));
  }

  @Test
  void testDropTable() {
    String tableName = "test_drop";
    createTable(SCHEMA_NAME, tableName);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DROP TABLE %s", tableName));
    boolean exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertTrue(exists);

    setTableOwner(tableName);
    Assertions.assertDoesNotThrow(() -> sql("DROP TABLE %s", tableName));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(exists);

    // recreate the dropped table
    createTable(SCHEMA_NAME, tableName);
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(SUPER_USER, owner.get().name());
    setTableOwner(tableName);
    Assertions.assertDoesNotThrow(() -> sql("DROP TABLE %s", tableName));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(exists);
  }

  @Test
  void testUpdateTable() {
    String tableName = "test_update";
    createTable(SCHEMA_NAME, tableName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('a'='b')", tableName));
    Table table =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(table.properties().containsKey("a"));

    setTableOwner(tableName);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('a'='b')", tableName));
    table =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertTrue(table.properties().containsKey("a"));
    Assertions.assertEquals("b", table.properties().get("a"));

    // Test with explicit ModifyTable privilege
    String tableName2 = "test_update_with_privilege";
    createTable(SCHEMA_NAME, tableName2);
    String roleName = grantModifyTableRole(tableName2);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('c'='d')", tableName2));
    table =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName2));
    Assertions.assertTrue(table.properties().containsKey("c"));
    Assertions.assertEquals("d", table.properties().get("c"));
    revokeRole(roleName);
  }

  @Test
  void testListTable() {
    createTable(SCHEMA_NAME, "test_list1");
    createTable(SCHEMA_NAME, "test_list2");

    Set<String> tableNames = listTableNames(SCHEMA_NAME);
    Assertions.assertEquals(0, tableNames.size());

    setTableOwner("test_list1");
    Assertions.assertDoesNotThrow(() -> sql("SHOW TABLES in %s", SCHEMA_NAME));
    tableNames = listTableNames(SCHEMA_NAME);
    Assertions.assertEquals(1, tableNames.size());
    Assertions.assertTrue(tableNames.contains("test_list1"));
  }

  @Test
  void testIcebergMetadataOperation() {
    String tableName = "test_basic_op";
    grantCreateTableRole(SCHEMA_NAME);
    sql("CREATE TABLE %s(a int, b int) PARTITIONED BY (a)", tableName);
    sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s", SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s.partitions", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT *,_file FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());
  }

  @Test
  public void testRenameTableSameNamespace() {
    String tableName = "test_rename_same_ns";
    createTable(SCHEMA_NAME, tableName);

    // No privileges - should fail
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("ALTER TABLE %s RENAME TO %s", tableName, tableName + "_renamed"));

    // Test with MODIFY_TABLE privilege
    String modifyTableRole = grantModifyTableRole(tableName);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER TABLE %s RENAME TO %s", tableName, tableName + "_renamed"));

    // Verify ownership remains with original owner (SUPER_USER created the table)
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName + "_renamed"),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(SUPER_USER, owner.get().name());

    // Clean up for next test
    revokeRole(modifyTableRole);
    catalogClientWithAllPrivilege
        .asTableCatalog()
        .dropTable(NameIdentifier.of(SCHEMA_NAME, tableName + "_renamed"));

    // Test with table ownership (without MODIFY_TABLE)
    createTable(SCHEMA_NAME, tableName);
    setTableOwner(tableName);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER TABLE %s RENAME TO %s", tableName, tableName + "_renamed2"));

    // Verify ownership is retained
    owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName + "_renamed2"),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());
  }

  @Test
  public void testRenameTableToDifferentNamespace() {
    String sourceSchema = SCHEMA_NAME;
    String destSchema = SCHEMA_NAME + "_dest";
    String tableName = "test_cross_ns_rename";

    // Create destination schema
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(destSchema, "dest schema", new HashMap<>());
    grantUseSchemaRole(destSchema);

    // Create table in source schema
    createTable(sourceSchema, tableName);

    // Test 1: No privileges - should fail
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName, destSchema, tableName + "_renamed1"));

    // Test 2: Only MODIFY_TABLE on source (no CREATE_TABLE on dest) - should fail
    String modifyTableRole = grantModifyTableRole(tableName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName, destSchema, tableName + "_renamed2"));
    revokeRole(modifyTableRole);

    // Test 3: Only CREATE_TABLE on dest (no ownership on source) - should fail
    String createTableRole = grantCreateTableRole(destSchema);
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName, destSchema, tableName + "_renamed3"));
    revokeRole(createTableRole);

    // Test 4: Table ownership + CREATE_TABLE on dest - should succeed
    setTableOwner(tableName);
    createTableRole = grantCreateTableRole(destSchema);
    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName, destSchema, tableName + "_renamed4"));

    // Verify table exists in destination schema
    boolean existsInDest =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(destSchema, tableName + "_renamed4"));
    Assertions.assertTrue(existsInDest);

    // Verify table no longer exists in source schema
    boolean existsInSource =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(sourceSchema, tableName));
    Assertions.assertFalse(existsInSource);

    // Verify ownership is retained (NORMAL_USER was already the owner before rename)
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, destSchema, tableName + "_renamed4"),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());

    // Clean up destination schema
    revokeRole(createTableRole);
    clearTable(destSchema);
    catalogClientWithAllPrivilege.asSchemas().dropSchema(destSchema, false);
  }

  @Test
  public void testRenameTableToDifferentNamespaceWithOwnership() {
    String sourceSchema = SCHEMA_NAME;
    String destSchema = SCHEMA_NAME + "_dest2";
    String tableName = "test_owner_rename";

    // Create destination schema
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(destSchema, "dest schema for ownership test", new HashMap<>());
    grantUseSchemaRole(destSchema);

    // Test 1: Source schema owner + CREATE_TABLE on dest - should succeed
    createTable(sourceSchema, tableName + "_1");
    setSchemaOwner(NORMAL_USER); // NORMAL_USER owns source schema
    String createTableRole = grantCreateTableRole(destSchema);

    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_1", destSchema, tableName + "_1_renamed"));

    revokeRole(createTableRole);
    setSchemaOwner(SUPER_USER); // Reset source schema owner

    // Test 2: Table owner + destination schema owner - should succeed
    createTable(sourceSchema, tableName + "_2");
    setTableOwner(tableName + "_2");
    setSchemaOwner(destSchema, NORMAL_USER); // NORMAL_USER owns dest schema

    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_2", destSchema, tableName + "_2_renamed"));

    setSchemaOwner(destSchema, SUPER_USER); // Reset dest schema owner

    // Test 3: Both source and dest schema owner - should succeed
    createTable(sourceSchema, tableName + "_3");
    setSchemaOwner(NORMAL_USER); // Source schema owner
    setSchemaOwner(destSchema, NORMAL_USER); // Dest schema owner

    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_3", destSchema, tableName + "_3_renamed"));

    // Test 4: Only source schema owner (no dest privileges) - should fail
    setSchemaOwner(destSchema, SUPER_USER); // Reset dest schema owner
    createTable(sourceSchema, tableName + "_4");

    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_4", destSchema, tableName + "_4_renamed"));

    // Test 5: Only dest schema owner (no source table ownership) - should fail
    setSchemaOwner(SUPER_USER); // Reset source schema owner
    setSchemaOwner(destSchema, NORMAL_USER);

    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_4", destSchema, tableName + "_4_renamed2"));

    // Test 6: Catalog owner - should succeed
    setSchemaOwner(destSchema, SUPER_USER); // Reset dest schema owner
    createTable(sourceSchema, tableName + "_5");
    setCatalogOwner(NORMAL_USER);

    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER TABLE %s.%s RENAME TO %s.%s",
                sourceSchema, tableName + "_5", destSchema, tableName + "_5_renamed"));

    setCatalogOwner(SUPER_USER); // Reset catalog owner

    // Clean up
    setSchemaOwner(SUPER_USER);
    clearTable(destSchema);
    catalogClientWithAllPrivilege.asSchemas().dropSchema(destSchema, false);
  }

  @Test
  void testTableDenyOverridesSchemaAllow() {
    // Test that table-level DENY overrides schema-level ALLOW
    String tableName = "test_table_deny_override";
    createTable(SCHEMA_NAME, tableName);

    // Create a role that:
    // 1. Grants ALLOW on SelectTable at schema level (should apply to all tables)
    // 2. Denies SelectTable at specific table level (should override schema allow)
    String roleName = "tableDenyOverride_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);

    // ALLOW SelectTable at schema level
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject,
            SCHEMA_NAME,
            ImmutableList.of(Privileges.UseSchema.allow(), Privileges.SelectTable.allow()));
    securableObjects.add(schemaObject);

    // DENY SelectTable at table level - this should override the schema-level allow
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.SelectTable.deny()));
    securableObjects.add(tableObject);

    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);

    // Table-level DENY should override schema-level ALLOW, so access should be denied
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DESC TABLE %s", tableName));

    revokeRole(roleName);
  }

  @Test
  void testSelectTablePrivilegeCannotModifyTable() {
    // Test that SELECT privilege does not grant modification rights
    String tableName = "test_select_no_modify";
    createTable(SCHEMA_NAME, tableName);

    // Grant only SELECT privilege
    String roleName = grantSelectTableRole(tableName);

    // User should be able to read the table
    Assertions.assertDoesNotThrow(() -> sql("DESC TABLE %s", tableName));
    Assertions.assertDoesNotThrow(() -> sql("SELECT * FROM %s", tableName));

    // But should NOT be able to modify it
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('key'='value')", tableName));

    // Verify the property was not added
    Table table =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName));
    Assertions.assertFalse(table.properties().containsKey("key"));

    revokeRole(roleName);
  }

  private void grantUseSchemaRole(String schema) {
    String roleName = "useSchema_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);

    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  private String grantCreateTableRole(String schema) {
    String roleName = "createTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.CreateTable.allow()));
    securableObjects.add(schemaObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private String grantSelectTableRole(String tableName) {
    String roleName = "selectTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.SelectTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private String grantModifyTableRole(String tableName) {
    String roleName = "modifyTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.ModifyTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private void revokeRole(String roleName) {
    User user =
        metalakeClientWithAllPrivilege.revokeRolesFromUser(ImmutableList.of(roleName), NORMAL_USER);
    Assertions.assertFalse(user.roles().contains(roleName));
  }

  private void setTableOwner(String tableName) {
    MetadataObject tableMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName),
            MetadataObject.Type.TABLE);
    metalakeClientWithAllPrivilege.setOwner(tableMetadataObject, NORMAL_USER, Owner.Type.USER);
  }

  private void setSchemaOwner(String userName) {
    setSchemaOwner(SCHEMA_NAME, userName);
  }

  private void setSchemaOwner(String schemaName, String userName) {
    MetadataObject schemaMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, schemaName), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaMetadataObject, userName, Owner.Type.USER);
  }

  private void setCatalogOwner(String userName) {
    MetadataObject catalogMetadataObject =
        MetadataObjects.of(Arrays.asList(GRAVITINO_CATALOG_NAME), MetadataObject.Type.CATALOG);
    metalakeClientWithAllPrivilege.setOwner(catalogMetadataObject, userName, Owner.Type.USER);
  }

  private void setMetalakeOwner(String userName) {
    MetadataObject metalakeMetadataObject =
        MetadataObjects.of(Arrays.asList(METALAKE_NAME), MetadataObject.Type.METALAKE);
    metalakeClientWithAllPrivilege.setOwner(metalakeMetadataObject, userName, Owner.Type.USER);
  }

  private void clearTable() {
    clearTable(SCHEMA_NAME);
  }

  private void clearTable(String schemaName) {
    Arrays.stream(
            catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(schemaName)))
        .forEach(
            table -> {
              catalogClientWithAllPrivilege
                  .asTableCatalog()
                  .dropTable(NameIdentifier.of(schemaName, table.name()));
            });
    NameIdentifier[] nameIdentifiers =
        catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(schemaName));
    Assertions.assertEquals(0, nameIdentifiers.length);
  }
}
