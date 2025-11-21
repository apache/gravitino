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
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("DESC TABLE %s_not_exist", tableName));

    setSchemaOwner(SUPER_USER);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("DESC TABLE %s", tableName));
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
  public void testRenameTable() {
    String tableName = "test_rename";
    createTable(SCHEMA_NAME, tableName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("ALTER TABLE %s RENAME TO %s", tableName, tableName + "_renamed"));

    setTableOwner(tableName);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER TABLE %s RENAME TO %s", tableName, tableName + "_renamed"));
    Table table =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA_NAME, tableName + "_renamed"));
    Assertions.assertNotNull(table);

    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, tableName + "_renamed"),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());
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
            catalogObject,
            schema,
            ImmutableList.of(Privileges.CreateTable.allow(), Privileges.SelectTable.allow()));
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
    MetadataObject schemaMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaMetadataObject, userName, Owner.Type.USER);
  }

  private void clearTable() {
    Arrays.stream(
            catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(SCHEMA_NAME)))
        .forEach(
            table -> {
              catalogClientWithAllPrivilege
                  .asTableCatalog()
                  .dropTable(NameIdentifier.of(SCHEMA_NAME, table.name()));
            });
    NameIdentifier[] nameIdentifiers =
        catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(SCHEMA_NAME));
    Assertions.assertEquals(0, nameIdentifiers.length);
  }
}
