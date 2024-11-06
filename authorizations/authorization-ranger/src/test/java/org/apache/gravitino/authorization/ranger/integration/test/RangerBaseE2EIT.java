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
package org.apache.gravitino.authorization.ranger.integration.test;

import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.TableChange;
import org.apache.kyuubi.plugin.spark.authz.AccessControlException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RangerBaseE2EIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerBaseE2EIT.class);
  public static final String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
  public static final String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();

  public static final String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

  public static String metalakeName;
  protected static GravitinoMetalake metalake;
  protected static Catalog catalog;
  protected static String HIVE_METASTORE_URIS;
  protected static String RANGER_ADMIN_URL = null;

  protected static SparkSession sparkSession = null;
  protected static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  protected static final String SQL_SHOW_DATABASES =
      String.format("SHOW DATABASES like '%s'", schemaName);

  protected static final String SQL_CREATE_SCHEMA = String.format("CREATE DATABASE %s", schemaName);

  protected static final String SQL_DROP_SCHEMA = String.format("DROP DATABASE %s", schemaName);

  protected static final String SQL_USE_SCHEMA = String.format("USE SCHEMA %s", schemaName);

  protected static final String SQL_CREATE_TABLE =
      String.format("CREATE TABLE %s (a int, b string, c string)", tableName);

  protected static final String SQL_INSERT_TABLE =
      String.format("INSERT INTO %s (a, b, c) VALUES (1, 'a', 'b')", tableName);

  protected static final String SQL_SELECT_TABLE = String.format("SELECT * FROM %s", tableName);

  protected static final String SQL_UPDATE_TABLE =
      String.format("UPDATE %s SET b = 'b', c = 'c' WHERE a = 1", tableName);

  protected static final String SQL_DELETE_TABLE =
      String.format("DELETE FROM %s WHERE a = 1", tableName);

  protected static final String SQL_ALTER_TABLE =
      String.format("ALTER TABLE %s ADD COLUMN d string", tableName);

  protected static final String SQL_ALTER_TABLE_BACK =
      String.format("ALTER TABLE %s DROP COLUMN d", tableName);
  protected static final String SQL_RENAME_TABLE =
      String.format("ALTER TABLE %s RENAME TO new_table", tableName);

  protected static final String SQL_RENAME_BACK_TABLE =
      String.format("ALTER TABLE new_table RENAME TO %s", tableName);

  protected static final String SQL_DROP_TABLE = String.format("DROP TABLE %s", tableName);

  protected static void generateRangerSparkSecurityXML() throws IOException {
    String templatePath =
        String.join(
            File.separator,
            System.getenv("GRAVITINO_ROOT_DIR"),
            "authorizations",
            "authorization-ranger",
            "src",
            "test",
            "resources",
            "ranger-spark-security.xml.template");
    String xmlPath =
        String.join(
            File.separator,
            System.getenv("GRAVITINO_ROOT_DIR"),
            "authorizations",
            "authorization-ranger",
            "build",
            "resources",
            "test",
            "ranger-spark-security.xml");

    String templateContext =
        FileUtils.readFileToString(new File(templatePath), StandardCharsets.UTF_8);
    templateContext =
        templateContext
            .replace("__REPLACE__RANGER_ADMIN_URL", RANGER_ADMIN_URL)
            .replace("__REPLACE__RANGER_HIVE_REPO_NAME", RangerITEnv.RANGER_HIVE_REPO_NAME);
    FileUtils.writeStringToFile(new File(xmlPath), templateContext, StandardCharsets.UTF_8);
  }

  protected void cleanIT() {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, false);
              }));
      Arrays.stream(metalake.listCatalogs())
          .forEach((catalogName -> metalake.dropCatalog(catalogName, true)));
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName);
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
    client = null;
    RangerITEnv.cleanup();
  }

  protected void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  protected static void waitForUpdatingPolicies() throws InterruptedException {
    // After Ranger authorization, Must wait a period of time for the Ranger Spark plugin to update
    // the policy Sleep time must be greater than the policy update interval
    // (ranger.plugin.spark.policy.pollIntervalMs) in the
    // `resources/ranger-spark-security.xml.template`
    Thread.sleep(1000L);
  }

  protected abstract void checkTableAllPrivilegesExceptForCreating();

  protected abstract void checkUpdateSQLWithReadWritePrivileges();

  protected abstract void checkUpdateSQLWithReadPrivileges();

  protected abstract void checkUpdateSQLWithWritePrivileges();

  protected abstract void checkDeleteSQLWithReadWritePrivileges();

  protected abstract void checkDeleteSQLWithReadPrivileges();

  protected abstract void checkDeleteSQLWithWritePrivileges();

  protected abstract void useCatalog() throws InterruptedException;

  protected abstract void checkHaveNoPrivileges();

  protected abstract void testAlterTable();

  @Test
  void testCreateSchema() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, fail to create the schema
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));

    // Second, grant the `CREATE_SCHEMA` role
    String userName1 = System.getenv(HADOOP_USER_NAME);
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Third, succeed to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Fourth, fail to create the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testCreateTable() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, create a role for creating a database and grant role to the user
    String createSchemaRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(Privileges.UseSchema.allow(), Privileges.CreateSchema.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(
        createSchemaRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(createSchemaRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, fail to create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Fourth, create a role for creating a table and grant to the user
    String createTableRole = currentFunName() + "2";
    securableObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateTable.allow()));
    metalake.createRole(
        createTableRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(createTableRole), userName1);
    waitForUpdatingPolicies();

    // Fifth, succeed to create a table
    sparkSession.sql(SQL_CREATE_TABLE);

    // Sixth, fail to read and write a table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(createTableRole);
    metalake.deleteRole(createSchemaRole);
  }

  @Test
  void testReadWriteTableWithMetalakeLevelRole() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, create a role for creating a database and grant role to the user
    String readWriteRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.SelectTable.allow(),
                Privileges.ModifyTable.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(readWriteRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(readWriteRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // case 1: Succeed to insert data into table
    sparkSession.sql(SQL_INSERT_TABLE);

    // case 2: Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // case 3: Update data in the table
    checkUpdateSQLWithReadWritePrivileges();

    // case 4:  Delete data from the table.
    checkDeleteSQLWithReadWritePrivileges();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readWriteRole);
    waitForUpdatingPolicies();
    checkHaveNoPrivileges();

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testReadWriteTableWithTableLevelRole() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, create a role for creating a database and grant role to the user
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // Fourth, revoke and grant a table level role
    metalake.deleteRole(roleName);
    securableObject =
        SecurableObjects.parse(
            String.format("%s.%s.%s", catalogName, schemaName, tableName),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow(), Privileges.SelectTable.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // case 1: Succeed to insert data into table
    sparkSession.sql(SQL_INSERT_TABLE);

    // case 2: Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // case 3: Update data in the table.
    checkUpdateSQLWithReadWritePrivileges();

    // case 4: Delete data from the table.
    checkDeleteSQLWithReadWritePrivileges();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();
    checkHaveNoPrivileges();

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testReadOnlyTable() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, create a role for creating a database and grant role to the user
    String readOnlyRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.SelectTable.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(readOnlyRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(readOnlyRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // case 1: Fail to insert data into table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));

    // case 2: Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // case 3: Update data in the table
    checkUpdateSQLWithReadPrivileges();

    // case 4: Delete data from the table
    checkDeleteSQLWithReadPrivileges();

    // case 5: Fail to alter the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_ALTER_TABLE));

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readOnlyRole);
    waitForUpdatingPolicies();
    checkHaveNoPrivileges();

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testWriteOnlyTable() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // First, create a role for creating a database and grant role to the user
    String writeOnlyRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.ModifyTable.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(writeOnlyRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(writeOnlyRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // case 1: Succeed to insert data into the table
    sparkSession.sql(SQL_INSERT_TABLE);

    // case 2: Fail to select data from the table
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());

    // case 3: Update data in the table
    checkUpdateSQLWithWritePrivileges();

    // case 4: Delete data from the table
    checkDeleteSQLWithWritePrivileges();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(writeOnlyRole);
    waitForUpdatingPolicies();
    checkHaveNoPrivileges();

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testCreateAllPrivilegesRole() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a role
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.CreateCatalog.allow(),
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateFileset.allow(),
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow(),
                Privileges.CreateTopic.allow(),
                Privileges.ConsumeTopic.allow(),
                Privileges.ProduceTopic.allow(),
                Privileges.CreateTable.allow(),
                Privileges.SelectTable.allow(),
                Privileges.ModifyTable.allow(),
                Privileges.ManageUsers.allow(),
                Privileges.ManageGroups.allow(),
                Privileges.CreateRole.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Test to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Test to create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDeleteAndRecreateRole() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Fail to create schema
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Succeed to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);
    catalog.asSchemas().dropSchema(schemaName, false);

    // Delete the role
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();

    // Fail to create the schema
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));

    // Create the role again
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Grant the role again
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Succeed to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDeleteAndRecreateMetadataObject() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));

    // Set owner
    MetadataObject schemaObject =
        MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.setOwner(schemaObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Delete a schema
    sparkSession.sql(SQL_DROP_SCHEMA);
    catalog.asSchemas().dropSchema(schemaName, false);
    waitForUpdatingPolicies();

    // Recreate a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));

    // Set owner
    schemaObject = MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.setOwner(schemaObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();
    sparkSession.sql(SQL_DROP_SCHEMA);

    // Delete the role and fail to create schema
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();

    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testRenameMetadataObject() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a role with CREATE_SCHEMA and CREATE_TABLE privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(
                Privileges.UseCatalog.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.ModifyTable.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Create a schema and a table
    sparkSession.sql(SQL_CREATE_SCHEMA);
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // Rename a table and rename back
    sparkSession.sql(SQL_RENAME_TABLE);
    sparkSession.sql(SQL_RENAME_BACK_TABLE);

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testRenameMetadataObjectPrivilege() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a role with CREATE_SCHEMA and CREATE_TABLE privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(
                Privileges.UseCatalog.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.ModifyTable.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Create a schema and a table
    sparkSession.sql(SQL_CREATE_SCHEMA);
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // Rename a table and rename back
    catalog
        .asTableCatalog()
        .alterTable(NameIdentifier.of(schemaName, tableName), TableChange.rename("new_table"));

    // Succeed to insert data
    sparkSession.sql("INSERT INTO new_table (a, b, c) VALUES (1, 'a', 'b')");

    catalog
        .asTableCatalog()
        .alterTable(NameIdentifier.of(schemaName, "new_table"), TableChange.rename(tableName));

    // Succeed to insert data
    sparkSession.sql(SQL_INSERT_TABLE);

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testChangeOwner() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a schema and a table
    String helperRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow(),
                Privileges.ModifyTable.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(helperRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(helperRole), userName1);
    waitForUpdatingPolicies();

    // Create a schema and a table
    sparkSession.sql(SQL_CREATE_SCHEMA);
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);
    sparkSession.sql(SQL_INSERT_TABLE);

    metalake.revokeRolesFromUser(Lists.newArrayList(helperRole), userName1);
    metalake.deleteRole(helperRole);
    waitForUpdatingPolicies();

    checkHaveNoPrivileges();

    // case 2. user is the  table owner
    MetadataObject tableObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);
    metalake.setOwner(tableObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Owner has all the privileges except for creating table
    checkTableAllPrivilegesExceptForCreating();

    // Delete Gravitino's meta data
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    waitForUpdatingPolicies();

    // Fail to create the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // case 3. user is the schema owner
    MetadataObject schemaObject =
        MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.setOwner(schemaObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Succeed to create a table
    sparkSession.sql(SQL_CREATE_TABLE);

    // Succeed to check other table privileges
    checkTableAllPrivilegesExceptForCreating();

    // Succeed to drop schema
    sparkSession.sql(SQL_DROP_SCHEMA);
    catalog.asSchemas().dropSchema(schemaName, false);
    waitForUpdatingPolicies();

    // Fail to create schema
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));

    // case 4. user is the catalog owner
    MetadataObject catalogObject =
        MetadataObjects.of(null, catalogName, MetadataObject.Type.CATALOG);
    metalake.setOwner(catalogObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Succeed to create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Succeed to create a table
    sparkSession.sql(SQL_CREATE_TABLE);

    // Succeed to check other table privileges
    checkTableAllPrivilegesExceptForCreating();

    // Succeed to drop schema
    sparkSession.sql(SQL_DROP_SCHEMA);
    catalog.asSchemas().dropSchema(schemaName, false);
    waitForUpdatingPolicies();

    metalake.setOwner(catalogObject, AuthConstants.ANONYMOUS_USER, Owner.Type.USER);
    // case 5. user is the metalake owner
    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeName, MetadataObject.Type.METALAKE);
    metalake.setOwner(metalakeObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Succeed to create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Succeed to create a table
    sparkSession.sql(SQL_CREATE_TABLE);

    // Succeed to check other table privileges
    checkTableAllPrivilegesExceptForCreating();

    // Succeed to drop schema
    sparkSession.sql(SQL_DROP_SCHEMA);

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testAllowUseSchemaPrivilege() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // create a schema use Gravitino client
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Revoke the privilege of creating schema
    MetadataObject catalogObject =
        MetadataObjects.of(null, catalogName, MetadataObject.Type.CATALOG);
    metalake.revokePrivilegesFromRole(
        roleName, catalogObject, Lists.newArrayList(Privileges.CreateSchema.allow()));
    waitForUpdatingPolicies();

    // Use Spark to show this databases(schema)
    Dataset dataset1 = sparkSession.sql(SQL_SHOW_DATABASES);
    dataset1.show();
    List<Row> rows1 = dataset1.collectAsList();
    // The schema should not be shown, because the user does not have the permission
    Assertions.assertEquals(
        0, rows1.stream().filter(row -> row.getString(0).equals(schemaName)).count());

    // Grant the privilege of using schema
    MetadataObject schemaObject =
        MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.grantPrivilegesToRole(
        roleName, schemaObject, Lists.newArrayList(Privileges.UseSchema.allow()));
    waitForUpdatingPolicies();

    // Use Spark to show this databases(schema) again
    Dataset dataset2 = sparkSession.sql(SQL_SHOW_DATABASES);
    dataset2.show(100, 100);
    List<Row> rows2 = dataset2.collectAsList();
    rows2.stream()
        .filter(row -> row.getString(0).equals(schemaName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Database not found: " + schemaName));
    // The schema should be shown, because the user has the permission
    Assertions.assertEquals(
        1, rows2.stream().filter(row -> row.getString(0).equals(schemaName)).count());

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.revokeRolesFromUser(Lists.newArrayList(roleName), userName1);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDenyPrivileges() throws InterruptedException {
    // Choose a catalog
    useCatalog();

    // Create a schema
    catalog.asSchemas().createSchema(schemaName, "test", Collections.emptyMap());

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject allowObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.UseSchema.allow(), Privileges.CreateTable.allow()));
    SecurableObject denyObject =
        SecurableObjects.parse(
            String.format("%s.%s", catalogName, schemaName),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.deny()));
    // Create a role, catalog allows to create a table, schema denies to create a table
    metalake.createRole(
        roleName, Collections.emptyMap(), Lists.newArrayList(allowObject, denyObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Fail to create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Delete the role
    metalake.deleteRole(roleName);

    // Create another role, but catalog denies to create a table, schema allows to create a table
    allowObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateTable.deny()));
    denyObject =
        SecurableObjects.parse(
            String.format("%s.%s", catalogName, schemaName),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    metalake.createRole(
        roleName, Collections.emptyMap(), Lists.newArrayList(allowObject, denyObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Fail to create a table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }
}
