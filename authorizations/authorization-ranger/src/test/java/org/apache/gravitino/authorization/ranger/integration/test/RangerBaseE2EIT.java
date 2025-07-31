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
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.MetalakeChange;
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

  protected static void generateRangerSparkSecurityXML(String modeName) throws IOException {
    String templatePath =
        String.join(
            File.separator,
            System.getenv("GRAVITINO_ROOT_DIR"),
            "authorizations",
            modeName,
            "src",
            "test",
            "resources",
            "ranger-spark-security.xml.template");
    String xmlPath =
        String.join(
            File.separator,
            System.getenv("GRAVITINO_ROOT_DIR"),
            "authorizations",
            modeName,
            "build",
            "resources",
            "test",
            "ranger-spark-security.xml");

    String templateContext =
        FileUtils.readFileToString(new File(templatePath), StandardCharsets.UTF_8);
    templateContext =
        templateContext
            .replace("__REPLACE__RANGER_ADMIN_URL", RangerITEnv.RANGER_ADMIN_URL)
            .replace("__REPLACE__RANGER_HIVE_REPO_NAME", RangerITEnv.RANGER_HIVE_REPO_NAME);
    FileUtils.writeStringToFile(new File(xmlPath), templateContext, StandardCharsets.UTF_8);
  }

  @Override
  public void startIntegrationTest() throws Exception {
    customConfigs.put(
        Configs.AUTHORIZATION_IMPL.getKey(),
        "org.apache.gravitino.server.authorization.PassThroughAuthorizer");
    super.startIntegrationTest();
  }

  protected void cleanIT() {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach((schema -> catalog.asSchemas().dropSchema(schema, false)));

      // The `dropCatalog` call will invoke the catalog metadata object to remove privileges
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

  protected static void waitForUpdatingPolicies() {
    // After Ranger authorization, Must wait a period of time for the Ranger Spark plugin to update
    // the policy Sleep time must be greater than the policy update interval
    // (ranger.plugin.spark.policy.pollIntervalMs) in the
    // `resources/ranger-spark-security.xml.template`
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      LOG.error("Failed to sleep", e);
    }
  }

  protected abstract void createCatalog();

  protected abstract String testUserName();

  protected abstract void checkTableAllPrivilegesExceptForCreating();

  protected abstract void checkUpdateSQLWithSelectModifyPrivileges();

  protected abstract void checkUpdateSQLWithSelectPrivileges();

  protected abstract void checkUpdateSQLWithModifyPrivileges();

  protected abstract void checkDeleteSQLWithSelectModifyPrivileges();

  protected abstract void checkDeleteSQLWithSelectPrivileges();

  protected abstract void checkDeleteSQLWithModifyPrivileges();

  protected abstract void reset();

  protected abstract void checkWithoutPrivileges();

  protected abstract void testAlterTable();

  protected void testDropTable() {
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
  }

  // ISSUE-5947: can't rename a catalog or a metalake
  @Test
  protected void testRenameMetalakeOrCatalog() {
    Assertions.assertDoesNotThrow(
        () -> client.alterMetalake(metalakeName, MetalakeChange.rename("new_name")));
    Assertions.assertDoesNotThrow(
        () -> client.alterMetalake("new_name", MetalakeChange.rename(metalakeName)));

    Assertions.assertDoesNotThrow(
        () -> metalake.alterCatalog(catalogName, CatalogChange.rename("new_name")));
    Assertions.assertDoesNotThrow(
        () -> metalake.alterCatalog("new_name", CatalogChange.rename(catalogName)));
  }

  @Test
  protected void testCreateSchema() throws InterruptedException {
    // Choose a catalog
    reset();

    TestCreateSchemaChecker createSchemaChecker = getTestCreateSchemaChecker();

    // First, fail to create the schema
    createSchemaChecker.checkCreateSchemaWithoutPriv();

    // Second, grant the `CREATE_SCHEMA` role
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(roleName), testUserName());
    waitForUpdatingPolicies();

    // Third, succeed to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Fourth, test to create the table
    createSchemaChecker.checkCreateTable();

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  protected TestCreateSchemaChecker getTestCreateSchemaChecker() {
    return new TestCreateSchemaChecker();
  }

  protected static class TestCreateSchemaChecker {
    void checkCreateSchemaWithoutPriv() {
      Assertions.assertThrows(Exception.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    }

    void checkCreateTable() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));
    }
  }

  @Test
  void testCreateTable() {
    // Choose a catalog
    reset();

    TestCreateTableChecker checker = getTestCreateTableChecker();

    // First, create a role for creating a database and grant role to the user
    String createSchemaRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(Privileges.UseSchema.allow(), Privileges.CreateSchema.allow()));
    String userName1 = testUserName();
    metalake.createRole(
        createSchemaRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(createSchemaRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, fail to create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    checker.checkCreateTable();

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
    checker.checkTableReadWrite();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(createTableRole);
    metalake.deleteRole(createSchemaRole);
  }

  protected TestCreateTableChecker getTestCreateTableChecker() {
    return new TestCreateTableChecker();
  }

  protected static class TestCreateTableChecker {
    void checkTableReadWrite() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    }

    void checkCreateTable() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));
    }
  }

  @Test
  void testSelectModifyTableWithMetalakeLevelRole() {
    // Choose a catalog
    reset();

    TestSelectModifyTableChecker checker = getTestSelectModifyTableChecker();

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
    String userName1 = testUserName();
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
    checker.checkUpdateSQL();

    // case 4:  Delete data from the table.
    checker.checkDeleteSQL();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    testDropTable();

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readWriteRole);
    waitForUpdatingPolicies();
    checker.checkNoPrivSQL();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    waitForUpdatingPolicies();
  }

  TestSelectModifyTableChecker getTestSelectModifyTableChecker() {
    return new TestSelectModifyTableChecker() {
      @Override
      void checkUpdateSQL() {
        checkUpdateSQLWithSelectModifyPrivileges();
      }

      @Override
      void checkDeleteSQL() {
        checkDeleteSQLWithSelectModifyPrivileges();
      }

      @Override
      void checkNoPrivSQL() {
        checkWithoutPrivileges();
      }
    };
  }

  protected abstract static class TestSelectModifyTableChecker {
    abstract void checkUpdateSQL();

    abstract void checkDeleteSQL();

    abstract void checkNoPrivSQL();
  }

  @Test
  void testSelectModifyTableWithTableLevelRole() {
    // Choose a catalog
    reset();

    TestSelectModifyTableChecker checker = getTestSelectModifyTableChecker();

    // First, create a role for creating a database and grant role to the user
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofMetalake(
            metalakeName,
            Lists.newArrayList(
                Privileges.UseSchema.allow(),
                Privileges.CreateSchema.allow(),
                Privileges.CreateTable.allow()));
    String userName1 = testUserName();
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
    checker.checkUpdateSQL();

    // case 4: Delete data from the table.
    checker.checkDeleteSQL();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    testDropTable();

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();
    checker.checkNoPrivSQL();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  @Test
  void testSelectOnlyTable() {
    // Choose a catalog
    reset();

    TestSelectOnlyTableChecker checker = getTestSelectOnlyTableChecker();

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
    String userName1 = testUserName();
    metalake.createRole(readOnlyRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(readOnlyRole), userName1);
    waitForUpdatingPolicies();
    // Second, create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Third, create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // case 1: Fail to insert data into table
    checker.checkInsertSQL();

    // case 2: Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // case 3: Update data in the table
    checker.checkUpdateSQL();

    // case 4: Delete data from the table
    checker.checkDeleteSQL();

    // case 5: Test to alter the table
    checker.checkAlterSQL();

    // case 6: Test to drop the table
    checker.checkDropSQL();

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readOnlyRole);
    waitForUpdatingPolicies();
    checker.checkNoPrivSQL();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  TestSelectOnlyTableChecker getTestSelectOnlyTableChecker() {
    return new TestSelectOnlyTableChecker() {
      @Override
      void checkUpdateSQL() {
        checkUpdateSQLWithSelectPrivileges();
      }

      @Override
      void checkDeleteSQL() {
        checkDeleteSQLWithSelectPrivileges();
      }

      @Override
      void checkNoPrivSQL() {
        checkWithoutPrivileges();
      }

      @Override
      void checkInsertSQL() {
        Assertions.assertThrows(
            AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
      }

      @Override
      void checkAlterSQL() {
        Assertions.assertThrows(
            AccessControlException.class, () -> sparkSession.sql(SQL_ALTER_TABLE));
      }

      @Override
      void checkDropSQL() {
        Assertions.assertThrows(
            AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
      }
    };
  }

  protected abstract static class TestSelectOnlyTableChecker {
    abstract void checkUpdateSQL();

    abstract void checkDeleteSQL();

    abstract void checkNoPrivSQL();

    abstract void checkInsertSQL();

    abstract void checkDropSQL();

    abstract void checkAlterSQL();
  }

  @Test
  void testModifyOnlyTable() {
    // Choose a catalog
    reset();

    TestModifyOnlyTableChecker checker = getTestModifyOnlyTableChecker();

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
    String userName1 = testUserName();
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

    // case 2: Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // case 3: Update data in the table
    checker.checkUpdateSQL();

    // case 4: Delete data from the table
    checker.checkDeleteSQL();

    // case 5: Succeed to alter the table
    testAlterTable();

    // case 6: Fail to drop the table
    testDropTable();

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(writeOnlyRole);
    waitForUpdatingPolicies();
    checker.checkNoPrivSQL();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  TestModifyOnlyTableChecker getTestModifyOnlyTableChecker() {
    return new TestModifyOnlyTableChecker() {
      @Override
      void checkUpdateSQL() {
        checkUpdateSQLWithModifyPrivileges();
      }

      @Override
      void checkDeleteSQL() {
        checkDeleteSQLWithModifyPrivileges();
      }

      @Override
      void checkNoPrivSQL() {
        checkWithoutPrivileges();
      }
    };
  }

  protected abstract static class TestModifyOnlyTableChecker {
    abstract void checkUpdateSQL();

    abstract void checkDeleteSQL();

    abstract void checkNoPrivSQL();
  }

  @Test
  void testCreateAllPrivilegesRole() {
    // Choose a catalog
    reset();

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
                Privileges.CreateRole.allow(),
                Privileges.ManageGrants.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Test to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Test to create a table
    sparkSession.sql(SQL_USE_SCHEMA);
    sparkSession.sql(SQL_CREATE_TABLE);

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDeleteAndRecreateRole() {
    // Choose a catalog
    reset();

    TestDeleteAndRecreateRoleChecker checker = getTestDeleteAndRecreateRoleChecker();
    // Fail to create schema
    checker.checkDropSchema();

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.UseCatalog.allow(), Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Succeed to create the schema
    sparkSession.sql(SQL_CREATE_SCHEMA);
    catalog.asSchemas().dropSchema(schemaName, false);
    reset();

    // Delete the role
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();

    // Fail to create the schema
    checker.checkCreateSchema();

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

  protected TestDeleteAndRecreateRoleChecker getTestDeleteAndRecreateRoleChecker() {
    return new TestDeleteAndRecreateRoleChecker();
  }

  protected static class TestDeleteAndRecreateRoleChecker {
    void checkDropSchema() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    }

    void checkCreateSchema() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    }
  }

  @Test
  void testDeleteAndRecreateMetadataObject() {
    // Choose a catalog
    reset();

    TestDeleteAndRecreateMetadataObject checker = getTestDeleteAndRecreateMetadataObject();

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // Create a schema
    sparkSession.sql(SQL_CREATE_SCHEMA);

    checker.checkDropSchema();

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

    // Set owner
    schemaObject = MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.setOwner(schemaObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();
    sparkSession.sql(SQL_DROP_SCHEMA);

    // Delete the role and fail to create schema
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();

    checker.checkRecreateSchema();

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  TestDeleteAndRecreateMetadataObject getTestDeleteAndRecreateMetadataObject() {
    return new TestDeleteAndRecreateMetadataObject();
  }

  protected static class TestDeleteAndRecreateMetadataObject {
    void checkDropSchema() {
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    }

    void checkRecreateSchema() {
      sparkSession.sql(SQL_CREATE_SCHEMA);
    }
  }

  @Test
  void testRenameMetadataObject() {
    // Choose a catalog
    reset();

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
    String userName1 = testUserName();
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
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testRenameMetadataObjectPrivilege() {
    // Choose a catalog
    reset();

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
    String userName1 = testUserName();
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
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  @Test
  void testChangeOwner() {
    // Choose a catalog
    reset();

    TestChangeOwnerChecker checker = getTestChangeOwnerChecker();

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
    String userName1 = testUserName();
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

    checkWithoutPrivileges();

    // case 2. user is the  table owner
    MetadataObject tableObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);
    metalake.setOwner(tableObject, userName1, Owner.Type.USER);
    waitForUpdatingPolicies();

    // Owner has all the privileges except for creating table
    checkTableAllPrivilegesExceptForCreating();

    // Delete Gravitino's meta data
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    waitForUpdatingPolicies();

    checker.checkCreateTable();

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

    checker.checkCreateSchema();

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
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
  }

  TestChangeOwnerChecker getTestChangeOwnerChecker() {
    return new TestChangeOwnerChecker();
  }

  protected static class TestChangeOwnerChecker {
    void checkCreateTable() {
      // Fail to create the table
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));
    }

    void checkCreateSchema() {
      // Fail to create schema
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    }
  }

  @Test
  protected void testAllowUseSchemaPrivilege() throws InterruptedException {
    // Choose a catalog
    reset();

    TestAllowUseSchemaPrivilegeChecker checker = getTestAllowUseSchemaPrivilegeChecker();

    // Create a role with CREATE_SCHEMA privilege
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);
    waitForUpdatingPolicies();

    // create a schema use Gravitino client
    sparkSession.sql(SQL_CREATE_SCHEMA);

    // Revoke the privilege of creating schema
    MetadataObject catalogObject =
        MetadataObjects.of(null, catalogName, MetadataObject.Type.CATALOG);
    metalake.revokePrivilegesFromRole(
        roleName, catalogObject, Sets.newHashSet(Privileges.CreateSchema.allow()));
    waitForUpdatingPolicies();

    checker.checkShowSchemas();

    // Grant the privilege of using schema
    MetadataObject schemaObject =
        MetadataObjects.of(catalogName, schemaName, MetadataObject.Type.SCHEMA);
    metalake.grantPrivilegesToRole(
        roleName, schemaObject, Sets.newHashSet(Privileges.UseSchema.allow()));
    waitForUpdatingPolicies();

    checker.checkShowSchemasAgain();

    // Clean up
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.revokeRolesFromUser(Lists.newArrayList(roleName), userName1);
    metalake.deleteRole(roleName);
  }

  protected TestAllowUseSchemaPrivilegeChecker getTestAllowUseSchemaPrivilegeChecker() {
    return new TestAllowUseSchemaPrivilegeChecker();
  }

  protected static class TestAllowUseSchemaPrivilegeChecker {

    void checkShowSchemas() {
      // Use Spark to show this databases(schema)
      Dataset dataset1 = sparkSession.sql(SQL_SHOW_DATABASES);
      dataset1.show();
      List<Row> rows1 = dataset1.collectAsList();
      // The schema should not be shown, because the user does not have the permission
      Assertions.assertEquals(
          0, rows1.stream().filter(row -> row.getString(0).equals(schemaName)).count());
    }

    void checkShowSchemasAgain() {
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
    }
  }

  void testDenyPrivileges() {
    // Choose a catalog
    reset();

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
    String userName1 = testUserName();
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
    userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    // Fail to create a table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  // ISSUE-5892 Fix to grant privilege for the metalake
  @Test
  void testGrantPrivilegesForMetalake() {
    // Choose a catalog
    reset();

    TestGrantPrivilegesForMetalakeChecker checker = getTestGrantPrivilegesForMetalakeChecker();

    // Create a schema
    String roleName = currentFunName();
    metalake.createRole(roleName, Collections.emptyMap(), Collections.emptyList());

    // Grant a create schema privilege
    metalake.grantPrivilegesToRole(
        roleName,
        MetadataObjects.of(null, metalakeName, MetadataObject.Type.METALAKE),
        Sets.newHashSet(Privileges.CreateSchema.allow()));
    checker.checkCreateSchema();

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = testUserName();
    metalake.grantRolesToUser(Lists.newArrayList(roleName), userName1);

    waitForUpdatingPolicies();

    Assertions.assertDoesNotThrow(() -> sparkSession.sql(SQL_CREATE_SCHEMA));

    // Clean up
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(roleName);
  }

  TestGrantPrivilegesForMetalakeChecker getTestGrantPrivilegesForMetalakeChecker() {
    return new TestGrantPrivilegesForMetalakeChecker();
  }

  protected static class TestGrantPrivilegesForMetalakeChecker {
    void checkCreateSchema() {
      // Fail to create a schema
      Assertions.assertThrows(
          AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    }
  }
}
