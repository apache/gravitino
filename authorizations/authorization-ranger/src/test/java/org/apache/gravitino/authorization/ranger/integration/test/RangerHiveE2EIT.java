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

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_AUTH_TYPE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_PASSWORD;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_SERVICE_NAME;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_USERNAME;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.TableChange;
import org.apache.kyuubi.plugin.spark.authz.AccessControlException;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHiveE2EIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveE2EIT.class);

  public static final String metalakeName =
      GravitinoITUtils.genRandomName("metalake").toLowerCase();
  public static final String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();
  public static final String schemaName = GravitinoITUtils.genRandomName("schema").toLowerCase();

  public static final String tableName = GravitinoITUtils.genRandomName("table").toLowerCase();

  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static final String provider = "hive";
  private static String HIVE_METASTORE_URIS;

  private static SparkSession sparkSession = null;
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  private static final String SQL_SHOW_DATABASES =
      String.format("SHOW DATABASES like '%s'", schemaName);

  private static final String SQL_CREATE_SCHEMA = String.format("CREATE DATABASE %s", schemaName);

  private static final String SQL_DROP_SCHEMA = String.format("DROP DATABASE %s", schemaName);

  private static final String SQL_USE_SCHEMA = String.format("USE SCHEMA %s", schemaName);

  private static final String SQL_CREATE_TABLE =
      String.format("CREATE TABLE %s (a int, b string, c string)", tableName);

  private static final String SQL_INSERT_TABLE =
      String.format("INSERT INTO %s (a, b, c) VALUES (1, 'a', 'b')", tableName);

  private static final String SQL_SELECT_TABLE = String.format("SELECT * FROM %s", tableName);

  private static final String SQL_UPDATE_TABLE =
      String.format("UPDATE %s SET b = 'b', c = 'c' WHERE a = 1", tableName);

  private static final String SQL_DELETE_TABLE =
      String.format("DELETE FROM %s WHERE a = 1", tableName);

  private static final String SQL_ALTER_TABLE =
      String.format("ALTER TABLE %s ADD COLUMN d string", tableName);

  private static final String SQL_RENAME_TABLE =
      String.format("ALTER TABLE %s RENAME TO new_table", tableName);

  private static final String SQL_RENAME_BACK_TABLE =
      String.format("ALTER TABLE new_table RENAME TO %s", tableName);

  private static final String SQL_DROP_TABLE = String.format("DROP TABLE %s", tableName);

  private static String RANGER_ADMIN_URL = null;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    RangerITEnv.init();
    RangerITEnv.startHiveRangerContainer();

    RANGER_ADMIN_URL =
        String.format(
            "http://%s:%d",
            containerSuite.getRangerContainer().getContainerIpAddress(), RANGER_SERVER_PORT);

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    generateRangerSparkSecurityXML();

    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Ranger Hive E2E integration test")
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "hdfs://%s:%d/user/hive/warehouse",
                    containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                    HiveContainer.HDFS_DEFAULTFS_PORT))
            .config("spark.sql.storeAssignmentPolicy", "LEGACY")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
            .config(
                "spark.sql.extensions",
                "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension")
            .enableHiveSupport()
            .getOrCreate();

    createMetalake();
    createCatalog();

    metalake.addUser("test");
  }

  private static void generateRangerSparkSecurityXML() throws IOException {
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

  @AfterAll
  public void stop() {
    if (client != null) {
      Arrays.stream(catalog.asSchemas().listSchemas())
          .filter(schema -> !schema.equals("default"))
          .forEach(
              (schema -> {
                catalog.asSchemas().dropSchema(schema, true);
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

  @Test
  void testCreateSchema() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  @Test
  void testCreateTable() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(createTableRole);
    metalake.deleteRole(createSchemaRole);
  }

  @Test
  void testReadWriteTableWithMetalakeLevelRole() throws InterruptedException {
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

    // case 3: Fail to update data in the table, Because Hive doesn't support.
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // case 4: Fail to delete data from the table, Because Hive doesn't support.
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // case 5: Succeed to alter the table
    sparkSession.sql(SQL_ALTER_TABLE);

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readWriteRole);
    waitForUpdatingPolicies();
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_USE_SCHEMA));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testReadWriteTableWithTableLevelRole() throws InterruptedException {
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

    // case 3: Fail to update data in the table, Because Hive doesn't support.
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // case 4: Fail to delete data from the table, Because Hive doesn't support.
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // case 5: Succeed to alter the table
    sparkSession.sql(SQL_ALTER_TABLE);

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(roleName);
    waitForUpdatingPolicies();
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_USE_SCHEMA));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testReadOnlyTable() throws InterruptedException {
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

    // case 3: Fail to alter data in the table
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // case 4: Fail to delete data from the table
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // case 5: Fail to alter the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_ALTER_TABLE));

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(readOnlyRole);
    waitForUpdatingPolicies();
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_USE_SCHEMA));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testWriteOnlyTable() throws InterruptedException {
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

    // case 3: Succeed to update data in the table
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // case 4: Succeed to delete data from the table
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // case 5: Succeed to alter the table
    sparkSession.sql(SQL_ALTER_TABLE);

    // case 6: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

    // case 7: If we don't have the role, we can't insert and select from data.
    metalake.deleteRole(writeOnlyRole);
    waitForUpdatingPolicies();
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_USE_SCHEMA));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));

    // Clean up
    catalog.asTableCatalog().dropTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testCreateAllPrivilegesRole() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDeleteAndRecreateRole() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);

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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDeleteAndRecreateMetadataObject() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
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
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testRenameMetadataObject() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  @Test
  void testRenameMetadataObjectPrivilege() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  @Test
  void testChangeOwner() throws InterruptedException {
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

    // case 1. Have none of privileges of the table

    // - a. Fail to insert data into the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));

    // - b. Fail to select data from the table
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());

    // - c: Fail to update data in the table
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // - d: Fail to delete data from the table
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // - e: Fail to alter the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_ALTER_TABLE));

    // - f: Fail to drop the table
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));

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
    catalog.asSchemas().dropSchema(schemaName, true);
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
    catalog.asSchemas().dropSchema(schemaName, true);
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
    catalog.asSchemas().dropSchema(schemaName, true);
  }

  @Test
  void testAllowUseSchemaPrivilege() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.revokeRolesFromUser(Lists.newArrayList(roleName), userName1);
    metalake.deleteRole(roleName);
  }

  @Test
  void testDenyPrivileges() throws InterruptedException {
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
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.deleteRole(roleName);
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> properties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HIVE_REPO_NAME,
            AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
            RANGER_ADMIN_URL,
            RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RANGER_PASSWORD,
            RangerContainer.rangerPassword);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  private void checkTableAllPrivilegesExceptForCreating() {
    // - a. Succeed to insert data into the table
    sparkSession.sql(SQL_INSERT_TABLE);

    // - b. Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // - c: Fail to update data in the table. Because Hive doesn't support
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // - d: Fail to delete data from the table, Because Hive doesn't support
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // - e: Succeed to alter the table
    sparkSession.sql(SQL_ALTER_TABLE);

    // - f: Succeed to drop the table
    sparkSession.sql(SQL_DROP_TABLE);
  }

  private static void waitForUpdatingPolicies() throws InterruptedException {
    // After Ranger authorization, Must wait a period of time for the Ranger Spark plugin to update
    // the policy Sleep time must be greater than the policy update interval
    // (ranger.plugin.spark.policy.pollIntervalMs) in the
    // `resources/ranger-spark-security.xml.template`
    Thread.sleep(1000L);
  }
}
