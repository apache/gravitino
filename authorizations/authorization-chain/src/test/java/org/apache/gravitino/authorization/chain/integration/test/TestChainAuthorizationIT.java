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
package org.apache.gravitino.authorization.chain.integration.test;

import static org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv.currentFunName;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.chain.ChainAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.integration.test.RangerBaseE2EIT;
import org.apache.gravitino.authorization.ranger.integration.test.RangerITEnv;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.kyuubi.plugin.spark.authz.AccessControlException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestChainAuthorizationIT extends RangerBaseE2EIT {
  private static final Logger LOG = LoggerFactory.getLogger(TestChainAuthorizationIT.class);

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    metalakeName = GravitinoITUtils.genRandomName("metalake").toLowerCase();
    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);

    super.startIntegrationTest();
    RangerITEnv.init(RangerBaseE2EIT.metalakeName, false);
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

    BaseIT.runInEnv(
        "HADOOP_USER_NAME",
        AuthConstants.ANONYMOUS_USER,
        () -> {
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
          sparkSession.sql(SQL_SHOW_DATABASES); // must be called to activate the Spark session
        });
    createMetalake();
    createCatalog();

    RangerITEnv.cleanup();
    try {
      metalake.addUser(System.getenv(HADOOP_USER_NAME));
    } catch (UserAlreadyExistsException e) {
      LOG.error("Failed to add user: {}", System.getenv(HADOOP_USER_NAME), e);
    }
  }

  @Test
  public void testCreateSchema() {
    // Choose a catalog
    useCatalog();

    // First, fail to create the schema
    org.apache.kyuubi.plugin.spark.authz.AccessControlException accessControlException =
        Assertions.assertThrows(
            org.apache.kyuubi.plugin.spark.authz.AccessControlException.class,
            () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertTrue(
        accessControlException
            .getMessage()
            .contains("Permission denied: user [anonymous] does not have [create] privilege"));

    // Second, grant the `CREATE_SCHEMA` role
    String roleName = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.ofCatalog(
            catalogName, Lists.newArrayList(Privileges.CreateSchema.allow()));
    //                    metalakeName, Lists.newArrayList(Privileges.CreateSchema.allow()));
    metalake.createRole(roleName, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(roleName), AuthConstants.ANONYMOUS_USER);
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
  public void init() {
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
  }

  @Override
  public void createCatalog() {
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(HiveConstants.METASTORE_URIS, HIVE_METASTORE_URIS);
    catalogConf.put(IMPERSONATION_ENABLE, "true");
    catalogConf.put(Catalog.AUTHORIZATION_PROVIDER, "chain");
    catalogConf.put(ChainAuthorizationProperties.CHAIN_PLUGINS_PROPERTIES_KEY, "hive1,hdfs1");
    catalogConf.put("authorization.chain.hive1.provider", "ranger");
    catalogConf.put("authorization.chain.hive1.ranger.auth.type", RangerContainer.authType);
    catalogConf.put("authorization.chain.hive1.ranger.admin.url", RANGER_ADMIN_URL);
    catalogConf.put("authorization.chain.hive1.ranger.username", RangerContainer.rangerUserName);
    catalogConf.put("authorization.chain.hive1.ranger.password", RangerContainer.rangerPassword);
    catalogConf.put("authorization.chain.hive1.ranger.service.type", "HadoopSQL");
    catalogConf.put(
        "authorization.chain.hive1.ranger.service.name", RangerITEnv.RANGER_HIVE_REPO_NAME);
    catalogConf.put("authorization.chain.hdfs1.provider", "ranger");
    catalogConf.put("authorization.chain.hdfs1.ranger.auth.type", RangerContainer.authType);
    catalogConf.put("authorization.chain.hdfs1.ranger.admin.url", RANGER_ADMIN_URL);
    catalogConf.put("authorization.chain.hdfs1.ranger.username", RangerContainer.rangerUserName);
    catalogConf.put("authorization.chain.hdfs1.ranger.password", RangerContainer.rangerPassword);
    catalogConf.put("authorization.chain.hdfs1.ranger.service.type", "HDFS");
    catalogConf.put(
        "authorization.chain.hdfs1.ranger.service.name", RangerITEnv.RANGER_HDFS_REPO_NAME);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, "hive", "comment", catalogConf);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
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
    catalog.asTableCatalog().purgeTable(NameIdentifier.of(schemaName, tableName));
    catalog.asSchemas().dropSchema(schemaName, false);
    metalake.deleteRole(createTableRole);
    metalake.deleteRole(createSchemaRole);
  }

  @Override
  protected void checkTableAllPrivilegesExceptForCreating() {}

  @Override
  protected void checkUpdateSQLWithReadWritePrivileges() {}

  @Override
  protected void checkUpdateSQLWithReadPrivileges() {}

  @Override
  protected void checkUpdateSQLWithWritePrivileges() {}

  @Override
  protected void checkDeleteSQLWithReadWritePrivileges() {}

  @Override
  protected void checkDeleteSQLWithReadPrivileges() {}

  @Override
  protected void checkDeleteSQLWithWritePrivileges() {}

  @Override
  protected void useCatalog() {}

  @Override
  protected void checkWithoutPrivileges() {}

  @Override
  protected void testAlterTable() {}
}
