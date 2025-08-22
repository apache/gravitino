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
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.kyuubi.plugin.spark.authz.AccessControlException;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHiveE2EIT extends RangerBaseE2EIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveE2EIT.class);

  private static final String provider = "hive";

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

    RangerITEnv.init(RangerBaseE2EIT.metalakeName, true);
    RangerITEnv.startHiveRangerContainer();

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    generateRangerSparkSecurityXML("authorization-ranger");

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

    RangerITEnv.cleanup();
    try {
      metalake.addUser(System.getenv(HADOOP_USER_NAME));
    } catch (UserAlreadyExistsException e) {
      LOG.error("Failed to add user: {}", System.getenv(HADOOP_USER_NAME), e);
    }
  }

  @AfterAll
  public void stop() {
    cleanIT();
  }

  @Override
  protected void reset() {
    // Do nothing, default catalog is ok for Hive.
  }

  @Override
  protected String testUserName() {
    return System.getenv(HADOOP_USER_NAME);
  }

  @Override
  protected void checkWithoutPrivileges() {
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_INSERT_TABLE));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_SELECT_TABLE).collectAsList());
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_TABLE));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_USE_SCHEMA));
    Assertions.assertThrows(
        AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
    Assertions.assertThrows(AccessControlException.class, () -> sparkSession.sql(SQL_CREATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithSelectModifyPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithSelectPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithModifyPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithSelectModifyPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithSelectPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithModifyPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void testAlterTable() {
    sparkSession.sql(SQL_ALTER_TABLE);
  }

  @Override
  public void createCatalog() {
    Map<String, String> properties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HadoopSQL",
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HIVE_REPO_NAME,
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);

    // Test to create catalog automatically
    Map<String, String> uuidProperties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HadoopSQL",
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword,
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            "test555",
            RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT,
            "true");

    try {
      List<RangerService> serviceList = RangerITEnv.rangerClient.findServices(Maps.newHashMap());
      int expectServiceCount = serviceList.size() + 1;
      Catalog catalogTest =
          metalake.createCatalog(
              "test", Catalog.Type.RELATIONAL, provider, "comment", uuidProperties);
      Map<String, String> newProperties = catalogTest.properties();
      Assertions.assertTrue(newProperties.containsKey("authorization.ranger.service.name"));
      serviceList = RangerITEnv.rangerClient.findServices(Maps.newHashMap());
      Assertions.assertEquals(expectServiceCount, serviceList.size());
      metalake.dropCatalog("test", true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Map<String, String> rollbackFailProperties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HadoopSQL",
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            "invalid_password",
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            "test_rollback_fail",
            RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT,
            "true");
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            metalake.createCatalog(
                "testRollbackFail",
                Catalog.Type.RELATIONAL,
                provider,
                "comment",
                rollbackFailProperties));

    // Test to create a catalog with wrong properties which lacks Ranger service name,
    // It will throw RuntimeException with message that Ranger service name is required.
    Map<String, String> wrongProperties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HadoopSQL",
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword,
            RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT,
            "true");

    int catalogSize = metalake.listCatalogs().length;
    Exception exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                metalake.createCatalog(
                    "wrongTestProperties",
                    Catalog.Type.RELATIONAL,
                    provider,
                    "comment",
                    wrongProperties));
    Assertions.assertTrue(
        exception.getMessage().contains("authorization.ranger.service.name is required"));

    Assertions.assertEquals(catalogSize, metalake.listCatalogs().length);
  }

  protected void checkTableAllPrivilegesExceptForCreating() {
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
}
