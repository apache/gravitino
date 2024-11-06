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
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_AUTH_TYPE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_PASSWORD;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_SERVICE_NAME;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_USERNAME;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.kyuubi.plugin.spark.authz.AccessControlException;
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

    RangerITEnv.cleanup();
    metalake.addUser(System.getenv(HADOOP_USER_NAME));
  }

  @AfterAll
  public void stop() {
    cleanIT();
  }

  @Override
  protected void useCatalog() throws InterruptedException {
    // Do nothing, default catalog is ok for Hive.
  }

  @Override
  protected void checkHaveNoPrivileges() {
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
  protected void checkUpdateSQLWithReadWritePrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithReadPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithWritePrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithReadWritePrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithReadPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithWritePrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void testAlterTable() {
    sparkSession.sql(SQL_ALTER_TABLE);
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
