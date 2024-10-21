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
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthenticatorType;
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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
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
      GravitinoITUtils.genRandomName("RangerHiveE2EIT_metalake").toLowerCase();
  public static final String catalogName =
      GravitinoITUtils.genRandomName("RangerHiveE2EIT_catalog").toLowerCase();
  public static final String schemaName =
      GravitinoITUtils.genRandomName("RangerHiveE2EIT_schema").toLowerCase();

  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static final String provider = "hive";
  private static String HIVE_METASTORE_URIS;

  private static SparkSession sparkSession = null;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  private static final String TEST_USER_NAME = "e2e_it_user";

  private static final String SQL_SHOW_DATABASES =
      String.format("SHOW DATABASES like '%s'", schemaName);

  private static String RANGER_ADMIN_URL = null;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", TEST_USER_NAME);
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
    RangerITEnv.cleanup();
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
  }

  @Test
  void testAllowUseSchemaPrivilege() throws InterruptedException {
    // First, create a schema use Gravitino client
    createSchema();

    // Use Spark to show this databases(schema)
    Dataset dataset1 = sparkSession.sql(SQL_SHOW_DATABASES);
    dataset1.show();
    List<Row> rows1 = dataset1.collectAsList();
    // The schema should not be shown, because the user does not have the permission
    Assertions.assertEquals(
        0, rows1.stream().filter(row -> row.getString(0).equals(schemaName)).count());

    // Create a role with CREATE_SCHEMA privilege
    SecurableObject securableObject1 =
        SecurableObjects.parse(
            String.format("%s", catalogName),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(currentFunName())
            .withAuditInfo(auditInfo)
            .withSecurableObjects(Lists.newArrayList(securableObject1))
            .build();
    RangerITEnv.rangerAuthHivePlugin.onRoleCreated(role);

    // Granted this role to the spark execution user `HADOOP_USER_NAME`
    String userName1 = System.getenv(HADOOP_USER_NAME);
    UserEntity userEntity1 =
        UserEntity.builder()
            .withId(1L)
            .withName(userName1)
            .withRoleNames(Collections.emptyList())
            .withRoleIds(Collections.emptyList())
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertTrue(
        RangerITEnv.rangerAuthHivePlugin.onGrantedRolesToUser(
            Lists.newArrayList(role), userEntity1));

    // After Ranger Authorization, Must wait a period of time for the Ranger Spark plugin to update
    // the policy Sleep time must be greater than the policy update interval
    // (ranger.plugin.spark.policy.pollIntervalMs) in the
    // `resources/ranger-spark-security.xml.template`
    Thread.sleep(1000L);

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

  private static void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse/%s.db",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT,
            schemaName.toLowerCase()));
    String comment = "comment";

    catalog.asSchemas().createSchema(schemaName, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName.toLowerCase(), loadSchema.name());
  }
}
