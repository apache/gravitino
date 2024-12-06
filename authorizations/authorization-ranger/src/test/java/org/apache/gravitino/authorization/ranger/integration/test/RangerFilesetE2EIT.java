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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerFilesetE2EIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerFilesetE2EIT.class);

  private String RANGER_ADMIN_URL;
  private String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  private String defaultBaseLocation;
  private String metalakeName = GravitinoITUtils.genRandomName("metalake").toLowerCase();
  private String catalogName = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_schema");
  private static final String provider = "hadoop";
  private FileSystem fileSystem;
  private GravitinoMetalake metalake;
  private Catalog catalog;

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

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultBaseLocation());
    fileSystem = FileSystem.get(conf);

    createCatalogAndSchema();
  }

  @AfterAll
  public void stop() throws IOException {
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
    if (fileSystem != null) {
      fileSystem.close();
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
  void testReadWritePath() throws InterruptedException, IOException {
    String filename = GravitinoITUtils.genRandomName("RangerFilesetE2EIT_fileset");
    Fileset fileset = createFileset(filename, Fileset.Type.MANAGED, storageLocation(filename));
    Assertions.assertTrue(fileSystem.exists(new Path(storageLocation(filename))));

    Assertions.assertThrows(
        Exception.class, () -> fileSystem.listFiles(new Path(storageLocation(filename)), true));
    Assertions.assertThrows(
        Exception.class, () -> fileSystem.mkdirs(new Path(storageLocation(filename) + "/test")));

    String filesetRole = currentFunName();
    SecurableObject securableObject =
        SecurableObjects.parse(
            fileset.name(),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    String userName1 = System.getenv(HADOOP_USER_NAME);
    metalake.createRole(filesetRole, Collections.emptyMap(), Lists.newArrayList(securableObject));
    metalake.grantRolesToUser(Lists.newArrayList(filesetRole), userName1);
    waitForUpdatingPolicies();
    Assertions.assertNotNull(fileSystem.listFiles(new Path(storageLocation(filename)), true));
    Assertions.assertThrows(
        Exception.class, () -> fileSystem.mkdirs(new Path(storageLocation(filename) + "/test")));

    metalake.grantPrivilegesToRole(
        filesetRole,
        MetadataObjects.of(
            String.format("%s.%s", catalogName, schemaName),
            fileset.name(),
            MetadataObject.Type.FILESET),
        Lists.newArrayList(Privileges.WriteFileset.allow()));
    waitForUpdatingPolicies();
    Assertions.assertNotNull(fileSystem.listFiles(new Path(storageLocation(filename)), true));
    Assertions.assertTrue(fileSystem.mkdirs(new Path(storageLocation(filename) + "/test")));
  }

  private void createCatalogAndSchema() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    metalake = loadMetalake;
    metalake.createCatalog(
        catalogName,
        Catalog.Type.FILESET,
        provider,
        "comment",
        ImmutableMap.of(
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HDFS_REPO_NAME,
            AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
            RANGER_ADMIN_URL,
            RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RANGER_PASSWORD,
            RangerContainer.rangerPassword));

    catalog = metalake.loadCatalog(catalogName);
    catalog
        .asSchemas()
        .createSchema(schemaName, "comment", ImmutableMap.of("location", defaultBaseLocation()));
    Schema loadSchema = catalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertNotNull(loadSchema.properties().get("location"));
  }

  private String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      defaultBaseLocation =
          String.format(
              "hdfs://%s:%d/user/hadoop/%s",
              containerSuite.getHiveContainer().getContainerIpAddress(),
              HiveContainer.HDFS_DEFAULTFS_PORT,
              schemaName.toLowerCase());
    }
    return defaultBaseLocation;
  }

  private Fileset createFileset(String filesetName, Fileset.Type type, String storageLocation) {
    return catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(schemaName, filesetName), "comment", type, storageLocation, null);
  }

  private String storageLocation(String filesetName) {
    return defaultBaseLocation() + "/" + filesetName;
  }

  private void waitForUpdatingPolicies() throws InterruptedException {
    // After Ranger authorization, Must wait a period of time for the Ranger Spark plugin to update
    // the policy Sleep time must be greater than the policy update interval
    // (ranger.plugin.spark.policy.pollIntervalMs) in the
    // `resources/ranger-spark-security.xml.template`
    Thread.sleep(1000L);
  }
}
