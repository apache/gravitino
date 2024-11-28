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
import static org.apache.gravitino.authorization.ranger.integration.test.RangerBaseE2EIT.generateRangerSparkSecurityXML;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_AUTH_TYPE;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_PASSWORD;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_SERVICE_NAME;
import static org.apache.gravitino.connector.AuthorizationPropertiesMeta.RANGER_USERNAME;
import static org.apache.gravitino.integration.test.container.RangerContainer.RANGER_SERVER_PORT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHudiE2EIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(RangerHudiE2EIT.class);
  private static final String provider = "lakehouse-hudi";

  public static final String catalogName = GravitinoITUtils.genRandomName("catalog").toLowerCase();

  public static String metalakeName;
  protected static GravitinoMetalake metalake;
  protected static Catalog catalog;
  protected static String HIVE_METASTORE_URIS;
  protected static String RANGER_ADMIN_URL = null;

  protected static SparkSession sparkSession = null;
  protected static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

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
            .appName("Hudi Catalog integration test")
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "hdfs://%s:%d/user/hive/warehouse-catalog-hudi",
                    containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                    HiveContainer.HDFS_DEFAULTFS_PORT))
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config("dfs.replication", "1")
            .enableHiveSupport()
            .getOrCreate();

    createMetalake();
    createCatalog();

    metalake.addUser(System.getenv(HADOOP_USER_NAME));
  }

  @AfterAll
  public void stop() {
    //
  }

  void createMetalake() {
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
            "uri",
            HIVE_METASTORE_URIS,
            "catalog-backend",
            "hms",
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
}
