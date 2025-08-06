/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.integration.test.authorization;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class SparkAuthorizationIT extends BaseIT {

  protected static final String METALAKE = "metalake";

  protected static final String USER = "tester";

  protected static final String NORMAL_USER = "tester2";

  protected static final String HIVE_CATALOG = "hiveCatalog";

  protected static final String HIVE_DATABASE = "hiveDatabase";

  private String gravitinoUri;

  protected String hiveMetastoreUri = "thrift://127.0.0.1:9083";

  protected String warehouse;

  private SparkSession adminUserSparkSession;

  private SparkSession normalUserSparkSession;

  protected final String TIME_ZONE_UTC = "UTC";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.CACHE_ENABLED.getKey(),
            "false",
            Configs.AUTHENTICATORS.getKey(),
            "simple"));
    putServiceAdmin();
    initHiveCatalog();
    super.startIntegrationTest();
    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());
    initMetalakeAndCatalogs();
    initSparkEnv();
  }

  private void initHiveCatalog() {
    containerSuite.startHiveContainer();
    hiveMetastoreUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }

  private void initMetalakeAndCatalogs() {
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(NORMAL_USER);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hiveMetastoreUri);
    client
        .loadMetalake(METALAKE)
        .createCatalog(HIVE_CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties);
  }

  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), USER);
  }

  private void initSparkEnv() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, METALAKE)
            .set("hive.exec.dynamic.partition.mode", "nonstrict")
            .set("spark.sql.warehouse.dir", warehouse)
            .set("spark.sql.session.timeZone", TIME_ZONE_UTC);
    System.setProperty("HADOOP_USER_NAME", USER);
    adminUserSparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
    System.setProperty("HADOOP_USER_NAME", NORMAL_USER);
    normalUserSparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
  }

  @Test
  public void testCreateSchema() {
    assertThrows(
        String.format(
            "%s is not authorized to perform operation 'loadMetalake' on metadata 'metalake'",
            NORMAL_USER),
        RuntimeException.class,
        () -> {
          normalUserSparkSession.sql("create database " + HIVE_DATABASE);
        });
    adminUserSparkSession.sql("create database " + HIVE_DATABASE);
  }
}
