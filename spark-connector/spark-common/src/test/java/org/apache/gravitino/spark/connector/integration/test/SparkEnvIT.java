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

package org.apache.gravitino.spark.connector.integration.test;

import static org.apache.gravitino.spark.connector.PropertiesConverter.SPARK_PROPERTY_PREFIX;
import static org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.spark.connector.integration.test.util.SparkUtilIT;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Setup Hive, Gravitino, Spark, Metalake environment to execute SparkSQL. */
public abstract class SparkEnvIT extends SparkUtilIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkEnvIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  protected static final String icebergRestServiceName = "iceberg-rest";
  protected String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected String warehouse;
  protected FileSystem hdfs;
  protected String icebergRestServiceUri;

  private final String metalakeName = "test";
  private SparkSession sparkSession;
  private String gravitinoUri = "http://127.0.0.1:8090";
  private final String lakeHouseIcebergProvider = "lakehouse-iceberg";

  protected abstract String getCatalogName();

  protected abstract String getProvider();

  protected abstract Map<String, String> getCatalogConfigs();

  @Override
  protected SparkSession getSparkSession() {
    Assertions.assertNotNull(sparkSession);
    return sparkSession;
  }

  @BeforeAll
  void startUp() throws Exception {
    initHiveEnv();
    // initialize the hiveMetastoreUri and warehouse at first to inject properties to
    // IcebergRestService
    if (lakeHouseIcebergProvider.equalsIgnoreCase(getProvider())) {
      initIcebergRestServiceEnv();
    }
    initCatalogEnv();
    // Start Gravitino server
    super.startIntegrationTest();
    initHdfsFileSystem();
    initGravitinoEnv();
    initMetalakeAndCatalogs();
    initSparkEnv();
    LOG.info(
        "Startup Spark env successfully, Gravitino uri: {}, Hive metastore uri: {}",
        gravitinoUri,
        hiveMetastoreUri);
  }

  @AfterAll
  void stop() throws IOException, InterruptedException {
    if (hdfs != null) {
      try {
        hdfs.close();
      } catch (IOException e) {
        LOG.warn("Close HDFS filesystem failed,", e);
      }
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
    super.stopIntegrationTest();
  }

  // AbstractIT#startIntegrationTest() is static, so we couldn't update the value of
  // ignoreIcebergRestService
  // if startIntegrationTest() is auto invoked by Junit. So here we override
  // startIntegrationTest() to disable the auto invoke by junit.
  @BeforeAll
  public void startIntegrationTest() {}

  @AfterAll
  public void stopIntegrationTest() {}

  private void initMetalakeAndCatalogs() {
    client.createMetalake(metalakeName, "", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    Map<String, String> properties = getCatalogConfigs();
    if (lakeHouseIcebergProvider.equalsIgnoreCase(getProvider())) {
      properties.put(SPARK_PROPERTY_PREFIX + ICEBERG_CATALOG_CACHE_ENABLED, "true");
    }
    metalake.createCatalog(
        getCatalogName(), Catalog.Type.RELATIONAL, getProvider(), "", properties);
  }

  private void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUrl
    int gravitinoPort = getGravitinoServerPort();
    gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);
    icebergRestServiceUri = getIcebergRestServiceUri();
  }

  private void initHiveEnv() {
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

  protected void initCatalogEnv() throws Exception {}

  private void initIcebergRestServiceEnv() {
    ignoreIcebergRestService = false;
    Map<String, String> icebergRestServiceConfigs = new HashMap<>();
    icebergRestServiceConfigs.put(
        "gravitino."
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    icebergRestServiceConfigs.put(
        "gravitino."
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
        hiveMetastoreUri);
    icebergRestServiceConfigs.put(
        "gravitino."
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
        warehouse);
    registerCustomConfigs(icebergRestServiceConfigs);
  }

  private void initHdfsFileSystem() {
    Configuration conf = new Configuration();
    conf.set(
        "fs.defaultFS",
        String.format(
            "hdfs://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT));
    try {
      hdfs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Create HDFS filesystem failed", e);
      throw new RuntimeException(e);
    }
  }

  private void initSparkEnv() {
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.plugins", GravitinoSparkPlugin.class.getName())
            .set(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, "true")
            .set(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, "true")
            .set("hive.exec.dynamic.partition.mode", "nonstrict")
            .set("spark.sql.warehouse.dir", warehouse)
            .set("spark.sql.session.timeZone", TIME_ZONE_UTC);
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
  }

  private String getIcebergRestServiceUri() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            serverConfig, String.format("gravitino.%s.", icebergRestServiceName));
    return String.format(
        "http://%s:%d/iceberg/", jettyServerConfig.getHost(), jettyServerConfig.getHttpPort());
  }
}
