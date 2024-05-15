/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkUtilIT;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
  protected String icebergRestServiceUri = "http://%s:%d/iceberg/";
  protected String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected String warehouse;
  protected FileSystem hdfs;

  private final String metalakeName = "test";
  private SparkSession sparkSession;
  private String gravitinoUri = "http://127.0.0.1:8090";

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
    if ("lakehouse-iceberg".equalsIgnoreCase(getProvider())) {
      initIcebergRestServiceEnv();
    }
    // Start Gravitino server
    AbstractIT.startIntegrationTest();
    initHdfsFileSystem();
    initGravitinoEnv();
    initMetalakeAndCatalogs();
    initSparkEnv();
    LOG.info(
        "Startup Spark env successfully, gravitino uri: {}, hive metastore uri: {}",
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
    AbstractIT.stopIntegrationTest();
  }

  // AbstractIT#startIntegrationTest() is static, so we couldn't update the value of
  // ignoreIcebergRestService
  // if startIntegrationTest() is auto invoked by Junit. So here we override
  // startIntegrationTest() to disable the auto invoke by junit.
  @BeforeAll
  public static void startIntegrationTest() {}

  @AfterAll
  public static void stopIntegrationTest() {}

  private void initMetalakeAndCatalogs() {
    client.createMetalake(NameIdentifier.of(metalakeName), "", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Map<String, String> properties = getCatalogConfigs();
    metalake.createCatalog(
        NameIdentifier.of(metalakeName, getCatalogName()),
        Catalog.Type.RELATIONAL,
        getProvider(),
        "",
        properties);
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

  private void initIcebergRestServiceEnv() {
    ignoreIcebergRestService = false;
    Map<String, String> icebergRestServiceConfigs = new HashMap<>();
    icebergRestServiceConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    icebergRestServiceConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
        hiveMetastoreUri);
    icebergRestServiceConfigs.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + icebergRestServiceName
            + "."
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
        warehouse);
    AbstractIT.registerCustomConfigs(icebergRestServiceConfigs);
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
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config("spark.plugins", GravitinoSparkPlugin.class.getName())
            .config(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .config(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.warehouse.dir", warehouse)
            .config("spark.sql.session.timeZone", TIME_ZONE_UTC)
            .enableHiveSupport()
            .getOrCreate();
  }

  private String getIcebergRestServiceUri() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            serverConfig,
            AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX + icebergRestServiceName + ".");
    return String.format(
        icebergRestServiceUri, jettyServerConfig.getHost(), jettyServerConfig.getHttpPort());
  }
}
