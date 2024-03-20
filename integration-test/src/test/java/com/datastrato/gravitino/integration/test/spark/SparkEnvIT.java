/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.spark.SparkUtilIT;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
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

  private final String metalakeName = "test";

  private SparkSession sparkSession;
  private String hiveMetastoreUri;
  private String gravitinoUri;
  private String warehouse;

  protected abstract String getCatalogName();

  protected abstract String getProvider();

  @Override
  protected SparkSession getSparkSession() {
    Assertions.assertNotNull(sparkSession);
    return sparkSession;
  }

  @BeforeAll
  void startUp() {
    initHiveEnv();
    initGravitinoEnv();
    initMetalakeAndCatalogs();
    initSparkEnv();
    LOG.info(
        "Startup Spark env successfully, gravitino uri: {}, hive metastore uri: {}",
        gravitinoUri,
        hiveMetastoreUri);
  }

  @AfterAll
  void stop() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private void initMetalakeAndCatalogs() {
    client.createMetalake(NameIdentifier.of(metalakeName), "", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Map<String, String> properties = Maps.newHashMap();
    properties.put(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI, hiveMetastoreUri);
    properties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_BACKEND, "hive");
    properties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_WAREHOUSE, warehouse);
    properties.put(GravitinoSparkConfig.LAKEHOUSE_ICEBERG_CATALOG_URI, hiveMetastoreUri);

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

  private void initSparkEnv() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark connector integration test")
            .config("spark.plugins", GravitinoSparkPlugin.class.getName())
            .config(GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri)
            .config(GravitinoSparkConfig.GRAVITINO_METALAKE, metalakeName)
            .config("spark.sql.warehouse.dir", warehouse)
            .enableHiveSupport()
            .getOrCreate();
  }
}
