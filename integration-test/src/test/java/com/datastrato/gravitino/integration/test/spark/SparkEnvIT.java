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
import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.datastrato.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
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

  protected FileSystem hdfs;
  private final String metalakeName = "test";

  private SparkSession sparkSession;
  private String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  private String gravitinoUri = "http://127.0.0.1:8090";
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
  void stop() {
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
  }

  private void initMetalakeAndCatalogs() {
    client.createMetalake(NameIdentifier.of(metalakeName), "", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Map<String, String> properties = Maps.newHashMap();
    switch (getProvider()) {
      case "hive":
        properties.put(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI, hiveMetastoreUri);
        break;
      case "lakehouse-iceberg":
        properties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND, "hive");
        properties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, warehouse);
        properties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, hiveMetastoreUri);
        break;
      default:
        throw new IllegalArgumentException("Unsupported provider: " + getProvider());
    }
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
            .enableHiveSupport()
            .getOrCreate();
  }
}
