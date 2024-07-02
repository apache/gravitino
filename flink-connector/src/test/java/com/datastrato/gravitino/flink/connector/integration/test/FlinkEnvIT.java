/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlinkEnvIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvIT.class);
  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();
  protected static final String GRAVITINO_METALAKE = "flink";
  protected static final String DEFAULT_CATALOG = "default_catalog";

  protected static GravitinoMetalake metalake;
  protected static TableEnvironment tableEnv;

  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";

  protected static String warehouse;

  protected static FileSystem hdfs;

  private static String gravitinoUri = "http://127.0.0.1:8090";

  @BeforeAll
  static void startUp() {
    // Start Gravitino server
    initGravitinoEnv();
    initMetalake();
    initHiveEnv();
    initHdfsEnv();
    initFlinkEnv();
    LOG.info("Startup Flink env successfully, Gravitino uri: {}.", gravitinoUri);
  }

  @AfterAll
  static void stop() {
    stopFlinkEnv();
    stopHdfsEnv();
    LOG.info("Stop Flink env successfully.");
  }

  protected String flinkByPass(String key) {
    return PropertiesConverter.FLINK_PROPERTY_PREFIX + key;
  }

  private static void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUrl
    int gravitinoPort = getGravitinoServerPort();
    gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);
  }

  private static void initMetalake() {
    metalake = client.createMetalake(GRAVITINO_METALAKE, "", Collections.emptyMap());
  }

  private static void initHiveEnv() {
    CONTAINER_SUITE.startHiveContainer();
    hiveMetastoreUri =
        String.format(
            "thrift://%s:%d",
            CONTAINER_SUITE.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse",
            CONTAINER_SUITE.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }

  private static void initHdfsEnv() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(
        "fs.defaultFS",
        String.format(
            "hdfs://%s:%d",
            CONTAINER_SUITE.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT));
    try {
      hdfs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error("Create HDFS filesystem failed", e);
      throw new RuntimeException(e);
    }
  }

  private static void initFlinkEnv() {
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);
    tableEnv = TableEnvironment.create(configuration);
  }

  private static void stopHdfsEnv() {
    if (hdfs != null) {
      try {
        hdfs.close();
      } catch (IOException e) {
        LOG.error("Close HDFS filesystem failed", e);
      }
    }
  }

  private static void stopFlinkEnv() {
    if (tableEnv != null) {
      try {
        TableEnvironmentImpl env = (TableEnvironmentImpl) tableEnv;
        env.getCatalogManager().close();
      } catch (Throwable throwable) {
        LOG.error("Close Flink environment failed", throwable);
      }
    }
  }

  @FormatMethod
  protected TableResult sql(@FormatString String sql, Object... args) {
    return tableEnv.executeSql(String.format(sql, args));
  }

  protected static void doWithCatalog(Catalog catalog, Consumer<Catalog> action) {
    Preconditions.checkNotNull(catalog);
    tableEnv.useCatalog(catalog.name());
    action.accept(catalog);
  }
}
