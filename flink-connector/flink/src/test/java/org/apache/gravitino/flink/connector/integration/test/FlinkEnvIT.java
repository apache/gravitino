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
package org.apache.gravitino.flink.connector.integration.test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlinkEnvIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvIT.class);
  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();

  protected static final String icebergRestServiceName = "iceberg-rest";

  protected static final String GRAVITINO_METALAKE = "flink";
  protected static final String DEFAULT_CATALOG = "default_catalog";

  protected static GravitinoMetalake metalake;
  protected static TableEnvironment tableEnv;

  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";

  protected static String warehouse;

  protected static FileSystem hdfs;

  private static String gravitinoUri = "http://127.0.0.1:8090";

  private final String lakeHouseIcebergProvider = "lakehouse-iceberg";

  protected String icebergRestServiceUri;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    initHiveEnv();
    if (lakeHouseIcebergProvider.equalsIgnoreCase(getProvider())) {
      initIcebergRestServiceEnv();
    }
    // Start Gravitino server
    super.startIntegrationTest();
    initGravitinoEnv();
    initMetalake();
    initHdfsEnv();
    initFlinkEnv();
    initCatalogEnv();
    LOG.info("Startup Flink env successfully, Gravitino uri: {}.", gravitinoUri);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() {
    try {
      stopCatalogEnv();
      stopFlinkEnv();
      stopHdfsEnv();
      super.stopIntegrationTest();
      LOG.info("Stop Flink env successfully.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void initCatalogEnv() throws Exception {}
  ;

  protected void stopCatalogEnv() throws Exception {}

  protected String flinkByPass(String key) {
    return PropertiesConverter.FLINK_PROPERTY_PREFIX + key;
  }

  protected abstract String getProvider();

  private void initIcebergRestServiceEnv() {
    super.ignoreAuxRestService = false;
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

  private void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUrl
    int gravitinoPort = getGravitinoServerPort();
    gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);
    if (lakeHouseIcebergProvider.equalsIgnoreCase(getProvider())) {
      this.icebergRestServiceUri = getIcebergRestServiceUri();
    }
  }

  private void initMetalake() {
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
    EnvironmentSettings.Builder builder =
        EnvironmentSettings.newInstance().withConfiguration(configuration);
    tableEnv = TableEnvironment.create(builder.inBatchMode().build());
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
      } catch (Exception e) {
        LOG.error("Close Flink environment failed", e);
      }
    }
  }

  @FormatMethod
  protected static TableResult sql(@FormatString String sql, Object... args) {
    return tableEnv.executeSql(String.format(sql, args));
  }

  protected void doWithSchema(
      Catalog catalog, String schemaName, Consumer<Catalog> action, boolean dropSchema) {
    doWithSchema(catalog, schemaName, action, dropSchema, true);
  }

  protected void doWithSchema(
      Catalog catalog,
      String schemaName,
      Consumer<Catalog> action,
      boolean dropSchema,
      boolean cascade) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(schemaName);
    try {
      tableEnv.useCatalog(catalog.name());
      if (!catalog.asSchemas().schemaExists(schemaName)) {
        catalog.asSchemas().createSchema(schemaName, null, getCreateSchemaProps(schemaName));
      }
      tableEnv.useDatabase(schemaName);
      action.accept(catalog);
    } finally {
      if (dropSchema) {
        clearTableInSchema();
        catalog.asSchemas().dropSchema(schemaName, cascade);
      }
    }
  }

  protected Map<String, String> getCreateSchemaProps(String schemaName) {
    return null;
  }

  protected static void doWithCatalog(Catalog catalog, Consumer<Catalog> action) {
    Preconditions.checkNotNull(catalog);
    tableEnv.useCatalog(catalog.name());
    action.accept(catalog);
  }

  /** Iceberg requires deleting the table first, then deleting the schema. */
  protected static void clearTableInSchema() {
    TableResult result = sql("SHOW TABLES");
    List<Row> rows = Lists.newArrayList(result.collect());
    for (Row row : rows) {
      String tableName = row.getField(0).toString();
      TableResult deleteResult = sql("DROP TABLE IF EXISTS %s", tableName);
      TestUtils.assertTableResult(deleteResult, ResultKind.SUCCESS);
    }
  }
}
