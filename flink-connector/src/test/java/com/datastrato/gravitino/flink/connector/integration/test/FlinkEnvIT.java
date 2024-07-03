/*
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
package com.datastrato.gravitino.flink.connector.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.hadoop.fs.FileSystem;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlinkEnvIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvIT.class);
  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();
  private static final int DEFAULT_PARALLELISM = 4;

  protected static final String GRAVITINO_METALAKE = "flink";
  protected static final String DEFAULT_CATALOG = "default_catalog";

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .build());

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
    initFlinkEnv(true);
    LOG.info("Startup Flink env successfully, Gravitino uri: {}.", gravitinoUri);
  }

  @AfterAll
  static void stop() {
    stopFlinkEnv();
    stopHdfsEnv();
    LOG.info("Stop Flink env successfully.");
  }

  @AfterEach
  public final void cleanupRunningJobs() throws Exception {
    if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
      // do nothing if the MiniCluster is not running
      LOG.warn("Mini cluster is not running after the test!");
      return;
    }

    for (JobStatusMessage path : MINI_CLUSTER_RESOURCE.getClusterClient().listJobs().get()) {
      if (!path.getJobState().isTerminalState()) {
        try {
          MINI_CLUSTER_RESOURCE.getClusterClient().cancel(path.getJobId()).get();
        } catch (Exception ignored) {
          // ignore exceptions when cancelling dangling jobs
        }
      }
    }
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

  protected static void initFlinkEnv(boolean isBatchMode) {
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);

    EnvironmentSettings.Builder builder =
        EnvironmentSettings.newInstance().withConfiguration(configuration);
    if (isBatchMode) {
      tableEnv = TableEnvironment.create(builder.inBatchMode().build());
    } else {
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(
              MINI_CLUSTER_RESOURCE.getClientConfiguration());
      tableEnv = StreamTableEnvironment.create(env, builder.inStreamingMode().build());
    }
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

  protected static void doWithSchema(
      Catalog catalog, String schemaName, Consumer<Catalog> action, boolean dropSchema) {
    Preconditions.checkNotNull(catalog);
    Preconditions.checkNotNull(schemaName);
    try {
      tableEnv.useCatalog(catalog.name());
      if (!catalog.asSchemas().schemaExists(schemaName)) {
        catalog
            .asSchemas()
            .createSchema(
                schemaName, null, ImmutableMap.of("location", warehouse + "/" + schemaName));
      }
      tableEnv.useDatabase(schemaName);
      action.accept(catalog);
    } finally {
      if (dropSchema) {
        catalog.asSchemas().dropSchema(schemaName, true);
      }
    }
  }

  protected static void doWithCatalog(Catalog catalog, Consumer<Catalog> action) {
    Preconditions.checkNotNull(catalog);
    tableEnv.useCatalog(catalog.name());
    action.accept(catalog);
  }
}
