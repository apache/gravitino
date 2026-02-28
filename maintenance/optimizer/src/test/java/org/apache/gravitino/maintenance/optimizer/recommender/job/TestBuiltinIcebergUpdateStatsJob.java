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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.Statistic;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Environment IT for triggering built-in Iceberg update stats job through the client API. */
@EnabledIfEnvironmentVariable(named = "GRAVITINO_ENV_IT", matches = "true")
public class TestBuiltinIcebergUpdateStatsJob {

  private static final String SERVER_URI = "http://localhost:8090";
  private static final String ICEBERG_REST_URI = "http://localhost:9001/iceberg";
  private static final String JOB_TEMPLATE_NAME = "builtin-iceberg-update-stats";
  private static final String SPARK_CATALOG_NAME = "rest_catalog";
  private static final String WAREHOUSE_LOCATION = "";
  private static final String METALAKE_NAME = "test";

  @Test
  void testRunBuiltinUpdateStatsJobAndPersistStatistics() throws Exception {
    String tableName = "update_stats_table_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    SparkSession spark = createSparkSession();
    try (GravitinoAdminClient adminClient = GravitinoAdminClient.builder(SERVER_URI).build()) {
      GravitinoMetalake metalake = loadOrCreateMetalake(adminClient, METALAKE_NAME);
      recreateRestCatalog(metalake);
      createTableAndInsertData(spark, fullTableName);
      submitJob(metalake, buildUpdateStatsJobConfig(tableName));

      try (GravitinoClient client =
          GravitinoClient.builder(SERVER_URI).withMetalake(METALAKE_NAME).build()) {
        Table table =
            client
                .loadCatalog(SPARK_CATALOG_NAME)
                .asTableCatalog()
                .loadTable(NameIdentifier.of("db", tableName));
        List<Statistic> statistics = table.supportsStatistics().listStatistics();
        Map<String, Statistic> statisticMap =
            statistics.stream().collect(Collectors.toMap(Statistic::name, s -> s));
        Assertions.assertTrue(statisticMap.containsKey("custom-file_count"));
        Assertions.assertTrue(statisticMap.containsKey("custom-datafile_mse"));
        Assertions.assertTrue(statisticMap.containsKey("custom-total_size"));
      }
    } finally {
      spark.stop();
    }
  }

  private static SparkSession createSparkSession() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("builtin-iceberg-update-stats-it")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".type", "rest")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".cache-enabled", "false")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".warehouse", WAREHOUSE_LOCATION)
        .getOrCreate();
  }

  private static void createTableAndInsertData(SparkSession spark, String fullTableName) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    spark.sql("CREATE TABLE " + fullTableName + " (id INT, data STRING) USING iceberg");
    spark.sql(
        "ALTER TABLE "
            + fullTableName
            + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024000')");
    for (int i = 0; i < 10; i++) {
      spark.sql("INSERT INTO " + fullTableName + " VALUES (" + i + ", 'value_" + i + "')");
    }
  }

  private static Map<String, String> buildUpdateStatsJobConfig(String tableName) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("table_identifier", "db." + tableName);
    jobConf.put("gravitino_uri", SERVER_URI);
    jobConf.put("metalake", METALAKE_NAME);
    jobConf.put("target_file_size_bytes", "100000");
    jobConf.put("statistics_updater", "gravitino-statistics-updater");
    jobConf.put("catalog_name", SPARK_CATALOG_NAME);
    jobConf.put("catalog_type", "rest");
    jobConf.put("catalog_uri", ICEBERG_REST_URI);
    jobConf.put("warehouse_location", WAREHOUSE_LOCATION);
    jobConf.put("spark_master", "local[2]");
    jobConf.put("spark_executor_instances", "1");
    jobConf.put("spark_executor_cores", "1");
    jobConf.put("spark_executor_memory", "1g");
    jobConf.put("spark_driver_memory", "1g");
    jobConf.put("spark_conf", "{\"spark.hadoop.fs.defaultFS\":\"file:///\"}");
    return jobConf;
  }

  private static void submitJob(GravitinoMetalake metalake, Map<String, String> jobConf) {
    JobHandle jobHandle = metalake.runJob(JOB_TEMPLATE_NAME, jobConf);
    Assertions.assertTrue(StringUtils.isNotBlank(jobHandle.jobId()), "Job id should not be blank");

    Awaitility.await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(2))
        .until(
            () -> {
              JobHandle.Status status = metalake.getJob(jobHandle.jobId()).jobStatus();
              return status == JobHandle.Status.SUCCEEDED
                  || status == JobHandle.Status.FAILED
                  || status == JobHandle.Status.CANCELLED;
            });

    JobHandle.Status finalStatus = metalake.getJob(jobHandle.jobId()).jobStatus();
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, finalStatus, "Job should succeed");
  }

  private static GravitinoMetalake loadOrCreateMetalake(
      GravitinoAdminClient client, String metalakeName) {
    try {
      return client.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException ignored) {
      return client.createMetalake(metalakeName, "IT metalake", Map.of());
    }
  }

  private static void recreateRestCatalog(GravitinoMetalake metalake) {
    try {
      metalake.dropCatalog(SPARK_CATALOG_NAME, true);
    } catch (Exception ignored) {
      // Ignore when the catalog does not exist, or when force-drop is not needed.
    }

    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-backend", "REST");
    properties.put("uri", ICEBERG_REST_URI);

    metalake.createCatalog(
        SPARK_CATALOG_NAME,
        Catalog.Type.RELATIONAL,
        "lakehouse-iceberg",
        "IT Iceberg REST catalog",
        properties);
  }
}
