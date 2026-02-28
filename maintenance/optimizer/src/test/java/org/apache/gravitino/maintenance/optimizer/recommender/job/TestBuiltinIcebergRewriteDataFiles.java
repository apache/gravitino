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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.Statistic;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

// Requires a running Gravitino server and Spark environment; enable with GRAVITINO_ENV_IT=true.
@EnabledIfEnvironmentVariable(named = "GRAVITINO_ENV_IT", matches = "true")
public class TestBuiltinIcebergRewriteDataFiles {

  private static final String SERVER_URI = "http://localhost:8090";
  private static final String METALAKE_NAME = "test";
  private static final String ICEBERG_REST_URI = "http://localhost:9001/iceberg";
  private static final String JOB_TEMPLATE_NAME = "builtin-iceberg-rewrite-data-files";
  private static final String UPDATE_STATS_JOB_TEMPLATE_NAME = "builtin-iceberg-update-stats";
  private static final String SPARK_CATALOG_NAME = "rest_catalog";
  private static final String WAREHOUSE_LOCATION = "";

  @Test
  void testSubmitRewriteDataFilesJobByConfig() throws Exception {
    String tableName = "rewrite_table1";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;
    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);
          Map<String, String> jobConf = buildManualJobConfig(tableName);
          submitCompactionJob(metalake, jobConf);
        });
  }

  @Test
  void testSubmitBuiltinIcebergRewriteDataFilesJobFromAdapterAndOptimizerConfig() throws Exception {
    String tableName = "rewrite_table2";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;
    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);
          OptimizerConfig optimizerConfig = createOptimizerConfig();
          Map<String, String> jobOptions =
              Map.of("min-input-files", "1", "target-file-size-bytes", "1048576");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig,
                  tableName,
                  jobOptions,
                  new Column[0],
                  new Transform[0],
                  Collections.emptyList());
          submitCompactionJob(metalake, jobConf);
        });
  }

  @Test
  void testCompactNonPartitionTable() throws Exception {
    String tableName = "rewrite_non_partition_table";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);

          long beforeFiles = countDataFiles(spark, fullTableName);
          Assertions.assertTrue(beforeFiles > 1, "Expected multiple data files before compaction");

          OptimizerConfig optimizerConfig = createOptimizerConfig();
          Map<String, String> jobOptions = Map.of("min-input-files", "1");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig,
                  tableName,
                  jobOptions,
                  new Column[0],
                  new Transform[0],
                  Collections.emptyList());
          submitCompactionJob(metalake, jobConf);

          refreshTable(spark, fullTableName);
          long afterFiles = countDataFiles(spark, fullTableName);
          Assertions.assertTrue(
              afterFiles < beforeFiles,
              String.format(
                  "Expected fewer data files after compaction after:%d, before:%d",
                  afterFiles, beforeFiles));
        });
  }

  @Test
  void testCompactPartitionTable() throws Exception {
    String tableName = "rewrite_partition_table";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createPartitionTableAndInsertData(spark, fullTableName);

          Map<String, Long> beforeCounts = countDataFilesByPartition(spark, fullTableName);
          Map<String, Long> beforeMaxSizes = maxFileSizeByPartition(spark, fullTableName);
          String targetPartition1 = partitionKey(2024, "1");
          String targetPartition2 = partitionKey(2024, "2");
          String otherPartition1 = partitionKey(2025, "1");
          String otherPartition2 = partitionKey(2025, "2");

          Assertions.assertTrue(
              beforeCounts.getOrDefault(targetPartition1, 0L) > 1,
              "Expected multiple data files in " + targetPartition1 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(targetPartition2, 0L) > 1,
              "Expected multiple data files in " + targetPartition2 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(otherPartition1, 0L) > 1,
              "Expected multiple data files in " + otherPartition1 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(otherPartition2, 0L) > 1,
              "Expected multiple data files in " + otherPartition2 + " before compaction");

          OptimizerConfig optimizerConfig = createOptimizerConfig();

          List<PartitionPath> partitions =
              Arrays.asList(
                  PartitionPath.of(
                      Arrays.asList(
                          new PartitionEntryImpl("year", "2024"),
                          new PartitionEntryImpl("month", "1"))),
                  PartitionPath.of(
                      Arrays.asList(
                          new PartitionEntryImpl("year", "2024"),
                          new PartitionEntryImpl("month", "2"))));
          Column[] columns =
              new Column[] {
                column("year", Types.IntegerType.get()), column("month", Types.StringType.get())
              };
          Transform[] partitioning =
              new Transform[] {Transforms.identity("year"), Transforms.identity("month")};

          Map<String, String> jobOptions = Map.of("min-input-files", "1");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig, tableName, jobOptions, columns, partitioning, partitions);
          submitCompactionJob(metalake, jobConf);

          refreshTable(spark, fullTableName);
          Map<String, Long> afterCounts = countDataFilesByPartition(spark, fullTableName);
          Map<String, Long> afterMaxSizes = maxFileSizeByPartition(spark, fullTableName);
          Assertions.assertTrue(
              afterMaxSizes.getOrDefault(targetPartition1, 0L)
                  > beforeMaxSizes.getOrDefault(targetPartition1, 0L),
              "Expected larger data files after compaction for " + targetPartition1);
          Assertions.assertTrue(
              afterMaxSizes.getOrDefault(targetPartition2, 0L)
                  > beforeMaxSizes.getOrDefault(targetPartition2, 0L),
              "Expected larger data files after compaction for " + targetPartition2);
          Assertions.assertEquals(
              beforeCounts.getOrDefault(otherPartition1, 0L),
              afterCounts.getOrDefault(otherPartition1, 0L),
              "Expected no compaction for " + otherPartition1);
          Assertions.assertEquals(
              beforeCounts.getOrDefault(otherPartition2, 0L),
              afterCounts.getOrDefault(otherPartition2, 0L),
              "Expected no compaction for " + otherPartition2);
          Assertions.assertEquals(
              beforeMaxSizes.getOrDefault(otherPartition1, 0L),
              afterMaxSizes.getOrDefault(otherPartition1, 0L),
              "Expected no size changes for " + otherPartition1);
          Assertions.assertEquals(
              beforeMaxSizes.getOrDefault(otherPartition2, 0L),
              afterMaxSizes.getOrDefault(otherPartition2, 0L),
              "Expected no size changes for " + otherPartition2);
        });
  }

  @Test
  void testSubmitBuiltinIcebergUpdateStatsJobAndPersistStatistics() throws Exception {
    String tableName = "update_stats_table";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;
    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);

          Map<String, String> jobConf = buildUpdateStatsJobConfig(tableName);
          submitJob(metalake, UPDATE_STATS_JOB_TEMPLATE_NAME, jobConf);

          try (GravitinoClient client =
              GravitinoClient.builder(SERVER_URI).withMetalake(METALAKE_NAME).build()) {
            Table table =
                client
                    .loadCatalog(SPARK_CATALOG_NAME)
                    .asTableCatalog()
                    .loadTable(NameIdentifier.of("db", tableName));
            List<Statistic> stats = table.supportsStatistics().listStatistics();
            Assertions.assertFalse(stats.isEmpty(), "Expected table statistics to be updated");

            Map<String, Statistic> statsMap = new HashMap<>();
            for (Statistic stat : stats) {
              statsMap.put(stat.name(), stat);
            }
            Assertions.assertTrue(statsMap.containsKey("custom-file_count"));
            Assertions.assertTrue(statsMap.containsKey("custom-datafile_mse"));
            Assertions.assertTrue(statsMap.containsKey("custom-total_size"));
          }
        });
  }

  private static GravitinoMetalake loadOrCreateMetalake(
      GravitinoAdminClient client, String metalakeName) {
    try {
      return client.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException ignored) {
      return client.createMetalake(metalakeName, "IT metalake", Map.of());
    }
  }

  @FunctionalInterface
  private interface SparkMetalakeConsumer {
    void accept(SparkSession spark, GravitinoMetalake metalake) throws Exception;
  }

  private static void runWithSparkAndMetalake(SparkMetalakeConsumer consumer) throws Exception {
    SparkSession spark = createSparkSession();
    try (GravitinoAdminClient client = GravitinoAdminClient.builder(SERVER_URI).build()) {
      GravitinoMetalake metalake = loadOrCreateMetalake(client, METALAKE_NAME);
      consumer.accept(spark, metalake);
    } finally {
      spark.stop();
    }
  }

  private static void submitCompactionJob(GravitinoMetalake metalake, Map<String, String> jobConf) {
    submitJob(metalake, JOB_TEMPLATE_NAME, jobConf);
  }

  private static void submitJob(
      GravitinoMetalake metalake, String jobTemplateName, Map<String, String> jobConf) {
    JobHandle jobHandle = metalake.runJob(jobTemplateName, jobConf);
    System.out.println("Submitted job id: " + jobHandle.jobId());
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

  private static Map<String, String> buildManualJobConfig(String tableName) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("table_identifier", "db." + tableName);
    jobConf.put("where_clause", "");
    jobConf.put("sort_order", "");
    jobConf.put("strategy", "binpack");
    jobConf.put("options", "{\"min-input-files\":\"1\"}");
    jobConf.putAll(createOptimizerConfig().jobSubmitterConfigs());
    return jobConf;
  }

  private static Map<String, String> buildUpdateStatsJobConfig(String tableName) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("table_identifier", "db." + tableName);
    jobConf.put("gravitino_uri", SERVER_URI);
    jobConf.put("metalake", METALAKE_NAME);
    jobConf.put("target_file_size_bytes", "100000");
    jobConf.put("statistics_updater", "gravitino-statistics-updater");
    jobConf.putAll(createOptimizerConfig().jobSubmitterConfigs());
    return jobConf;
  }

  private static Map<String, String> buildCompactionJobConfig(
      OptimizerConfig optimizerConfig,
      String tableName,
      Map<String, String> jobOptions,
      Column[] columns,
      Transform[] partitioning,
      List<PartitionPath> partitions) {
    CompactionJobContext jobContext =
        new CompactionJobContext(
            NameIdentifier.of(SPARK_CATALOG_NAME, "db", tableName),
            jobOptions,
            JOB_TEMPLATE_NAME,
            columns,
            partitioning,
            partitions);
    GravitinoCompactionJobAdapter adapter = new GravitinoCompactionJobAdapter();
    return GravitinoJobSubmitter.buildJobConfig(optimizerConfig, jobContext, adapter);
  }

  private static long countDataFiles(SparkSession spark, String fullTableName) {
    Row row =
        spark
            .sql(
                "SELECT count(*) AS data_file_cnt FROM "
                    + fullTableName
                    + ".files WHERE content = 0")
            .first();
    return row.getLong(0);
  }

  private static void refreshTable(SparkSession spark, String fullTableName) {
    spark.sql("REFRESH TABLE " + fullTableName);
  }

  private static Map<String, Long> countDataFilesByPartition(
      SparkSession spark, String fullTableName) {
    List<Row> rows =
        spark
            .sql(
                "SELECT partition.year AS year, partition.month AS month, COUNT(*) AS data_file_cnt "
                    + "FROM "
                    + fullTableName
                    + ".files WHERE content = 0 "
                    + "GROUP BY partition.year, partition.month")
            .collectAsList();
    Map<String, Long> counts = new HashMap<>();
    for (Row row : rows) {
      int year = row.getInt(0);
      String month = row.getString(1);
      long count = row.getLong(2);
      counts.put(partitionKey(year, month), count);
    }
    return counts;
  }

  private static Map<String, Long> maxFileSizeByPartition(
      SparkSession spark, String fullTableName) {
    List<Row> rows =
        spark
            .sql(
                "SELECT partition.year AS year, partition.month AS month, "
                    + "MAX(file_size_in_bytes) AS max_size "
                    + "FROM "
                    + fullTableName
                    + ".files WHERE content = 0 "
                    + "GROUP BY partition.year, partition.month")
            .collectAsList();
    Map<String, Long> sizes = new HashMap<>();
    for (Row row : rows) {
      int year = row.getInt(0);
      String month = row.getString(1);
      long maxSize = row.getLong(2);
      sizes.put(partitionKey(year, month), maxSize);
    }
    return sizes;
  }

  private static String partitionKey(int year, String month) {
    return "year=" + year + ",month=" + month;
  }

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return ColumnDTO.builder().withName(name).withDataType(type).build();
  }

  private static OptimizerConfig createOptimizerConfig() {
    Map<String, String> optimizerConfigProps = new HashMap<>();
    optimizerConfigProps.put(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_type", "rest");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_uri", ICEBERG_REST_URI);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "warehouse_location", WAREHOUSE_LOCATION);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_name", SPARK_CATALOG_NAME);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_master", "local[2]");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_instances", "1");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_cores", "1");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_memory", "1g");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_driver_memory", "1g");
    optimizerConfigProps.put(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_conf", "{}");
    return new OptimizerConfig(optimizerConfigProps);
  }

  private static SparkSession createSparkSession() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("builtin-iceberg-rewrite-data-files-it")
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

  private static void createPartitionTableAndInsertData(SparkSession spark, String fullTableName) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT, data STRING, year INT, month STRING) USING iceberg "
            + "PARTITIONED BY (year, month)");
    spark.sql(
        "ALTER TABLE "
            + fullTableName
            + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024000')");
    int id = 0;
    int[] years = new int[] {2024, 2025};
    String[] months = new String[] {"1", "2"};
    int rowsPerPartition = 20;
    for (int year : years) {
      for (String month : months) {
        for (int i = 0; i < rowsPerPartition; i++) {
          spark.sql(
              "INSERT INTO "
                  + fullTableName
                  + " VALUES ("
                  + id
                  + ", 'value_"
                  + id
                  + "', "
                  + year
                  + ", '"
                  + month
                  + "'"
                  + ")");
          id++;
        }
      }
    }
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
}
