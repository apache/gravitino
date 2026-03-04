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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.optimizer.api.common.DataScope;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.GravitinoMetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.updater.metrics.storage.jdbc.GenericJdbcMetricsRepository;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.utils.jdbc.JdbcSqlScriptUtils;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;

/** Integration tests for IcebergUpdateStatsJob with a real Spark+Iceberg runtime. */
public class TestIcebergUpdateStatsJobWithSpark {

  private static final String SERVER_URI = "http://localhost:8090";
  private static final String ICEBERG_REST_URI = "http://localhost:9001/iceberg";
  private static final String JOB_TEMPLATE_NAME = "builtin-iceberg-update-stats";
  private static final String SPARK_CATALOG_NAME = "rest_catalog";
  private static final String METALAKE_NAME = "test";

  @TempDir static File tempDir;

  private static SparkSession spark;
  private static String catalogName;

  @BeforeAll
  public static void setUp() {
    String warehousePath = new File(tempDir, "warehouse").getAbsolutePath();
    catalogName = "test_catalog";

    spark =
        SparkSession.builder()
            .appName("TestIcebergUpdateStatsJob")
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".type", "hadoop")
            .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
            .getOrCreate();

    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.non_partitioned (id INT, name STRING) USING iceberg");
    spark.sql(
        "INSERT INTO " + catalogName + ".db.non_partitioned VALUES (1, 'A'), (2, 'B'), (3, 'C')");

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.partitioned (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)");
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".db.partitioned VALUES "
            + "(1, '2026-01-01'), (2, '2026-01-01'), (3, '2026-01-02')");

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.multi_partitioned (id INT, event_ts TIMESTAMP, region STRING) "
            + "USING iceberg PARTITIONED BY (days(event_ts), region)");
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".db.multi_partitioned VALUES "
            + "(1, TIMESTAMP '2026-01-01 09:00:00', 'ap-south'), "
            + "(2, TIMESTAMP '2026-01-01 11:00:00', 'us-east'), "
            + "(3, TIMESTAMP '2026-01-02 08:30:00', 'ap-south'), "
            + "(4, TIMESTAMP '2026-01-02 12:45:00', 'us-east')");
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.non_partitioned");
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.partitioned");
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.multi_partitioned");
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db");
      spark.stop();
    }
  }

  @Test
  public void testUpdateNonPartitionedTableStatistics() {
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        updater,
        null,
        IcebergUpdateStatsJob.UpdateMode.STATS,
        catalogName,
        "db.non_partitioned",
        100_000L);

    assertEquals(NameIdentifier.of(catalogName, "db", "non_partitioned"), updater.tableIdentifier);
    assertNotNull(updater.tableStatistics);
    assertEquals(8, updater.tableStatistics.size());
    assertTrue(updater.partitionStatistics.isEmpty());

    Map<String, Object> stats =
        updater.tableStatistics.stream()
            .collect(Collectors.toMap(StatisticEntry::name, stat -> stat.value().value()));
    assertTrue((Long) stats.get("custom-file_count") > 0L);
    assertTrue((Double) stats.get("custom-datafile_mse") >= 0D);
    assertTrue((Long) stats.get("custom-total_size") > 0L);
  }

  @Test
  public void testUpdatePartitionedTableStatistics() {
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        updater,
        null,
        IcebergUpdateStatsJob.UpdateMode.STATS,
        catalogName,
        "db.partitioned",
        100_000L);

    assertEquals(NameIdentifier.of(catalogName, "db", "partitioned"), updater.tableIdentifier);
    assertTrue(updater.tableStatistics.isEmpty());
    assertFalse(updater.partitionStatistics.isEmpty());
    assertEquals(2, updater.partitionStatistics.size());

    updater.partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          assertEquals(1, partitionPath.entries().size());
          assertEquals("ds", partitionPath.entries().get(0).partitionName());
          Map<String, Object> statMap =
              statistics.stream()
                  .collect(Collectors.toMap(StatisticEntry::name, stat -> stat.value().value()));
          assertTrue(statMap.containsKey("custom-datafile_mse"));
          assertTrue((Long) statMap.get("custom-file_count") > 0L);
        });
  }

  @Test
  public void testUpdatePartitionedTableStatisticsAndMetrics() {
    RecordingStatisticsUpdater statisticsUpdater = new RecordingStatisticsUpdater();
    RecordingMetricsUpdater metricsUpdater = new RecordingMetricsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        statisticsUpdater,
        metricsUpdater,
        IcebergUpdateStatsJob.UpdateMode.ALL,
        catalogName,
        "db.partitioned",
        100_000L);

    assertEquals(
        NameIdentifier.of(catalogName, "db", "partitioned"), statisticsUpdater.tableIdentifier);
    assertFalse(statisticsUpdater.partitionStatistics.isEmpty());
    assertEquals(2, statisticsUpdater.partitionStatistics.size());

    assertEquals(16, metricsUpdater.tableMetrics.size());
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.scope() == DataScope.Type.PARTITION));
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.partitionPath().isPresent()));
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.value() != null && metric.value().value() != null));
    assertTrue(metricsUpdater.jobMetrics.isEmpty());
  }

  @Test
  public void testUpdatePartitionedTableMetricsOnly() {
    RecordingMetricsUpdater metricsUpdater = new RecordingMetricsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        null,
        metricsUpdater,
        IcebergUpdateStatsJob.UpdateMode.METRICS,
        catalogName,
        "db.partitioned",
        100_000L);

    assertEquals(16, metricsUpdater.tableMetrics.size());
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.scope() == DataScope.Type.PARTITION));
    assertTrue(metricsUpdater.jobMetrics.isEmpty());
  }

  @Test
  public void testUpdateNonPartitionedTableMetricsOnly() {
    RecordingMetricsUpdater metricsUpdater = new RecordingMetricsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        null,
        metricsUpdater,
        IcebergUpdateStatsJob.UpdateMode.METRICS,
        catalogName,
        "db.non_partitioned",
        100_000L);

    assertEquals(8, metricsUpdater.tableMetrics.size());
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.scope() == DataScope.Type.TABLE));
    assertTrue(
        metricsUpdater.tableMetrics.stream().allMatch(metric -> metric.partitionPath().isEmpty()));
    assertTrue(metricsUpdater.jobMetrics.isEmpty());
  }

  @Test
  public void testUpdateNonPartitionedTableStatisticsAndMetrics() {
    RecordingStatisticsUpdater statisticsUpdater = new RecordingStatisticsUpdater();
    RecordingMetricsUpdater metricsUpdater = new RecordingMetricsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        statisticsUpdater,
        metricsUpdater,
        IcebergUpdateStatsJob.UpdateMode.ALL,
        catalogName,
        "db.non_partitioned",
        100_000L);

    assertEquals(
        NameIdentifier.of(catalogName, "db", "non_partitioned"), statisticsUpdater.tableIdentifier);
    assertNotNull(statisticsUpdater.tableStatistics);
    assertEquals(8, statisticsUpdater.tableStatistics.size());

    assertEquals(8, metricsUpdater.tableMetrics.size());
    assertTrue(
        metricsUpdater.tableMetrics.stream()
            .allMatch(metric -> metric.scope() == DataScope.Type.TABLE));
    assertTrue(
        metricsUpdater.tableMetrics.stream().allMatch(metric -> metric.partitionPath().isEmpty()));
    assertTrue(metricsUpdater.jobMetrics.isEmpty());
  }

  @Test
  @Tag("gravitino-docker-test")
  public void testUpdatePartitionedTableMetricsStoredInMySql() throws Exception {
    try (MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")) {
      mysql.start();

      Map<String, String> conf = new HashMap<>();
      conf.put("gravitino.optimizer.jdbcMetrics.jdbcUrl", mysql.getJdbcUrl());
      conf.put("gravitino.optimizer.jdbcMetrics.jdbcUser", mysql.getUsername());
      conf.put("gravitino.optimizer.jdbcMetrics.jdbcPassword", mysql.getPassword());
      conf.put("gravitino.optimizer.jdbcMetrics.jdbcDriver", mysql.getDriverClassName());
      initializeMySqlMetricsSchema(mysql);

      GravitinoMetricsUpdater metricsUpdater = new GravitinoMetricsUpdater();
      metricsUpdater.initialize(new OptimizerEnv(new OptimizerConfig(conf)));
      try {
        IcebergUpdateStatsJob.updateStatistics(
            spark,
            null,
            metricsUpdater,
            IcebergUpdateStatsJob.UpdateMode.METRICS,
            catalogName,
            "db.partitioned",
            100_000L);
      } finally {
        metricsUpdater.close();
      }

      GenericJdbcMetricsRepository repository = new GenericJdbcMetricsRepository();
      repository.initialize(conf);
      try {
        NameIdentifier identifier = NameIdentifier.of(catalogName, "db", "partitioned");
        long now = Instant.now().getEpochSecond();

        PartitionPath partitionPath =
            PartitionPath.of(List.of(new PartitionEntryImpl("ds", "2026-01-01")));
        List<MetricPoint> partitionMetrics =
            repository.getMetrics(
                DataScope.forPartition(identifier, partitionPath), now - 300, now + 300);
        assertEquals(8, partitionMetrics.size());
      } finally {
        repository.close();
      }
    }
  }

  private static void initializeMySqlMetricsSchema(MySQLContainer<?> mysql) throws Exception {
    Path schemaPath =
        findRepoRoot()
            .resolve("scripts")
            .resolve("mysql")
            .resolve("schema-" + ConfigConstants.CURRENT_SCRIPT_VERSION + "-mysql.sql");
    String schemaSql = Files.readString(schemaPath, StandardCharsets.UTF_8);
    try (Connection connection =
        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
      JdbcSqlScriptUtils.executeSqlScript(connection, schemaSql);
    }
  }

  private static Path findRepoRoot() {
    Path current = Path.of("").toAbsolutePath();
    while (current != null) {
      if (Files.exists(current.resolve("gradlew"))) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException("Failed to locate repository root containing gradlew");
  }

  @Test
  public void testUpdateMultiLevelPartitionedTableStatistics() {
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark,
        updater,
        null,
        IcebergUpdateStatsJob.UpdateMode.STATS,
        catalogName,
        "db.multi_partitioned",
        100_000L);

    assertEquals(
        NameIdentifier.of(catalogName, "db", "multi_partitioned"), updater.tableIdentifier);
    assertTrue(updater.tableStatistics.isEmpty());
    assertFalse(updater.partitionStatistics.isEmpty());
    assertEquals(4, updater.partitionStatistics.size());

    Set<String> parsedPartitions =
        updater.partitionStatistics.keySet().stream()
            .map(
                partitionPath ->
                    partitionPath.entries().get(0).partitionName()
                        + "="
                        + partitionPath.entries().get(0).partitionValue()
                        + ","
                        + partitionPath.entries().get(1).partitionName()
                        + "="
                        + partitionPath.entries().get(1).partitionValue())
            .collect(Collectors.toSet());

    assertEquals(
        Set.of(
            "event_ts_day=2026-01-01,region=ap-south",
            "event_ts_day=2026-01-01,region=us-east",
            "event_ts_day=2026-01-02,region=ap-south",
            "event_ts_day=2026-01-02,region=us-east"),
        parsedPartitions);
  }

  // Requires a running deploy-mode Gravitino server and Spark environment.
  @Test
  @EnabledIfEnvironmentVariable(named = "GRAVITINO_ENV_IT", matches = "true")
  public void testRunBuiltInUpdateStatsJobViaServer() throws Exception {
    String tableName = "jobs_it_update_stats_" + UUID.randomUUID().toString().replace("-", "");
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    try (SparkSession restSpark = createRestSparkSession();
        GravitinoAdminClient adminClient = GravitinoAdminClient.builder(SERVER_URI).build()) {
      GravitinoMetalake metalake = loadOrCreateMetalake(adminClient, METALAKE_NAME);
      recreateRestCatalog(metalake);
      createTableAndInsertData(restSpark, fullTableName);

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
        assertTrue(statisticMap.containsKey("custom-file_count"));
        assertTrue(statisticMap.containsKey("custom-datafile_mse"));
        assertTrue(statisticMap.containsKey("custom-total_size"));
      }
    }
  }

  private static SparkSession createRestSparkSession() {
    return SparkSession.builder()
        .master("local[2]")
        .appName("jobs-iceberg-update-stats-it")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".type", "rest")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".cache-enabled", "false")
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".warehouse", "")
        .getOrCreate();
  }

  private static void createTableAndInsertData(SparkSession sparkSession, String fullTableName) {
    sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    sparkSession.sql("DROP TABLE IF EXISTS " + fullTableName);
    sparkSession.sql("CREATE TABLE " + fullTableName + " (id INT, data STRING) USING iceberg");
    sparkSession.sql(
        "ALTER TABLE "
            + fullTableName
            + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024000')");
    for (int i = 0; i < 10; i++) {
      sparkSession.sql("INSERT INTO " + fullTableName + " VALUES (" + i + ", 'value_" + i + "')");
    }
  }

  private static Map<String, String> buildUpdateStatsJobConfig(String tableName) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("table_identifier", "db." + tableName);
    jobConf.put("update_mode", "all");
    jobConf.put("target_file_size_bytes", "100000");
    jobConf.put(
        "updater_options",
        "{\"gravitino_uri\":\""
            + SERVER_URI
            + "\",\"metalake\":\""
            + METALAKE_NAME
            + "\",\"statistics_updater\":\"gravitino-statistics-updater\"}");
    jobConf.put("catalog_name", SPARK_CATALOG_NAME);
    jobConf.put(
        "spark_conf",
        "{\"spark.master\":\"local[2]\","
            + "\"spark.executor.instances\":\"1\","
            + "\"spark.executor.cores\":\"1\","
            + "\"spark.executor.memory\":\"1g\","
            + "\"spark.driver.memory\":\"1g\","
            + "\"spark.hadoop.fs.defaultFS\":\"file:///\","
            + "\"spark.sql.catalog."
            + SPARK_CATALOG_NAME
            + "\":\"org.apache.iceberg.spark.SparkCatalog\","
            + "\"spark.sql.catalog."
            + SPARK_CATALOG_NAME
            + ".type\":\"rest\","
            + "\"spark.sql.catalog."
            + SPARK_CATALOG_NAME
            + ".uri\":\""
            + ICEBERG_REST_URI
            + "\","
            + "\"spark.sql.catalog."
            + SPARK_CATALOG_NAME
            + ".warehouse\":\"\","
            + "\"spark.sql.extensions\":\"org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\"}");
    return jobConf;
  }

  private static void submitJob(GravitinoMetalake metalake, Map<String, String> jobConf) {
    JobHandle jobHandle = metalake.runJob(JOB_TEMPLATE_NAME, jobConf);
    assertTrue(StringUtils.isNotBlank(jobHandle.jobId()), "Job id should not be blank");

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
    assertEquals(JobHandle.Status.SUCCEEDED, finalStatus, "Job should succeed");
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

  private static final class RecordingStatisticsUpdater implements StatisticsUpdater {
    private NameIdentifier tableIdentifier;
    private List<StatisticEntry<?>> tableStatistics = List.of();
    private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics = Map.of();

    @Override
    public String name() {
      return "recording-updater";
    }

    @Override
    public void initialize(OptimizerEnv optimizerEnv) {}

    @Override
    public void updateTableStatistics(
        NameIdentifier tableIdentifier, List<StatisticEntry<?>> tableStatistics) {
      this.tableIdentifier = tableIdentifier;
      this.tableStatistics = tableStatistics;
    }

    @Override
    public void updatePartitionStatistics(
        NameIdentifier tableIdentifier,
        Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
      this.tableIdentifier = tableIdentifier;
      this.partitionStatistics = partitionStatistics;
    }

    @Override
    public void close() {}
  }

  private static final class RecordingMetricsUpdater implements MetricsUpdater {
    private List<MetricPoint> tableMetrics = List.of();
    private List<MetricPoint> jobMetrics = List.of();

    @Override
    public String name() {
      return "recording-metrics-updater";
    }

    @Override
    public void initialize(OptimizerEnv optimizerEnv) {}

    @Override
    public void updateTableAndPartitionMetrics(List<MetricPoint> metrics) {
      this.tableMetrics = metrics;
    }

    @Override
    public void updateJobMetrics(List<MetricPoint> metrics) {
      this.jobMetrics = metrics;
    }

    @Override
    public void close() {}
  }
}
