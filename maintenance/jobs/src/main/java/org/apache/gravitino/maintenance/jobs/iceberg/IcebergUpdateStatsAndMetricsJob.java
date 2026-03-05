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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.api.common.MetricPoint;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.MetricsUpdater;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Built-in job for computing Iceberg table file statistics and persisting them to Gravitino. */
public class IcebergUpdateStatsAndMetricsJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergUpdateStatsAndMetricsJob.class);

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-update-stats";
  private static final String VERSION = "v1";
  private static final String DEFAULT_STATISTICS_UPDATER = "gravitino-statistics-updater";
  private static final String DEFAULT_METRICS_UPDATER = "gravitino-metrics-updater";
  private static final long DEFAULT_TARGET_FILE_SIZE_BYTES = 128L * 1024 * 1024;
  private static final long SMALL_FILE_THRESHOLD_BYTES = 32L * 1024 * 1024;
  private static final String DEFAULT_UPDATE_MODE = UpdateMode.ALL.modeName;
  private static final String CUSTOM_STAT_PREFIX = "custom-";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "Built-in Iceberg update stats job template for computing datafile MSE and file metrics")
        .withExecutable(resolveExecutable(IcebergUpdateStatsAndMetricsJob.class))
        .withClassName(IcebergUpdateStatsAndMetricsJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /** Main entry point. */
  public static void main(String[] args) {
    Map<String, String> argMap = parseArguments(args);
    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");
    UpdateMode updateMode = parseUpdateMode(argMap.get("update-mode"));

    if (catalogName == null || tableIdentifier == null) {
      System.err.println("Error: --catalog and --table are required arguments");
      printUsage();
      System.exit(1);
    }

    long targetFileSizeBytes = parseTargetFileSize(argMap.get("target-file-size-bytes"));
    Map<String, String> updaterOptions = parseJsonOptions(argMap.get("updater-options"));
    String sparkConfJson = argMap.get("spark-conf");

    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Update Stats");

    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      Map<String, String> customConfigs = parseCustomSparkConfigs(sparkConfJson);
      for (Map.Entry<String, String> entry : customConfigs.entrySet()) {
        sparkBuilder.config(entry.getKey(), entry.getValue());
      }
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    StatisticsUpdater statisticsUpdater = null;
    MetricsUpdater metricsUpdater = null;
    try {
      Map<String, String> optimizerProperties = buildOptimizerProperties(updaterOptions);
      if (updateMode.updateStats) {
        String statisticsUpdaterName =
            updaterOptions.getOrDefault("statistics_updater", DEFAULT_STATISTICS_UPDATER).trim();
        statisticsUpdater =
            createStatisticsUpdater(
                statisticsUpdaterName, requireGravitinoConfig(optimizerProperties));
      }
      if (updateMode.updateMetrics) {
        String metricsUpdaterName =
            updaterOptions.getOrDefault("metrics_updater", DEFAULT_METRICS_UPDATER).trim();
        metricsUpdater = createMetricsUpdater(metricsUpdaterName, optimizerProperties);
      }

      updateStatistics(
          spark,
          statisticsUpdater,
          metricsUpdater,
          updateMode,
          catalogName,
          tableIdentifier,
          targetFileSizeBytes);
    } catch (Exception e) {
      LOG.error("Failed to update Iceberg statistics/metrics", e);
      System.exit(1);
    } finally {
      if (statisticsUpdater != null) {
        try {
          statisticsUpdater.close();
        } catch (Exception e) {
          LOG.warn("Failed to close statistics updater", e);
        }
      }
      if (metricsUpdater != null) {
        try {
          metricsUpdater.close();
        } catch (Exception e) {
          LOG.warn("Failed to close metrics updater", e);
        }
      }
      spark.stop();
    }
  }

  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater statisticsUpdater,
      String catalogName,
      String tableIdentifier,
      long targetFileSizeBytes) {
    updateStatistics(
        spark,
        statisticsUpdater,
        null,
        UpdateMode.STATS,
        catalogName,
        tableIdentifier,
        targetFileSizeBytes);
  }

  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater statisticsUpdater,
      MetricsUpdater metricsUpdater,
      String catalogName,
      String tableIdentifier,
      long targetFileSizeBytes) {
    updateStatistics(
        spark,
        statisticsUpdater,
        metricsUpdater,
        UpdateMode.ALL,
        catalogName,
        tableIdentifier,
        targetFileSizeBytes);
  }

  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater statisticsUpdater,
      MetricsUpdater metricsUpdater,
      UpdateMode updateMode,
      String catalogName,
      String tableIdentifier,
      long targetFileSizeBytes) {
    Objects.requireNonNull(updateMode, "updateMode must not be null");

    if (updateMode.updateStats && statisticsUpdater == null) {
      throw new IllegalArgumentException(
          "Statistics updater must be configured when update_mode is stats or all");
    }

    if (updateMode.updateMetrics && metricsUpdater == null) {
      throw new IllegalArgumentException(
          "Metrics updater must be configured when update_mode is metrics or all");
    }

    NameIdentifier gravitinoTableIdentifier =
        toGravitinoTableIdentifier(catalogName, tableIdentifier);
    long metricTimestamp = System.currentTimeMillis() / 1000L;
    boolean partitioned = isPartitionedTable(spark, catalogName, tableIdentifier);
    if (partitioned) {
      String sql = buildPartitionStatsSql(catalogName, tableIdentifier, targetFileSizeBytes);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics = new LinkedHashMap<>();
      List<MetricPoint> tableAndPartitionMetrics = new ArrayList<>();
      for (Row row : rows) {
        PartitionPath partitionPath = toPartitionPath(row.getAs("partition"));
        List<StatisticEntry<?>> statistics = toStatistics(row);
        if (updateMode.updateStats) {
          partitionStatistics.put(partitionPath, statistics);
        }
        if (updateMode.updateMetrics) {
          tableAndPartitionMetrics.addAll(
              toPartitionMetricPoints(
                  gravitinoTableIdentifier, partitionPath, statistics, metricTimestamp));
        }
      }

      if (updateMode.updateStats) {
        statisticsUpdater.updatePartitionStatistics(gravitinoTableIdentifier, partitionStatistics);
      }

      if (updateMode.updateMetrics && !tableAndPartitionMetrics.isEmpty()) {
        metricsUpdater.updateTableAndPartitionMetrics(tableAndPartitionMetrics);
      }

      LOG.info(
          "Updated partition data in mode {} for {} partitions on {}",
          updateMode.modeName,
          rows.length,
          gravitinoTableIdentifier);
    } else {
      String sql = buildTableStatsSql(catalogName, tableIdentifier, targetFileSizeBytes);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      List<StatisticEntry<?>> tableStatistics =
          rows.length == 0 ? Collections.emptyList() : toStatistics(rows[0]);

      if (updateMode.updateStats) {
        statisticsUpdater.updateTableStatistics(gravitinoTableIdentifier, tableStatistics);
      }

      if (updateMode.updateMetrics && !tableStatistics.isEmpty()) {
        metricsUpdater.updateTableAndPartitionMetrics(
            toTableMetricPoints(gravitinoTableIdentifier, tableStatistics, metricTimestamp));
      }

      LOG.info(
          "Updated table data in mode {} with {} metrics on {}",
          updateMode.modeName,
          tableStatistics.size(),
          gravitinoTableIdentifier);
    }
  }

  static String buildTableStatsSql(
      String catalogName, String tableIdentifier, long targetFileSizeBytes) {
    String filesTable = buildFilesTableIdentifier(catalogName, tableIdentifier);
    return "SELECT "
        + "COUNT(*) AS file_count, "
        + "SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files, "
        + "SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files, "
        + "SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files, "
        + "SUM(CASE WHEN file_size_in_bytes < "
        + SMALL_FILE_THRESHOLD_BYTES
        + " THEN 1 ELSE 0 END) AS small_files, "
        + "AVG(POWER("
        + targetFileSizeBytes
        + " - LEAST("
        + targetFileSizeBytes
        + ", file_size_in_bytes), 2)) AS datafile_mse, "
        + "AVG(file_size_in_bytes) AS avg_size, "
        + "SUM(file_size_in_bytes) AS total_size "
        + "FROM "
        + filesTable;
  }

  static String buildPartitionStatsSql(
      String catalogName, String tableIdentifier, long targetFileSizeBytes) {
    String filesTable = buildFilesTableIdentifier(catalogName, tableIdentifier);
    return "SELECT "
        + "partition, "
        + "COUNT(*) AS file_count, "
        + "SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files, "
        + "SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files, "
        + "SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files, "
        + "SUM(CASE WHEN file_size_in_bytes < "
        + SMALL_FILE_THRESHOLD_BYTES
        + " THEN 1 ELSE 0 END) AS small_files, "
        + "AVG(POWER("
        + targetFileSizeBytes
        + " - LEAST("
        + targetFileSizeBytes
        + ", file_size_in_bytes), 2)) AS datafile_mse, "
        + "AVG(file_size_in_bytes) AS avg_size, "
        + "SUM(file_size_in_bytes) AS total_size "
        + "FROM "
        + filesTable
        + " GROUP BY partition";
  }

  static boolean isPartitionedTable(
      SparkSession spark, String catalogName, String tableIdentifier) {
    StructType filesSchema =
        spark.table(buildFilesTableIdentifier(catalogName, tableIdentifier)).schema();
    if (!Arrays.asList(filesSchema.fieldNames()).contains("partition")) {
      return false;
    }
    StructField partitionField = filesSchema.apply("partition");
    if (!(partitionField.dataType() instanceof StructType)) {
      return false;
    }
    return ((StructType) partitionField.dataType()).fields().length > 0;
  }

  static List<StatisticEntry<?>> toStatistics(Row row) {
    List<StatisticEntry<?>> statistics = new ArrayList<>();
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "file_count",
            StatisticValues.longValue(toLongValue(row, "file_count"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "data_files",
            StatisticValues.longValue(toLongValue(row, "data_files"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "position_delete_files",
            StatisticValues.longValue(toLongValue(row, "position_delete_files"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "equality_delete_files",
            StatisticValues.longValue(toLongValue(row, "equality_delete_files"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "small_files",
            StatisticValues.longValue(toLongValue(row, "small_files"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "datafile_mse",
            StatisticValues.doubleValue(toDoubleValue(row, "datafile_mse"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "avg_size",
            StatisticValues.doubleValue(toDoubleValue(row, "avg_size"))));
    statistics.add(
        new StatisticEntryImpl<>(
            CUSTOM_STAT_PREFIX + "total_size",
            StatisticValues.longValue(toLongValue(row, "total_size"))));
    return statistics;
  }

  static PartitionPath toPartitionPath(Row partitionRow) {
    StructType partitionSchema = partitionRow.schema();
    List<PartitionEntry> entries = new ArrayList<>(partitionSchema.fields().length);
    for (int i = 0; i < partitionSchema.fields().length; i++) {
      String name = partitionSchema.fields()[i].name();
      Object value = partitionRow.get(i);
      entries.add(new PartitionEntryImpl(name, String.valueOf(value)));
    }
    return PartitionPath.of(entries);
  }

  static Map<String, String> parseArguments(String[] args) {
    Map<String, String> argMap = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2);
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          String value = args[i + 1];
          if (value != null && !value.trim().isEmpty()) {
            argMap.put(key, value);
          }
          i++;
        }
      }
    }
    return argMap;
  }

  static Map<String, String> parseCustomSparkConfigs(String sparkConfJson) {
    return parseJsonOptions(sparkConfJson);
  }

  static Map<String, String> parseJsonOptions(String json) {
    if (json == null || json.isEmpty()) {
      return new HashMap<>();
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> parsedMap =
          mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
      Map<String, String> configs = new HashMap<>();
      for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof Map || value instanceof List) {
          throw new IllegalArgumentException(
              String.format(
                  Locale.ROOT,
                  "JSON options must be a flat key-value map, but key '%s' has non-scalar value",
                  entry.getKey()));
        }
        configs.put(entry.getKey(), value == null ? "" : value.toString());
      }
      return configs;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse JSON options: " + json + ". Error: " + e.getMessage(), e);
    }
  }

  static long parseTargetFileSize(String value) {
    if (value == null || value.trim().isEmpty()) {
      return DEFAULT_TARGET_FILE_SIZE_BYTES;
    }
    try {
      long parsed = Long.parseLong(value.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException("target-file-size-bytes must be > 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid target-file-size-bytes: " + value, e);
    }
  }

  static UpdateMode parseUpdateMode(String value) {
    if (value == null || value.trim().isEmpty()) {
      return UpdateMode.from(DEFAULT_UPDATE_MODE);
    }
    return UpdateMode.from(value);
  }

  static Map<String, String> buildOptimizerProperties(Map<String, String> updaterOptions) {
    Map<String, String> optimizerProperties = new HashMap<>(updaterOptions);

    Optional<String> gravitinoUri =
        firstNonEmpty(
            updaterOptions.get("gravitino_uri"),
            updaterOptions.get("gravitino-uri"),
            updaterOptions.get(OptimizerConfig.GRAVITINO_URI));
    Optional<String> metalake =
        firstNonEmpty(
            updaterOptions.get("metalake"), updaterOptions.get(OptimizerConfig.GRAVITINO_METALAKE));

    gravitinoUri.ifPresent(uri -> optimizerProperties.put(OptimizerConfig.GRAVITINO_URI, uri));
    metalake.ifPresent(value -> optimizerProperties.put(OptimizerConfig.GRAVITINO_METALAKE, value));
    return optimizerProperties;
  }

  static Map<String, String> requireGravitinoConfig(Map<String, String> optimizerProperties) {
    String gravitinoUri = optimizerProperties.get(OptimizerConfig.GRAVITINO_URI);
    String metalake = optimizerProperties.get(OptimizerConfig.GRAVITINO_METALAKE);

    if (gravitinoUri == null || gravitinoUri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "updater_options must contain 'gravitino_uri' when update_mode is stats or all");
    }

    if (metalake == null || metalake.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "updater_options must contain 'metalake' when update_mode is stats or all");
    }

    return optimizerProperties;
  }

  private static StatisticsUpdater createStatisticsUpdater(
      String updaterName, Map<String, String> optimizerProperties) {
    StatisticsUpdater statisticsUpdater =
        ProviderUtils.createStatisticsUpdaterInstance(updaterName);
    statisticsUpdater.initialize(new OptimizerEnv(new OptimizerConfig(optimizerProperties)));
    return statisticsUpdater;
  }

  private static MetricsUpdater createMetricsUpdater(
      String updaterName, Map<String, String> optimizerProperties) {
    MetricsUpdater metricsUpdater = ProviderUtils.createMetricsUpdaterInstance(updaterName);
    metricsUpdater.initialize(new OptimizerEnv(new OptimizerConfig(optimizerProperties)));
    return metricsUpdater;
  }

  private static List<MetricPoint> toTableMetricPoints(
      NameIdentifier tableIdentifier,
      List<StatisticEntry<?>> tableStatistics,
      long metricTimestamp) {
    List<MetricPoint> requests = new ArrayList<>(tableStatistics.size());
    for (StatisticEntry<?> statistic : tableStatistics) {
      requests.add(
          MetricPoint.forTable(
              tableIdentifier, statistic.name(), statistic.value(), metricTimestamp));
    }
    return requests;
  }

  private static List<MetricPoint> toPartitionMetricPoints(
      NameIdentifier tableIdentifier,
      PartitionPath partitionPath,
      List<StatisticEntry<?>> statistics,
      long metricTimestamp) {
    List<MetricPoint> requests = new ArrayList<>(statistics.size());
    for (StatisticEntry<?> statistic : statistics) {
      requests.add(
          MetricPoint.forPartition(
              tableIdentifier,
              partitionPath,
              statistic.name(),
              statistic.value(),
              metricTimestamp));
    }
    return requests;
  }

  private static NameIdentifier toGravitinoTableIdentifier(
      String catalogName, String tableIdentifier) {
    String[] levels = tableIdentifier.split("\\.");
    if (levels.length != 2) {
      throw new IllegalArgumentException(
          "--table must use schema.table format, but got: " + tableIdentifier);
    }
    return NameIdentifier.of(catalogName, levels[0], levels[1]);
  }

  private static String buildFilesTableIdentifier(String catalogName, String tableIdentifier) {
    String[] levels = tableIdentifier.split("\\.");
    if (levels.length != 2) {
      throw new IllegalArgumentException(
          "--table must use schema.table format, but got: " + tableIdentifier);
    }
    return escapeSqlIdentifier(catalogName)
        + "."
        + escapeSqlIdentifier(levels[0])
        + "."
        + escapeSqlIdentifier(levels[1])
        + ".files";
  }

  private static String escapeSqlIdentifier(String identifier) {
    return identifier.replace("`", "``");
  }

  private static long toLongValue(Row row, String fieldName) {
    Number number = row.getAs(fieldName);
    return number == null ? 0L : number.longValue();
  }

  private static double toDoubleValue(Row row, String fieldName) {
    Number number = row.getAs(fieldName);
    return number == null ? 0D : number.doubleValue();
  }

  private static Optional<String> firstNonEmpty(String... candidates) {
    for (String candidate : candidates) {
      if (candidate != null && !candidate.trim().isEmpty()) {
        return Optional.of(candidate.trim());
      }
    }
    return Optional.empty();
  }

  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--update-mode",
        "{{update_mode}}",
        "--target-file-size-bytes",
        "{{target_file_size_bytes}}",
        "--updater-options",
        "{{updater_options}}",
        "--spark-conf",
        "{{spark_conf}}");
  }

  private static Map<String, String> buildSparkConfigs() {
    return Collections.emptyMap();
  }

  private static void printUsage() {
    System.err.println(
        "Usage: IcebergUpdateStatsAndMetricsJob [OPTIONS]\\n"
            + "\\n"
            + "Required Options:\\n"
            + "  --catalog <name>                   Iceberg catalog name registered in Spark\\n"
            + "  --table <identifier>               Table name in schema.table format\\n"
            + "\\n"
            + "Optional Options:\\n"
            + "  --update-mode <stats|metrics|all> Update behavior mode, default: all\\n"
            + "  --target-file-size-bytes <bytes>   MSE target file size in bytes\\n"
            + "                                     Default: 134217728 (128MB)\\n"
            + "                                     small_files threshold is fixed at 33554432 (32MB)\\n"
            + "  --updater-options <json>           JSON map for updater and repository settings\\n"
            + "                                     Example: '{\"gravitino_uri\":\"http://localhost:8090\",\\n"
            + "                                     \"metalake\":\"test\",\"statistics_updater\":\"gravitino-statistics-updater\",\\n"
            + "                                     \"metrics_updater\":\"gravitino-metrics-updater\"}'\\n"
            + "  --spark-conf <json>                JSON map of custom Spark configs\\n"
            + "                                     Must include Iceberg catalog configs for --catalog\\n"
            + "                                     Example: '{\"spark.master\":\"local[2]\","
            + "\"spark.sql.catalog.rest_catalog\":\"org.apache.iceberg.spark.SparkCatalog\","
            + "\"spark.sql.catalog.rest_catalog.type\":\"rest\","
            + "\"spark.sql.catalog.rest_catalog.uri\":\"http://localhost:9001/iceberg\"}'");
  }

  enum UpdateMode {
    STATS("stats", true, false),
    METRICS("metrics", false, true),
    ALL("all", true, true);

    private final String modeName;
    private final boolean updateStats;
    private final boolean updateMetrics;

    UpdateMode(String modeName, boolean updateStats, boolean updateMetrics) {
      this.modeName = modeName;
      this.updateStats = updateStats;
      this.updateMetrics = updateMetrics;
    }

    static UpdateMode from(String value) {
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      for (UpdateMode mode : values()) {
        if (mode.modeName.equals(normalized)) {
          return mode;
        }
      }
      throw new IllegalArgumentException(
          "Invalid update_mode value: " + value + ". Supported values are: stats, metrics, all");
    }
  }
}
