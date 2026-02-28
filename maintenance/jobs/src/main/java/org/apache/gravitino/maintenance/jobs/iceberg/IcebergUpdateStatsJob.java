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
import java.util.Map;
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
public class IcebergUpdateStatsJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergUpdateStatsJob.class);

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-update-stats";
  private static final String VERSION = "v1";
  private static final String DEFAULT_STATISTICS_UPDATER = "gravitino-statistics-updater";
  private static final String DEFAULT_METRICS_UPDATER = "gravitino-metrics-updater";
  private static final long DEFAULT_TARGET_FILE_SIZE_BYTES = 100_000L;
  private static final String CUSTOM_STAT_PREFIX = "custom-";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "Built-in Iceberg update stats job template for computing datafile MSE and file metrics")
        .withExecutable(resolveExecutable(IcebergUpdateStatsJob.class))
        .withClassName(IcebergUpdateStatsJob.class.getName())
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
    String gravitinoUri = argMap.get("gravitino-uri");
    String metalake = argMap.get("metalake");

    if (catalogName == null
        || tableIdentifier == null
        || gravitinoUri == null
        || metalake == null) {
      System.err.println(
          "Error: --catalog, --table, --gravitino-uri and --metalake are required arguments");
      printUsage();
      System.exit(1);
    }

    String updaterName =
        argMap.getOrDefault("statistics-updater", DEFAULT_STATISTICS_UPDATER).trim();
    boolean enableMetrics = parseEnableMetrics(argMap.get("enable-metrics"));
    String metricsUpdaterName =
        argMap.getOrDefault("metrics-updater", DEFAULT_METRICS_UPDATER).trim();
    long targetFileSizeBytes = parseTargetFileSize(argMap.get("target-file-size-bytes"));
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
      statisticsUpdater = createStatisticsUpdater(updaterName, gravitinoUri, metalake);
      if (enableMetrics) {
        metricsUpdater = createMetricsUpdater(metricsUpdaterName, gravitinoUri, metalake);
      }
      updateStatistics(
          spark,
          statisticsUpdater,
          metricsUpdater,
          catalogName,
          tableIdentifier,
          targetFileSizeBytes);
    } catch (Exception e) {
      LOG.error("Failed to update Iceberg statistics", e);
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
        spark, statisticsUpdater, null, catalogName, tableIdentifier, targetFileSizeBytes);
  }

  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater statisticsUpdater,
      MetricsUpdater metricsUpdater,
      String catalogName,
      String tableIdentifier,
      long targetFileSizeBytes) {
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
        partitionStatistics.put(partitionPath, statistics);
        if (metricsUpdater != null) {
          tableAndPartitionMetrics.addAll(
              toPartitionMetricPoints(
                  gravitinoTableIdentifier, partitionPath, statistics, metricTimestamp));
        }
      }
      statisticsUpdater.updatePartitionStatistics(gravitinoTableIdentifier, partitionStatistics);
      if (metricsUpdater != null && !tableAndPartitionMetrics.isEmpty()) {
        metricsUpdater.updateTableAndPartitionMetrics(tableAndPartitionMetrics);
      }
      LOG.info(
          "Updated partition statistics for {} partitions on {}",
          partitionStatistics.size(),
          gravitinoTableIdentifier);
    } else {
      String sql = buildTableStatsSql(catalogName, tableIdentifier, targetFileSizeBytes);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      List<StatisticEntry<?>> tableStatistics =
          rows.length == 0 ? List.of() : toStatistics(rows[0]);
      statisticsUpdater.updateTableStatistics(gravitinoTableIdentifier, tableStatistics);
      if (metricsUpdater != null && !tableStatistics.isEmpty()) {
        metricsUpdater.updateTableAndPartitionMetrics(
            toTableMetricPoints(gravitinoTableIdentifier, tableStatistics, metricTimestamp));
      }
      LOG.info(
          "Updated table statistics with {} metrics on {}",
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
        + targetFileSizeBytes
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
        + targetFileSizeBytes
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
    if (sparkConfJson == null || sparkConfJson.isEmpty()) {
      return new HashMap<>();
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> parsedMap =
          mapper.readValue(sparkConfJson, new TypeReference<Map<String, Object>>() {});
      Map<String, String> configs = new HashMap<>();
      for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
        configs.put(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString());
      }
      return configs;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse Spark configurations JSON: "
              + sparkConfJson
              + ". Error: "
              + e.getMessage(),
          e);
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

  static boolean parseEnableMetrics(String value) {
    if (value == null || value.trim().isEmpty()) {
      return false;
    }
    if ("true".equalsIgnoreCase(value.trim())) {
      return true;
    }
    if ("false".equalsIgnoreCase(value.trim())) {
      return false;
    }
    throw new IllegalArgumentException("Invalid enable-metrics value: " + value);
  }

  private static StatisticsUpdater createStatisticsUpdater(
      String updaterName, String gravitinoUri, String metalake) {
    StatisticsUpdater statisticsUpdater =
        ProviderUtils.createStatisticsUpdaterInstance(updaterName);
    Map<String, String> conf = new HashMap<>();
    conf.put(OptimizerConfig.GRAVITINO_URI, gravitinoUri);
    conf.put(OptimizerConfig.GRAVITINO_METALAKE, metalake);
    statisticsUpdater.initialize(new OptimizerEnv(new OptimizerConfig(conf)));
    return statisticsUpdater;
  }

  private static MetricsUpdater createMetricsUpdater(
      String updaterName, String gravitinoUri, String metalake) {
    MetricsUpdater metricsUpdater = ProviderUtils.createMetricsUpdaterInstance(updaterName);
    Map<String, String> conf = new HashMap<>();
    conf.put(OptimizerConfig.GRAVITINO_URI, gravitinoUri);
    conf.put(OptimizerConfig.GRAVITINO_METALAKE, metalake);
    metricsUpdater.initialize(new OptimizerEnv(new OptimizerConfig(conf)));
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

  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--gravitino-uri",
        "{{gravitino_uri}}",
        "--metalake",
        "{{metalake}}",
        "--target-file-size-bytes",
        "{{target_file_size_bytes}}",
        "--statistics-updater",
        "{{statistics_updater}}",
        "--enable-metrics",
        "{{enable_metrics}}",
        "--metrics-updater",
        "{{metrics_updater}}",
        "--spark-conf",
        "{{spark_conf}}");
  }

  private static Map<String, String> buildSparkConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put("spark.master", "{{spark_master}}");
    configs.put("spark.executor.instances", "{{spark_executor_instances}}");
    configs.put("spark.executor.cores", "{{spark_executor_cores}}");
    configs.put("spark.executor.memory", "{{spark_executor_memory}}");
    configs.put("spark.driver.memory", "{{spark_driver_memory}}");
    configs.put("spark.sql.catalog.{{catalog_name}}", "org.apache.iceberg.spark.SparkCatalog");
    configs.put("spark.sql.catalog.{{catalog_name}}.type", "{{catalog_type}}");
    configs.put("spark.sql.catalog.{{catalog_name}}.uri", "{{catalog_uri}}");
    configs.put("spark.sql.catalog.{{catalog_name}}.warehouse", "{{warehouse_location}}");
    configs.put(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    return Collections.unmodifiableMap(configs);
  }

  private static void printUsage() {
    System.err.println(
        "Usage: IcebergUpdateStatsJob [OPTIONS]\n"
            + "\n"
            + "Required Options:\n"
            + "  --catalog <name>                   Iceberg catalog name registered in Spark\n"
            + "  --table <identifier>               Table name in schema.table format\n"
            + "  --gravitino-uri <uri>              Gravitino server URI\n"
            + "  --metalake <metalake_name>         Gravitino metalake name\n"
            + "\n"
            + "Optional Options:\n"
            + "  --target-file-size-bytes <bytes>   Small-file threshold and MSE target\n"
            + "                                     Default: 100000\n"
            + "  --statistics-updater <name>        StatisticsUpdater provider name\n"
            + "                                     Default: gravitino-statistics-updater\n"
            + "  --enable-metrics <true|false>     Whether to persist metrics via MetricsUpdater\n"
            + "                                     Default: false\n"
            + "  --metrics-updater <name>           MetricsUpdater provider name\n"
            + "                                     Default: gravitino-metrics-updater\n"
            + "  --spark-conf <json>                JSON map of custom Spark configs\n"
            + "                                     Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'");
  }
}
