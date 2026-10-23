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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoAuthSettings;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Built-in job for collecting all Iceberg manifest statistics in one table cycle. */
public class IcebergUpdateManifestStatsJob implements BuiltInJob {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergUpdateManifestStatsJob.class);
  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-update-manifest-stats";
  private static final String DEFAULT_STATISTICS_UPDATER = "gravitino-statistics-updater";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Collect manifest count and average size for every Iceberg partition spec")
        .withExecutable(resolveExecutable(IcebergUpdateManifestStatsJob.class))
        .withClassName(IcebergUpdateManifestStatsJob.class.getName())
        .withArguments(
            Arrays.asList(
                "--catalog",
                "{{catalog_name}}",
                "--table",
                "{{table_identifier}}",
                "--coordination-dir",
                "{{coordination_directory:-}}",
                "--updater-options",
                "{{updater_options}}",
                "--spark-conf",
                "{{spark_conf:-}}"))
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, "v1"))
        .build();
  }

  /**
   * Collects and persists manifest statistics using the configured Spark catalog and updater.
   *
   * @param args catalog, table, optional shared coordination directory, updater options, and Spark
   *     configuration
   * @throws Exception if collection, persistence, or resource cleanup fails
   */
  public static void main(String[] args) throws Exception {
    Map<String, String> arguments =
        IcebergJobUtils.parseArguments(
            args,
            ImmutableSet.of(
                "catalog", "table", "updater-options", "spark-conf", "coordination-dir"),
            ImmutableSet.of("catalog", "table", "updater-options"));
    String coordinationDirectory = arguments.get("coordination-dir");
    Path coordinationPath =
        coordinationDirectory == null || coordinationDirectory.trim().isEmpty()
            ? null
            : Paths.get(coordinationDirectory);
    String catalog = arguments.get("catalog");
    String table = arguments.get("table");
    tableIdentifier(catalog, table);
    Map<String, String> options =
        IcebergSparkConfigUtils.parseFlatJsonMap(
            arguments.get("updater-options"), "updater-options");
    Map<String, String> properties = buildOptimizerProperties(options);
    Map<String, String> sparkConfigs =
        IcebergSparkConfigUtils.parseFlatJsonMap(arguments.get("spark-conf"), "spark-conf");
    try (StatisticsUpdater updater =
        ProviderUtils.createStatisticsUpdaterInstance(
            options.getOrDefault("statistics_updater", DEFAULT_STATISTICS_UPDATER).trim())) {
      updater.initialize(new OptimizerEnv(new OptimizerConfig(properties)));
      SparkSession.Builder builder =
          SparkSession.builder().appName("Gravitino Iceberg Manifest Statistics");
      sparkConfigs.forEach(builder::config);
      SparkSession spark = builder.getOrCreate();
      try {
        IcebergJobUtils.requireIcebergSparkRuntime();
        updateStatistics(
            spark,
            updater,
            catalog,
            table,
            properties.get(OptimizerConfig.GRAVITINO_METALAKE),
            coordinationPath);
      } finally {
        spark.stop();
      }
    }
  }

  /**
   * Collects complete per-spec measurements from one pinned table snapshot.
   *
   * @param spark Spark session with the Iceberg catalog configured
   * @param catalogName Spark catalog name
   * @param tableIdentifier schema.table identifier
   * @return both complete maps and the snapshot ID (-1 for a table without a snapshot)
   */
  public static List<StatisticEntry<?>> collectManifestStatistics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    String identifier = buildTableIdentifier(catalogName, tableIdentifier);
    final Table table;
    try {
      table = Spark3Util.loadIcebergTable(spark, identifier);
      table.refresh();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot load Iceberg table " + identifier, e);
    }
    return collectManifestStatistics(table);
  }

  @VisibleForTesting
  static List<StatisticEntry<?>> collectManifestStatistics(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    Map<Integer, Long> counts = new HashMap<>();
    Map<Integer, Double> sizes = new HashMap<>();
    table.specs().keySet().forEach(spec -> counts.put(spec, 0L));
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        int spec = manifest.partitionSpecId();
        counts.merge(spec, 1L, Long::sum);
        sizes.merge(spec, (double) manifest.length(), Double::sum);
      }
    }
    Map<String, StatisticValue<?>> countValues = new HashMap<>();
    Map<String, StatisticValue<?>> averageValues = new HashMap<>();
    counts.forEach(
        (spec, count) -> {
          String key = Integer.toString(spec);
          countValues.put(key, StatisticValues.longValue(count));
          averageValues.put(
              key, StatisticValues.doubleValue(count == 0 ? 0D : sizes.get(spec) / count));
        });
    return Arrays.asList(
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.MANIFEST_NUMBER, StatisticValues.objectValue(countValues)),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.AVG_MANIFEST_SIZE,
            StatisticValues.objectValue(averageValues)),
        new StatisticEntryImpl<>(
            IcebergManifestStatistics.SNAPSHOT_ID,
            StatisticValues.longValue(snapshot == null ? -1L : snapshot.snapshotId())));
  }

  @VisibleForTesting
  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater updater,
      String catalogName,
      String tableIdentifier,
      String metalake,
      @Nullable Path coordinationDirectory)
      throws IOException {
    NameIdentifier identifier = tableIdentifier(catalogName, tableIdentifier);
    if (coordinationDirectory == null) {
      updater.updateTableStatistics(
          identifier, collectManifestStatistics(spark, catalogName, tableIdentifier));
      LOG.info("Updated complete manifest statistics for {}", identifier);
      return;
    }
    if (!coordinationDirectory.isAbsolute() || !Files.isDirectory(coordinationDirectory)) {
      throw new IllegalArgumentException(
          "coordination-dir must be an existing absolute shared directory");
    }
    String key =
        Hashing.sha256()
            .hashString(
                metalake + "\0" + catalogName + "\0" + tableIdentifier, StandardCharsets.UTF_8)
            .toString();
    Path lockPath = coordinationDirectory.resolve(key + ".lock");
    // Never delete lock files: replacing the inode lets two hosts lock different files.
    try (FileChannel channel =
            FileChannel.open(
                lockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        FileLock lock = channel.tryLock()) {
      if (lock == null) {
        throw new IllegalStateException("Manifest collection is already running for " + identifier);
      }
      if (channel.size() != 0) {
        throw new IllegalStateException(
            "Unfinished manifest collection requires recovery: " + lockPath);
      }
      List<StatisticEntry<?>> measurements =
          collectManifestStatistics(spark, catalogName, tableIdentifier);
      byte[] marker =
          ("In-progress manifest collection for "
                  + metalake
                  + "."
                  + identifier
                  + "; verify the previous process and any outstanding PUT have finished before recovery.\n")
              .getBytes(StandardCharsets.UTF_8);
      ByteBuffer buffer = ByteBuffer.wrap(marker);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
      updater.updateTableStatistics(identifier, measurements);
      // A failed or ambiguous PUT leaves the marker intact, preventing an unsafe automatic retry.
      channel.truncate(0);
      channel.force(true);
      LOG.info("Updated complete manifest statistics for {}", identifier);
    }
  }

  @VisibleForTesting
  static Map<String, String> buildOptimizerProperties(Map<String, String> options) {
    Map<String, String> properties = new HashMap<>(options);
    if (options.containsKey("gravitino_uri")) {
      properties.put(OptimizerConfig.GRAVITINO_URI, options.get("gravitino_uri"));
    }
    if (options.containsKey("metalake")) {
      properties.put(OptimizerConfig.GRAVITINO_METALAKE, options.get("metalake"));
    }
    for (String key :
        Arrays.asList(OptimizerConfig.GRAVITINO_URI, OptimizerConfig.GRAVITINO_METALAKE)) {
      String value = properties.get(key);
      if (value == null || value.trim().isEmpty()) {
        throw new IllegalArgumentException("updater_options must configure " + key);
      }
      properties.put(key, value.trim());
    }
    // A retried PUT can hide an older request that is still executing after a connection failure.
    properties.put(GravitinoClientConfiguration.CLIENT_RETRY_TIMES, "0");
    GravitinoAuthSettings.copyAliases(properties);
    return properties;
  }

  @VisibleForTesting
  static String buildTableIdentifier(String catalogName, String tableName) {
    NameIdentifier identifier = tableIdentifier(catalogName, tableName);
    return IcebergJobUtils.escapeSqlIdentifier(identifier.namespace().level(0))
        + "."
        + IcebergJobUtils.escapeSqlIdentifier(identifier.namespace().level(1))
        + "."
        + IcebergJobUtils.escapeSqlIdentifier(identifier.name());
  }

  private static NameIdentifier tableIdentifier(
      @Nullable String catalogName, @Nullable String tableName) {
    if (catalogName == null || catalogName.trim().isEmpty() || tableName == null) {
      throw new IllegalArgumentException("--catalog and --table are required");
    }
    String[] levels = tableName.split("\\.", -1);
    if (levels.length != 2 || levels[0].trim().isEmpty() || levels[1].trim().isEmpty()) {
      throw new IllegalArgumentException("--table must use schema.table format: " + tableName);
    }
    return NameIdentifier.of(catalogName, levels[0], levels[1]);
  }
}
