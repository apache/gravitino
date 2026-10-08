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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.ProviderUtils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Built-in job for collecting and atomically merging Iceberg manifest statistics by spec. */
public class IcebergUpdateManifestStatsJob implements BuiltInJob {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergUpdateManifestStatsJob.class);
  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-update-manifest-stats";
  private static final String DEFAULT_STATISTICS_UPDATER = "gravitino-statistics-updater";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Collect manifest count and average size for one Iceberg partition spec")
        .withExecutable(resolveExecutable(IcebergUpdateManifestStatsJob.class))
        .withClassName(IcebergUpdateManifestStatsJob.class.getName())
        .withArguments(
            Arrays.asList(
                "--catalog",
                "{{catalog_name}}",
                "--table",
                "{{table_identifier}}",
                "--spec-id",
                "{{spec_id:-}}",
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
   * @param args catalog, table, optional spec ID, updater options, and Spark configuration
   * @throws Exception if collection, persistence, or resource cleanup fails
   */
  public static void main(String[] args) throws Exception {
    Map<String, String> arguments = IcebergJobUtils.parseArguments(args);
    String catalog = arguments.get("catalog");
    String table = arguments.get("table");
    tableIdentifier(catalog, table);
    Integer specId = parseSpecId(arguments.get("spec-id"));
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
        updateStatistics(spark, updater, catalog, table, specId);
      } finally {
        spark.stop();
      }
    }
  }

  /**
   * Collects a complete manifest measurement from one snapshot for one resolved partition spec.
   *
   * @param spark Spark session with the Iceberg catalog configured
   * @param catalogName Spark catalog name
   * @param tableIdentifier schema.table identifier
   * @param requestedSpecId requested spec, or null to resolve the current default once
   * @return measurements carrying the resolved spec ID for evaluation and submission
   */
  public static IcebergManifestStatistics collectManifestStatistics(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable Integer requestedSpecId) {
    String identifier = buildTableIdentifier(catalogName, tableIdentifier);
    final Table table;
    try {
      table = Spark3Util.loadIcebergTable(spark, identifier);
      table.refresh();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot load Iceberg table " + identifier, e);
    }
    int specId = requestedSpecId == null ? table.spec().specId() : requestedSpecId;
    if (specId < 0 || !table.specs().containsKey(specId)) {
      throw new IllegalArgumentException("Unknown partition spec ID: " + specId);
    }
    // Pin one snapshot and the resolved spec even if the table evolves during collection.
    Snapshot snapshot = table.currentSnapshot();
    long count = 0;
    double totalBytes = 0;
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        if (manifest.partitionSpecId() == specId) {
          count++;
          totalBytes += manifest.length();
        }
      }
    }
    return new IcebergManifestStatistics(specId, count, count == 0 ? 0D : totalBytes / count);
  }

  @VisibleForTesting
  static void updateStatistics(
      SparkSession spark,
      StatisticsUpdater updater,
      String catalogName,
      String tableIdentifier,
      @Nullable Integer requestedSpecId) {
    NameIdentifier identifier = tableIdentifier(catalogName, tableIdentifier);
    IcebergManifestStatistics measurements =
        collectManifestStatistics(spark, catalogName, tableIdentifier, requestedSpecId);
    updater.mergeTableStatistics(identifier, measurements.statistics());
    LOG.info(
        "Updated manifest statistics for {} spec {}: count={}, averageBytes={}",
        identifier,
        measurements.specId(),
        measurements.count(),
        measurements.averageSize());
  }

  @VisibleForTesting
  @Nullable
  static Integer parseSpecId(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    if (!value.matches("[0-9]+")) {
      throw new IllegalArgumentException("spec_id must be a non-negative 32-bit integer");
    }
    return Integer.valueOf(value);
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
