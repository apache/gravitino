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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.jobs.iceberg.model.TableSnapshotMetrics;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in job for rewriting Iceberg table data files.
 *
 * <p>This job leverages Iceberg's RewriteDataFilesProcedure to optimize data file layout through
 * binpack or sort strategies. Z-order is a type of sort order, not a strategy.
 */
public class IcebergRewriteDataFilesJob implements BuiltInJob, IcebergMaintenanceAction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteDataFilesJob.class);

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-rewrite-data-files";
  private static final String VERSION = "v1";

  // Valid strategy values for Iceberg rewrite_data_files procedure
  private static final String STRATEGY_BINPACK = "binpack";
  private static final String STRATEGY_SORT = "sort";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in Iceberg rewrite data files job template for table optimization")
        .withExecutable(resolveExecutable(IcebergRewriteDataFilesJob.class))
        .withClassName(IcebergRewriteDataFilesJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Main entry point for the rewrite data files job.
   *
   * <p>Uses named arguments for flexibility:
   *
   * <ul>
   *   <li>--catalog &lt;catalog_name&gt; Required. Iceberg catalog name.
   *   <li>--table &lt;table_identifier&gt; Required. Table name (db.table)
   *   <li>--strategy &lt;strategy&gt; Optional. binpack or sort
   *   <li>--sort-order &lt;sort_order&gt; Optional. Sort order specification
   *   <li>--where &lt;where_clause&gt; Optional. Filter predicate
   *   <li>--options &lt;options_json&gt; Optional. JSON map of options
   *   <li>--spark-conf &lt;spark_conf_json&gt; Optional. JSON map of custom Spark configurations
   * </ul>
   *
   * <p><b>Important Notes on Special Characters:</b>
   *
   * <ul>
   *   <li><b>Via Gravitino API:</b> Pass values as-is without shell escaping. Example: {@code
   *       jobConf.put("options", "{\"a\":\"b\"}")} - Gravitino handles escaping internally via
   *       ProcessBuilder.
   *   <li><b>Via Command Line:</b> Use shell quoting. Example: {@code --options
   *       '{"min-input-files":"2"}'}
   *   <li><b>SQL Single Quotes:</b> Use as-is in where clauses. Example: {@code --where "status =
   *       'active'"} - Single quotes are automatically escaped for SQL.
   *   <li><b>JSON Values:</b> Must be valid JSON strings. The job parses and validates JSON before
   *       use.
   * </ul>
   *
   * <p>Example via command line: --catalog iceberg_catalog --table db.sample --strategy binpack
   * --options '{"min-input-files":"2"}' --spark-conf '{"spark.sql.shuffle.partitions":"200"}'
   *
   * <p>Example via Gravitino API:
   *
   * <pre>{@code
   * Map<String, String> jobConf = new HashMap<>();
   * jobConf.put("catalog_name", "iceberg_catalog");
   * jobConf.put("table_identifier", "db.sample");
   * jobConf.put("options", "{\"min-input-files\":\"2\"}");  // No shell escaping needed
   * jobConf.put("where_clause", "year = 2024 and status = 'active'");  // SQL quotes handled
   * metalake.runJob("builtin-iceberg-rewrite-data-files", jobConf);
   * }</pre>
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      printUsage();
      System.exit(1);
    }

    // Parse named arguments
    Map<String, String> argMap = IcebergJobUtils.parseArguments(args);

    // Validate required arguments
    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");

    if (catalogName == null || tableIdentifier == null) {
      System.err.println("Error: --catalog and --table are required arguments");
      printUsage();
      System.exit(1);
    }

    // Optional arguments
    String strategy = argMap.get("strategy");
    String sortOrder = argMap.get("sort-order");
    String whereClause = argMap.get("where");
    String optionsJson = argMap.get("options");
    String sparkConfJson = argMap.get("spark-conf");

    // Validate strategy if provided
    try {
      validateStrategy(strategy);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      printUsage();
      System.exit(1);
    }

    // Build Spark session with custom configs if provided
    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Rewrite Data Files");

    // Apply custom Spark configurations if provided
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      try {
        Map<String, String> customConfigs = IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson);
        for (Map.Entry<String, String> entry : customConfigs.entrySet()) {
          sparkBuilder.config(entry.getKey(), entry.getValue());
        }
      } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        printUsage();
        System.exit(1);
      }
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);

    try {
      List<Row> results =
          execute(
              spark, catalogName, tableIdentifier, strategy, sortOrder, whereClause, optionsJson);
      if (!results.isEmpty()) {
        printRewriteResults(results.get(0));
      }
      System.out.println("Rewrite data files job completed successfully");
    } catch (Exception e) {
      System.err.println("Error executing rewrite data files job: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      spark.stop();
    }
  }

  private static void printRewriteResults(Row result) {
    // Iceberg 1.6.1 returns 4 columns, newer versions may return 5
    if (result.size() >= 5) {
      System.out.printf(
          "Rewrite Data Files Results:%n"
              + "  Rewritten data files: %d%n"
              + "  Added data files: %d%n"
              + "  Rewritten bytes: %d%n"
              + "  Failed data files: %d%n"
              + "  Removed delete files: %d%n",
          result.getInt(0),
          result.getInt(1),
          result.getLong(2),
          result.getInt(3),
          result.getInt(4));
    } else {
      System.out.printf(
          "Rewrite Data Files Results:%n"
              + "  Rewritten data files: %d%n"
              + "  Added data files: %d%n"
              + "  Rewritten bytes: %d%n"
              + "  Failed data files: %d%n",
          result.getInt(0), result.getInt(1), result.getLong(2), result.getInt(3));
    }
  }

  /**
   * Build the SQL CALL statement for the rewrite_data_files procedure.
   *
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier Fully qualified table name
   * @param strategy Rewrite strategy (binpack or sort)
   * @param sortOrder Sort order specification
   * @param whereClause Filter predicate
   * @param optionsJson JSON map of options
   * @return SQL CALL statement
   */
  static String buildProcedureCall(
      String catalogName,
      String tableIdentifier,
      String strategy,
      String sortOrder,
      String whereClause,
      String optionsJson) {

    StringBuilder sql = new StringBuilder();
    sql.append("CALL ")
        .append(IcebergJobUtils.escapeSqlIdentifier(catalogName))
        .append(".system.rewrite_data_files(");
    sql.append("table => '").append(IcebergJobUtils.escapeSqlString(tableIdentifier)).append("'");

    if (strategy != null && !strategy.isEmpty()) {
      sql.append(", strategy => '").append(IcebergJobUtils.escapeSqlString(strategy)).append("'");
    }

    if (sortOrder != null && !sortOrder.isEmpty()) {
      sql.append(", sort_order => '")
          .append(IcebergJobUtils.escapeSqlString(sortOrder))
          .append("'");
    }

    if (whereClause != null && !whereClause.isEmpty()) {
      sql.append(", where => '").append(IcebergJobUtils.escapeSqlString(whereClause)).append("'");
    }

    if (optionsJson != null && !optionsJson.isEmpty()) {
      // Parse JSON and convert to map syntax for Iceberg procedure
      Map<String, String> options = parseOptionsJson(optionsJson);
      if (!options.isEmpty()) {
        sql.append(", options => map(");
        boolean first = true;
        for (Map.Entry<String, String> entry : options.entrySet()) {
          if (!first) {
            sql.append(", ");
          }
          sql.append("'")
              .append(IcebergJobUtils.escapeSqlString(entry.getKey()))
              .append("', '")
              .append(IcebergJobUtils.escapeSqlString(entry.getValue()))
              .append("'");
          first = false;
        }
        sql.append(")");
      }
    }

    sql.append(")");
    return sql.toString();
  }

  /** Delegates to {@link IcebergJobUtils#escapeSqlString(String)}. */
  static String escapeSqlString(String value) {
    return IcebergJobUtils.escapeSqlString(value);
  }

  /** Delegates to {@link IcebergJobUtils#escapeSqlIdentifier(String)}. */
  static String escapeSqlIdentifier(String identifier) {
    return IcebergJobUtils.escapeSqlIdentifier(identifier);
  }

  /** Delegates to {@link IcebergJobUtils#parseArguments(String[])}. */
  static Map<String, String> parseArguments(String[] args) {
    return IcebergJobUtils.parseArguments(args);
  }

  /**
   * Validate the strategy parameter value.
   *
   * @param strategy the strategy value to validate
   * @throws IllegalArgumentException if the strategy is invalid
   */
  static void validateStrategy(String strategy) {
    if (strategy == null || strategy.isEmpty()) {
      return; // Strategy is optional
    }

    if (!STRATEGY_BINPACK.equals(strategy) && !STRATEGY_SORT.equals(strategy)) {
      throw new IllegalArgumentException(
          "Invalid strategy '"
              + strategy
              + "'. Valid values are: '"
              + STRATEGY_BINPACK
              + "', '"
              + STRATEGY_SORT
              + "'");
    }
  }

  /** Delegates to {@link IcebergJobUtils#parseCustomSparkConfigs(String)}. */
  static Map<String, String> parseCustomSparkConfigs(String sparkConfJson) {
    return IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson);
  }

  /** Print usage information. */
  private static void printUsage() {
    System.err.println(
        "Usage: IcebergRewriteDataFilesJob [OPTIONS]\n"
            + "\n"
            + "Required Options:\n"
            + "  --catalog <name>          Iceberg catalog name registered in Spark\n"
            + "  --table <identifier>      Fully qualified table name (e.g., db.table_name)\n"
            + "\n"
            + "Optional Options:\n"
            + "  --strategy <name>         Rewrite strategy: binpack (default) or sort\n"
            + "  --sort-order <spec>       Sort order specification:\n"
            + "                              For columns: 'id DESC NULLS LAST, name ASC'\n"
            + "                              For Z-Order: 'zorder(c1,c2,c3)'\n"
            + "  --where <predicate>       Filter predicate to select files\n"
            + "                              Example: 'year = 2024 and status = ''active'''\n"
            + "  --options <json>          JSON map of Iceberg rewrite options\n"
            + "                              Example: '{\"min-input-files\":\"2\"}'\n"
            + "  --spark-conf <json>       JSON map of custom Spark configurations\n"
            + "                              Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'\n"
            + "                              Note: Cannot override catalog, extensions, or app name configs\n"
            + "\n"
            + "Examples:\n"
            + "  # Basic binpack\n"
            + "  --catalog iceberg_prod --table db.sample\n"
            + "\n"
            + "  # Sort by columns\n"
            + "  --catalog iceberg_prod --table db.sample --strategy sort \\\n"
            + "    --sort-order 'id DESC NULLS LAST'\n"
            + "\n"
            + "  # With filter and options\n"
            + "  --catalog iceberg_prod --table db.sample --where 'year = 2024 and status = ''active''' \\\n"
            + "    --options '{\"min-input-files\":\"2\",\"remove-dangling-deletes\":\"true\"}'\n"
            + "\n"
            + "  # With custom Spark configurations\n"
            + "  --catalog iceberg_prod --table db.sample --strategy binpack \\\n"
            + "    --spark-conf '{\"spark.sql.shuffle.partitions\":\"200\",\"spark.executor.memory\":\"4g\"}'");
  }

  /**
   * Parse options from a flat JSON map.
   *
   * <p>Parse errors name the real CLI flag {@code --options}. Nested objects/arrays are rejected.
   *
   * @param optionsJson JSON string
   * @return map of option keys to values
   */
  static Map<String, String> parseOptionsJson(String optionsJson) {
    return IcebergSparkConfigUtils.parseFlatJsonMap(optionsJson, "options");
  }

  /**
   * Build template arguments list with named argument format.
   *
   * @return list of template arguments
   */
  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--strategy",
        "{{strategy:-binpack}}",
        "--sort-order",
        "{{sort_order:-}}",
        // No default on purpose: an empty where clause rewrites the whole table, so callers must
        // pass it explicitly, as "" to target the whole table.
        "--where",
        "{{where_clause}}",
        "--options",
        "{{options:-}}",
        "--spark-conf",
        "{{spark_conf:-}}");
  }

  /**
   * Build Spark configuration template.
   *
   * @return map of Spark configuration keys to template values
   */
  private static Map<String, String> buildSparkConfigs() {
    return IcebergSparkConfigUtils.buildTemplateSparkConfigs();
  }

  /**
   * Runs {@code rewrite_data_files} on an existing Spark session. Shared by CLI {@link #main} and
   * TMS {@link #executeAndCollectAfter}.
   *
   * @return procedure result rows
   */
  static List<Row> execute(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String strategy,
      @Nullable String sortOrder,
      @Nullable String whereClause,
      @Nullable String optionsJson) {
    validateStrategy(strategy);
    String sql =
        buildProcedureCall(
            catalogName, tableIdentifier, strategy, sortOrder, whereClause, optionsJson);
    LOG.info("Executing Iceberg rewrite_data_files: {}", sql);
    return spark.sql(sql).collectAsList();
  }

  @Override
  public boolean hasBeforeMetrics() {
    return true;
  }

  @Override
  public TableSnapshotMetrics collectBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    long snapshotId =
        IcebergMaintenanceSnapshots.requireCurrentId(spark, catalogName, tableIdentifier);
    Map<String, Object> payload = collectCompactionMetrics(spark, catalogName, tableIdentifier);
    return new TableSnapshotMetrics(snapshotId, IcebergMaintenanceMetrics.toJson(payload));
  }

  @Override
  public boolean shouldCheckSnapshotDrift() {
    return true;
  }

  @Override
  public TableSnapshotMetrics executeAndCollectAfter(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argsByName,
      @Nullable TableSnapshotMetrics before) {
    execute(
        spark,
        catalogName,
        tableIdentifier,
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("strategy")),
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("sort-order")),
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("where")),
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("options")));
    return new TableSnapshotMetrics(
        IcebergMaintenanceSnapshots.requireCurrentId(spark, catalogName, tableIdentifier),
        IcebergMaintenanceMetrics.toJson(
            collectCompactionMetrics(spark, catalogName, tableIdentifier)));
  }

  // -------------------------------------------------------------------------
  // TMS metrics helpers (not used by CLI {@link #main})
  // -------------------------------------------------------------------------

  /**
   * Collects compaction inventory metrics for TMS before/after reporting. Uses the same {@code
   * files} metadata SQL as {@link IcebergUpdateStatsAndMetricsJob}. CLI entry points never call
   * this.
   */
  @VisibleForTesting
  static Map<String, Object> collectCompactionMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    boolean partitioned =
        IcebergUpdateStatsAndMetricsJob.isPartitionedTable(spark, catalogName, tableIdentifier);
    if (partitioned) {
      String sql =
          IcebergUpdateStatsAndMetricsJob.buildPartitionStatsSql(catalogName, tableIdentifier);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      return aggregatePartitionCompactionMetrics(rows);
    }

    String sql = IcebergUpdateStatsAndMetricsJob.buildTableStatsSql(catalogName, tableIdentifier);
    Row[] rows = (Row[]) spark.sql(sql).collect();
    if (rows.length == 0) {
      return compactionMetrics(0D, 0L, 0D, 0L, 0L);
    }
    Row row = rows[0];
    return compactionMetrics(
        numberAsDouble(row, "data-file-mse"),
        numberAsLong(row, "delete-file-number"),
        numberAsDouble(row, "avg-file-size"),
        numberAsLong(row, "total-file-size"),
        numberAsLong(row, "file-number"));
  }

  @VisibleForTesting
  static Map<String, Object> aggregatePartitionCompactionMetrics(Row[] rows) {
    double maxMse = 0D;
    long deleteFiles = 0L;
    long fileNumber = 0L;
    long totalFileSize = 0L;
    double weightedAvgNumerator = 0D;
    for (Row row : rows) {
      maxMse = Math.max(maxMse, numberAsDouble(row, "data-file-mse"));
      deleteFiles += numberAsLong(row, "delete-file-number");
      long partitionFiles = numberAsLong(row, "file-number");
      fileNumber += partitionFiles;
      totalFileSize += numberAsLong(row, "total-file-size");
      weightedAvgNumerator += numberAsDouble(row, "avg-file-size") * partitionFiles;
    }
    double avgFileSize = fileNumber == 0L ? 0D : weightedAvgNumerator / fileNumber;
    return compactionMetrics(maxMse, deleteFiles, avgFileSize, totalFileSize, fileNumber);
  }

  @VisibleForTesting
  static Map<String, Object> compactionMetrics(
      double dataFileMse,
      long deleteFileNumber,
      double avgFileSize,
      long totalFileSize,
      long fileNumber) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, dataFileMse);
    metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, deleteFileNumber);
    metrics.put(IcebergDataCompactionContent.AVG_FILE_SIZE_METRIC, avgFileSize);
    metrics.put(IcebergDataCompactionContent.TOTAL_FILE_SIZE_METRIC, totalFileSize);
    metrics.put(IcebergDataCompactionContent.FILE_NUMBER_METRIC, fileNumber);
    return metrics;
  }

  private static double numberAsDouble(Row row, String field) {
    Number number = row.getAs(field);
    return number == null ? 0D : number.doubleValue();
  }

  private static long numberAsLong(Row row, String field) {
    Number number = row.getAs(field);
    return number == null ? 0L : number.longValue();
  }
}
