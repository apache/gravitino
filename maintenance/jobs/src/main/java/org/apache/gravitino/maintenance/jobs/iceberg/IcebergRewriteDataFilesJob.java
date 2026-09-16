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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Built-in job for rewriting Iceberg table data files.
 *
 * <p>This job leverages Iceberg's RewriteDataFilesProcedure to optimize data file layout through
 * binpack or sort strategies. Z-order is a type of sort order, not a strategy.
 */
public class IcebergRewriteDataFilesJob implements BuiltInJob {

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-rewrite-data-files";
  private static final String VERSION = "v2";

  // Valid strategy values for Iceberg rewrite_data_files procedure
  private static final String STRATEGY_BINPACK = "binpack";
  private static final String STRATEGY_SORT = "sort";
  private static final String OPTION_STRATEGY = "strategy";
  private static final String OPTION_SORT_ORDER = "sort-order";
  private static final String OPTION_WHERE = "where-clause";
  private static final String OPTION_OPTIONS = "options";

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
   *   <li>--catalog-name &lt;catalog_name&gt; Required. Iceberg catalog name.
   *   <li>--table-identifier &lt;table_identifier&gt; Required. Table name (db.table)
   *   <li>--strategy &lt;strategy&gt; Optional. binpack or sort
   *   <li>--sort-order &lt;sort_order&gt; Optional. Sort order specification
   *   <li>--where-clause &lt;where_clause&gt; Optional. Filter predicate
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
   *   <li><b>SQL Single Quotes:</b> Use as-is in where clauses. Example: {@code --where-clause
   *       "status = 'active'"} - Single quotes are automatically escaped for SQL.
   *   <li><b>JSON Values:</b> Must be valid JSON strings. The job parses and validates JSON before
   *       use.
   * </ul>
   *
   * <p>Example via command line: --catalog-name iceberg_catalog --table-identifier db.sample
   * --strategy binpack --options '{"min-input-files":"2"}' --spark-conf
   * '{"spark.sql.shuffle.partitions":"200"}'
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
    String catalogName = IcebergJobUtils.trimToNull(argMap.get(IcebergJobUtils.OPTION_CATALOG));
    String tableIdentifier = IcebergJobUtils.trimToNull(argMap.get(IcebergJobUtils.OPTION_TABLE));

    if (catalogName == null || tableIdentifier == null) {
      System.err.println(
          "Error: --"
              + IcebergJobUtils.OPTION_CATALOG
              + " and --"
              + IcebergJobUtils.OPTION_TABLE
              + " are required arguments");
      printUsage();
      System.exit(1);
    }

    // Optional arguments
    String strategy = IcebergJobUtils.trimToNull(argMap.get(OPTION_STRATEGY));
    String sortOrder = IcebergJobUtils.trimToNull(argMap.get(OPTION_SORT_ORDER));
    String whereClause = IcebergJobUtils.trimToNull(argMap.get(OPTION_WHERE));
    String optionsJson = IcebergJobUtils.trimToNull(argMap.get(OPTION_OPTIONS));
    String sparkConfJson =
        IcebergJobUtils.trimToNull(argMap.get(IcebergJobUtils.OPTION_SPARK_CONF));

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
        System.out.println("Applied custom Spark configurations: " + customConfigs);
      } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        printUsage();
        System.exit(1);
      }
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);

    try {
      // Build the procedure call SQL
      String sql =
          buildProcedureCall(
              catalogName, tableIdentifier, strategy, sortOrder, whereClause, optionsJson);

      System.out.println("Executing Iceberg rewrite_data_files procedure: " + sql);

      // Execute the procedure
      Row[] results = (Row[]) spark.sql(sql).collect();

      // Print results
      if (results.length > 0) {
        Row result = results[0];
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

      System.out.println("Rewrite data files job completed successfully");
    } catch (Exception e) {
      System.err.println("Error executing rewrite data files job: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      spark.stop();
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
            + "  --"
            + IcebergJobUtils.OPTION_CATALOG
            + " <name>          Iceberg catalog name registered in Spark\n"
            + "  --"
            + IcebergJobUtils.OPTION_TABLE
            + " <identifier>      Fully qualified table name (e.g., db.table_name)\n"
            + "\n"
            + "Optional Options:\n"
            + "  --"
            + OPTION_STRATEGY
            + " <name>         Rewrite strategy: binpack (default) or sort\n"
            + "  --"
            + OPTION_SORT_ORDER
            + " <spec>       Sort order specification:\n"
            + "                              For columns: 'id DESC NULLS LAST, name ASC'\n"
            + "                              For Z-Order: 'zorder(c1,c2,c3)'\n"
            + "  --"
            + OPTION_WHERE
            + " <predicate>       Filter predicate to select files\n"
            + "                              Example: 'year = 2024 and status = ''active'''\n"
            + "  --"
            + OPTION_OPTIONS
            + " <json>          JSON map of Iceberg rewrite options\n"
            + "                              Example: '{\"min-input-files\":\"2\"}'\n"
            + "  --"
            + IcebergJobUtils.OPTION_SPARK_CONF
            + " <json>       JSON map of custom Spark configurations\n"
            + "                              Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'\n"
            + "                              Note: Cannot override catalog, extensions, or app name configs\n"
            + "\n"
            + "Examples:\n"
            + "  # Basic binpack\n"
            + "  --"
            + IcebergJobUtils.OPTION_CATALOG
            + " iceberg_prod --"
            + IcebergJobUtils.OPTION_TABLE
            + " db.sample\n"
            + "\n"
            + "  # Sort by columns\n"
            + "  --"
            + IcebergJobUtils.OPTION_CATALOG
            + " iceberg_prod --"
            + IcebergJobUtils.OPTION_TABLE
            + " db.sample --"
            + OPTION_STRATEGY
            + " sort \\\n"
            + "    --"
            + OPTION_SORT_ORDER
            + " 'id DESC NULLS LAST'\n"
            + "\n"
            + "  # With filter and options\n"
            + "  --"
            + IcebergJobUtils.OPTION_CATALOG
            + " iceberg_prod --"
            + IcebergJobUtils.OPTION_TABLE
            + " db.sample --"
            + OPTION_WHERE
            + " 'year = 2024 and status = ''active''' \\\n"
            + "    --"
            + OPTION_OPTIONS
            + " '{\"min-input-files\":\"2\",\"remove-dangling-deletes\":\"true\"}'\n"
            + "\n"
            + "  # With custom Spark configurations\n"
            + "  --"
            + IcebergJobUtils.OPTION_CATALOG
            + " iceberg_prod --"
            + IcebergJobUtils.OPTION_TABLE
            + " db.sample --"
            + OPTION_STRATEGY
            + " binpack \\\n"
            + "    --"
            + IcebergJobUtils.OPTION_SPARK_CONF
            + " '{\"spark.sql.shuffle.partitions\":\"200\",\"spark.executor.memory\":\"4g\"}'");
  }

  /**
   * Parse rewrite options from a flat JSON map.
   *
   * <p>Expected format: {"key1": "value1", "key2": "value2"}
   *
   * @param optionsJson JSON string
   * @return map of option keys to values
   * @throws IllegalArgumentException if JSON is invalid or not a flat map
   */
  static Map<String, String> parseOptionsJson(String optionsJson) {
    return new HashMap<>(IcebergSparkConfigUtils.parseFlatJsonMap(optionsJson, OPTION_OPTIONS));
  }

  /**
   * Build template arguments list with named argument format.
   *
   * @return list of template arguments
   */
  private static List<String> buildArguments() {
    return Arrays.asList(
        "--" + IcebergJobUtils.OPTION_CATALOG,
        "{{catalog_name}}",
        "--" + IcebergJobUtils.OPTION_TABLE,
        "{{table_identifier}}",
        "--" + OPTION_STRATEGY,
        "{{strategy}}",
        "--" + OPTION_SORT_ORDER,
        "{{sort_order}}",
        "--" + OPTION_WHERE,
        "{{where_clause}}",
        "--" + OPTION_OPTIONS,
        "{{options}}",
        "--" + IcebergJobUtils.OPTION_SPARK_CONF,
        "{{spark_conf}}");
  }

  /**
   * Build Spark configuration template.
   *
   * @return map of Spark configuration keys to template values
   */
  private static Map<String, String> buildSparkConfigs() {
    return IcebergSparkConfigUtils.buildTemplateSparkConfigs();
  }
}
