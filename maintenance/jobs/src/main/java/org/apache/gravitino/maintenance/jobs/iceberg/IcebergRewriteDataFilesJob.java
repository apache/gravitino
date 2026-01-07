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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
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
   * <p>Uses named arguments for flexibility: --catalog &lt;catalog_name&gt; Required. Iceberg
   * catalog name --table &lt;table_identifier&gt; Required. Table name (db.table) --strategy
   * &lt;strategy&gt; Optional. binpack or sort --sort-order &lt;sort_order&gt; Optional. Sort order
   * specification --where &lt;where_clause&gt; Optional. Filter predicate --options
   * &lt;options_json&gt; Optional. JSON map of options --spark-conf &lt;spark_conf_json&gt;
   * Optional. JSON map of custom Spark configurations
   *
   * <p>Example: --catalog iceberg_catalog --table db.sample --strategy binpack --options
   * '{"min-input-files":"2"}' --spark-conf '{"spark.sql.shuffle.partitions":"200"}'
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      printUsage();
      System.exit(1);
    }

    // Parse named arguments
    Map<String, String> argMap = parseArguments(args);

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
        Map<String, String> customConfigs = parseCustomSparkConfigs(sparkConfJson);
        validateSparkConfigs(customConfigs);
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
        .append(escapeSqlIdentifier(catalogName))
        .append(".system.rewrite_data_files(");
    sql.append("table => '").append(escapeSqlString(tableIdentifier)).append("'");

    if (strategy != null && !strategy.isEmpty()) {
      sql.append(", strategy => '").append(escapeSqlString(strategy)).append("'");
    }

    if (sortOrder != null && !sortOrder.isEmpty()) {
      sql.append(", sort_order => '").append(escapeSqlString(sortOrder)).append("'");
    }

    if (whereClause != null && !whereClause.isEmpty()) {
      sql.append(", where => '").append(escapeSqlString(whereClause)).append("'");
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
              .append(escapeSqlString(entry.getKey()))
              .append("', '")
              .append(escapeSqlString(entry.getValue()))
              .append("'");
          first = false;
        }
        sql.append(")");
      }
    }

    sql.append(")");
    return sql.toString();
  }

  /**
   * Escape single quotes in SQL string literals by replacing ' with ''.
   *
   * @param value the string value to escape
   * @return escaped string safe for use in SQL string literals
   */
  static String escapeSqlString(String value) {
    if (value == null) {
      return null;
    }
    return value.replace("'", "''");
  }

  /**
   * Escape SQL identifiers by replacing backticks and validating format.
   *
   * @param identifier the SQL identifier to escape
   * @return escaped identifier safe for use in SQL
   */
  static String escapeSqlIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    // Replace backticks to prevent breaking out of identifier quotes
    return identifier.replace("`", "``");
  }

  /**
   * Parse command line arguments in --key value format.
   *
   * @param args command line arguments
   * @return map of argument names to values
   */
  static Map<String, String> parseArguments(String[] args) {
    Map<String, String> argMap = new HashMap<>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2); // Remove "--" prefix

        // Check if there's a value for this key
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          String value = args[i + 1];
          // Only add non-empty values
          if (value != null && !value.trim().isEmpty()) {
            argMap.put(key, value);
          }
          i++; // Skip the value in next iteration
        } else {
          System.err.println("Warning: Flag " + args[i] + " has no value, ignoring");
        }
      }
    }

    return argMap;
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

  /**
   * Parse custom Spark configurations from JSON string.
   *
   * @param sparkConfJson JSON string containing Spark configurations
   * @return map of Spark configuration keys to values
   * @throws IllegalArgumentException if JSON parsing fails
   */
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
        String key = entry.getKey();
        Object value = entry.getValue();
        configs.put(key, value == null ? "" : value.toString());
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

  /**
   * Validate custom Spark configurations to prevent dangerous overrides.
   *
   * <p>This method checks that users don't override critical configurations that could break the
   * job or cause security issues.
   *
   * @param configs map of Spark configurations to validate
   * @throws IllegalArgumentException if any configuration is not allowed
   */
  static void validateSparkConfigs(Map<String, String> configs) {
    // Define configurations that should not be overridden by users
    // These are critical for the job to function correctly
    String[] forbiddenPrefixes = {
      "spark.sql.catalog.", // Catalog configs are managed by the template
      "spark.sql.extensions", // Extensions must include Iceberg extensions
      "spark.app.name" // App name is set by the job
    };

    for (String configKey : configs.keySet()) {
      // Check if config key starts with any forbidden prefix
      for (String forbiddenPrefix : forbiddenPrefixes) {
        if (configKey.startsWith(forbiddenPrefix)) {
          throw new IllegalArgumentException(
              "Configuration '"
                  + configKey
                  + "' cannot be overridden. "
                  + "Configs starting with '"
                  + forbiddenPrefix
                  + "' are managed by the job template.");
        }
      }

      // Validate that config keys follow Spark naming convention
      if (!configKey.startsWith("spark.")) {
        throw new IllegalArgumentException(
            "Invalid Spark configuration key '"
                + configKey
                + "'. All Spark configs must start with 'spark.'");
      }
    }
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
   * Parse options from JSON string using Jackson for robust parsing.
   *
   * <p>Expected format: {"key1": "value1", "key2": "value2"}
   *
   * <p>This method uses Jackson ObjectMapper to properly handle:
   *
   * <ul>
   *   <li>Escaped quotes in values
   *   <li>Colons and commas in values
   *   <li>Complex JSON structures
   *   <li>Various data types (strings, numbers, booleans)
   * </ul>
   *
   * @param optionsJson JSON string
   * @return map of option keys to values
   */
  static Map<String, String> parseOptionsJson(String optionsJson) {
    Map<String, String> options = new HashMap<>();
    if (optionsJson == null || optionsJson.isEmpty()) {
      return options;
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      // Parse JSON into a Map<String, Object> to handle various value types
      Map<String, Object> parsedMap =
          mapper.readValue(optionsJson, new TypeReference<Map<String, Object>>() {});

      // Convert all values to strings
      for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        // Convert value to string - handles strings, numbers, booleans, etc.
        options.put(key, value == null ? "" : value.toString());
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse options JSON: " + optionsJson + ". Error: " + e.getMessage(), e);
    }

    return options;
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
        "{{strategy}}",
        "--sort-order",
        "{{sort_order}}",
        "--where",
        "{{where_clause}}",
        "--options",
        "{{options}}",
        "--spark-conf",
        "{{spark_conf}}");
  }

  /**
   * Build Spark configuration template.
   *
   * @return map of Spark configuration keys to template values
   */
  private static Map<String, String> buildSparkConfigs() {
    Map<String, String> configs = new HashMap<>();

    // Spark runtime configs
    configs.put("spark.master", "{{spark_master}}");
    configs.put("spark.executor.instances", "{{spark_executor_instances}}");
    configs.put("spark.executor.cores", "{{spark_executor_cores}}");
    configs.put("spark.executor.memory", "{{spark_executor_memory}}");
    configs.put("spark.driver.memory", "{{spark_driver_memory}}");

    // Iceberg catalog configuration
    configs.put("spark.sql.catalog.{{catalog_name}}", "org.apache.iceberg.spark.SparkCatalog");
    configs.put("spark.sql.catalog.{{catalog_name}}.type", "{{catalog_type}}");
    configs.put("spark.sql.catalog.{{catalog_name}}.uri", "{{catalog_uri}}");
    configs.put("spark.sql.catalog.{{catalog_name}}.warehouse", "{{warehouse_location}}");

    // Iceberg extensions
    configs.put(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    return Collections.unmodifiableMap(configs);
  }
}
