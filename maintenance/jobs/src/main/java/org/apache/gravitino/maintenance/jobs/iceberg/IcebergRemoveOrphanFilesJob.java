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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Removes unreferenced Iceberg files after validating the requested scan location. */
public class IcebergRemoveOrphanFilesJob implements BuiltInJob {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRemoveOrphanFilesJob.class);
  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-remove-orphan-files";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in Iceberg orphan file cleanup job template")
        .withExecutable(resolveExecutable(IcebergRemoveOrphanFilesJob.class))
        .withClassName(IcebergRemoveOrphanFilesJob.class.getName())
        .withArguments(
            Arrays.asList(
                "--catalog",
                "{{catalog_name}}",
                "--table",
                "{{table_identifier}}",
                "--older-than",
                "{{older_than}}",
                "--location",
                "{{location}}",
                "--dry-run",
                "{{dry_run}}",
                "--spark-conf",
                "{{spark_conf}}"))
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, "v1"))
        .build();
  }

  /**
   * Runs orphan file cleanup using named arguments.
   *
   * <p>Required: {@code --catalog name --table db.table}. Optional: {@code --older-than 'yyyy-MM-dd
   * HH:mm:ss'}, {@code --location path}, {@code --dry-run true|false}, and {@code --spark-conf
   * json}. The cutoff defaults to three days ago and dry-run defaults to false. Iceberg's minimum
   * retention interval is preserved. A custom location must be within the table.
   *
   * @param args named command-line arguments
   * @throws IOException if a location cannot be validated
   * @throws AnalysisException if the table identifier is invalid or the table does not exist
   */
  public static void main(String[] args) throws IOException, AnalysisException {
    Map<String, String> options = IcebergJobUtils.parseArguments(args);
    requireOption(options, "catalog");
    requireOption(options, "table");
    parseDryRun(options.get("dry-run"));
    SparkSession.Builder builder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Remove Orphan Files");
    IcebergJobUtils.parseCustomSparkConfigs(options.get("spark-conf")).forEach(builder::config);
    try (SparkSession spark = builder.getOrCreate()) {
      execute(spark, options);
    }
  }

  static long execute(SparkSession spark, Map<String, String> options)
      throws IOException, AnalysisException {
    String catalog = requireOption(options, "catalog");
    String identifier = requireOption(options, "table");
    boolean dryRun = parseDryRun(options.get("dry-run"));
    // Backslash escapes in string literals must retain Spark's default interpretation.
    Preconditions.checkArgument(
        !Boolean.parseBoolean(spark.conf().get("spark.sql.parser.escapedStringLiterals", "false")),
        "spark.sql.parser.escapedStringLiterals must be false");
    Table table =
        Spark3Util.loadIcebergTable(
            spark, IcebergJobUtils.escapeSqlIdentifier(catalog) + "." + identifier);
    String location = options.getOrDefault("location", table.location());
    validateLocation(table.location(), location);
    validateLocalLocation(table.location(), location);
    validateRemoteLocation(spark, table.location(), location);
    String sql =
        buildProcedureCall(
            catalog,
            identifier,
            options.get("older-than"),
            normalizeLocation(location).toString(),
            dryRun);
    Iterator<Row> results = spark.sql(sql).toLocalIterator();
    long count = 0;
    while (results.hasNext()) {
      Row row = results.next();
      if (dryRun) {
        LOG.info("Orphan file (dry-run): {}", row.getString(0));
      }
      count++;
    }
    LOG.info(
        "Orphan file cleanup completed: {} files {}, table {}.{}",
        count,
        dryRun ? "found (dry-run)" : "removed",
        catalog,
        identifier);
    return count;
  }

  static String buildProcedureCall(
      String catalog,
      String table,
      @Nullable String olderThan,
      @Nullable String location,
      boolean dryRun) {
    StringBuilder sql =
        new StringBuilder("CALL ")
            .append(IcebergJobUtils.escapeSqlIdentifier(catalog))
            .append(".system.remove_orphan_files(table => ")
            .append(stringLiteral(table));
    if (olderThan != null && !olderThan.isEmpty()) {
      sql.append(", older_than => TIMESTAMP ").append(stringLiteral(olderThan));
    }
    if (location != null && !location.isEmpty()) {
      sql.append(", location => ").append(stringLiteral(location));
    }
    return sql.append(", dry_run => ").append(dryRun).append(")").toString();
  }

  static boolean parseDryRun(@Nullable String value) {
    Preconditions.checkArgument(
        value == null || "false".equals(value) || "true".equals(value),
        "--dry-run must be true or false");
    return "true".equals(value);
  }

  static void validateLocation(String tableLocation, String location) {
    URI root = normalizeLocation(tableLocation);
    URI requested = normalizeLocation(location);
    String rootPath = root.getPath().replaceAll("/+$", "");
    String childPath = requested.getPath().replaceAll("/+$", "");
    boolean sameStorage =
        Objects.equals(root.getScheme(), requested.getScheme())
            && Objects.equals(root.getAuthority(), requested.getAuthority());
    Preconditions.checkArgument(
        sameStorage
            && (childPath.equals(rootPath)
                || childPath.startsWith(rootPath.endsWith("/") ? rootPath : rootPath + "/")),
        "location must be within the table's storage location: %s",
        tableLocation);
  }

  private static URI normalizeLocation(String value) {
    // Reject ambiguous encoded paths rather than allowing different filesystem decoders to
    // interpret the containment check and the subsequent listing differently.
    Preconditions.checkArgument(
        !value.isEmpty() && !value.contains("%") && !value.contains("\\"),
        "Invalid scan location: %s",
        value);
    URI uri = URI.create(value);
    Preconditions.checkArgument(
        uri.getQuery() == null
            && uri.getFragment() == null
            && uri.getPath() != null
            && uri.getPath().startsWith("/"),
        "Scan location must be an absolute path without query or fragment: %s",
        value);
    if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
      return (uri.getScheme() == null ? Paths.get(value) : Paths.get(uri)).normalize().toUri();
    }
    return uri.normalize();
  }

  private static void validateLocalLocation(String tableLocation, String location)
      throws IOException {
    URI requested = normalizeLocation(location);
    if (!"file".equals(requested.getScheme())) {
      return;
    }
    Path lexicalRoot = Paths.get(normalizeLocation(tableLocation));
    Path root = lexicalRoot.toRealPath();
    Path scan = Paths.get(requested);
    Preconditions.checkArgument(
        scan.toRealPath().startsWith(root),
        "Scan location resolves outside the table's storage location");
    for (Path ancestor = scan;
        ancestor != null && ancestor.startsWith(lexicalRoot);
        ancestor = ancestor.getParent()) {
      Preconditions.checkArgument(
          !Files.isSymbolicLink(ancestor), "Symlinks are not allowed in the scan location");
    }
    // Do not follow symlinks during validation. Iceberg must never list another table through one.
    try (Stream<Path> paths = Files.walk(scan)) {
      Preconditions.checkArgument(
          paths.noneMatch(Files::isSymbolicLink), "Symlinks are not allowed in the scan location");
    }
  }

  private static void validateRemoteLocation(
      SparkSession spark, String tableLocation, String location) throws IOException {
    if ("file".equals(normalizeLocation(location).getScheme())) {
      return;
    }
    RemoteLocationValidator.validate(
        spark.sparkContext().hadoopConfiguration(), tableLocation, location);
  }

  private static String requireOption(Map<String, String> options, String key) {
    String value = options.get(key);
    Preconditions.checkArgument(value != null && !value.trim().isEmpty(), "--%s is required", key);
    return value;
  }

  private static String stringLiteral(String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }
}
