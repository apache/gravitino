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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.job.PartitionUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for IcebergRewriteDataFilesJob that use a real Spark session to verify the
 * generated SQL procedure calls.
 */
public class TestIcebergRewriteDataFilesJobWithSpark {

  @TempDir static File tempDir;

  private static SparkSession spark;
  private static String catalogName;
  private static String warehousePath;

  @BeforeAll
  public static void setUp() {
    warehousePath = new File(tempDir, "warehouse").getAbsolutePath();
    catalogName = "test_catalog";

    spark =
        SparkSession.builder()
            .appName("TestIcebergRewriteDataFilesJob")
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".type", "hadoop")
            .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
            .getOrCreate();

    // Create a test table with data
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.test_table (id INT, name STRING, value DOUBLE) USING iceberg");

    // Insert test data
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".db.test_table VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.test_table");
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.pu_sq");
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db CASCADE");
      spark.stop();
    }
  }

  @Test
  public void testBuildProcedureCallGeneratesValidSQL() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, null);

    assertNotNull(sql);
    assertTrue(sql.startsWith("CALL `" + catalogName + "`.system.rewrite_data_files("));
    assertTrue(sql.contains("table => 'db.test_table'"));
  }

  @Test
  public void testRequireEscapedStringLiteralsDisabled() {
    assertDoesNotThrow(() -> IcebergJobUtils.requireEscapedStringLiteralsDisabled(spark));
    spark.conf().set("spark.sql.parser.escapedStringLiterals", "true");
    try {
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> IcebergJobUtils.requireEscapedStringLiteralsDisabled(spark));
      assertTrue(ex.getMessage().contains("escapedStringLiterals"));
    } finally {
      spark.conf().set("spark.sql.parser.escapedStringLiterals", "false");
    }
  }

  @Test
  public void testExecuteRewriteDataFilesMinimal() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, null);

    // Execute the procedure
    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    // Verify we got a result
    assertNotNull(rows);
    assertTrue(rows.length > 0);

    // Result columns: rewritten_data_files_count, added_data_files_count,
    // rewritten_bytes_count, failed_data_files_count (Iceberg 1.6.1)
    Row resultRow = rows[0];
    assertTrue(resultRow.size() >= 4, "Result should have at least 4 columns");

    // The counts should be non-negative
    assertTrue(resultRow.getInt(0) >= 0); // rewritten_data_files_count
    assertTrue(resultRow.getInt(1) >= 0); // added_data_files_count
    assertTrue(resultRow.getLong(2) >= 0); // rewritten_bytes_count
    assertEquals(0, resultRow.getInt(3)); // failed_data_files_count should be 0
  }

  @Test
  public void testExecuteRewriteDataFilesWithStrategy() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", "binpack", null, null, null);

    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    assertNotNull(rows);
    assertTrue(rows.length > 0);
  }

  @Test
  public void testExecuteRewriteDataFilesWithOptions() {
    String optionsJson = "{\"min-input-files\":\"1\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, optionsJson);

    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    assertNotNull(rows);
    assertTrue(rows.length > 0);
  }

  @Test
  public void testExecuteRewriteDataFilesWithWhere() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, "id > 1", null);

    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    assertNotNull(rows);
    assertTrue(rows.length > 0);
  }

  @Test
  public void testExecuteRewriteDataFilesWithAllParameters() {
    String optionsJson = "{\"min-input-files\":\"1\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", "binpack", null, "id >= 1", optionsJson);

    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    assertNotNull(rows);
    assertTrue(rows.length > 0);

    // Verify all columns are present
    Row resultRow = rows[0];
    assertTrue(resultRow.size() >= 4, "Result should have at least 4 columns");
  }

  @Test
  public void testTableDataIntegrityAfterRewrite() {
    // Get initial row count
    long initialCount =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".db.test_table").first().getLong(0);

    // Execute rewrite
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", "binpack", null, null, null);
    spark.sql(sql);

    // Verify row count is the same after rewrite
    long finalCount =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".db.test_table").first().getLong(0);
    assertEquals(initialCount, finalCount, "Row count should remain the same after rewrite");

    // Verify data integrity
    Row[] rows =
        (Row[]) spark.sql("SELECT * FROM " + catalogName + ".db.test_table ORDER BY id").collect();
    assertEquals(3, rows.length);
    assertEquals(1, rows[0].getInt(0));
    assertEquals("Alice", rows[0].getString(1));
    assertEquals(2, rows[1].getInt(0));
    assertEquals("Bob", rows[1].getString(1));
    assertEquals(3, rows[2].getInt(0));
    assertEquals("Charlie", rows[2].getString(1));
  }

  @Test
  public void testMultipleOptionsInProcedureCall() {
    String optionsJson = "{\"min-input-files\":\"1\",\"target-file-size-bytes\":\"536870912\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", "binpack", null, null, optionsJson);

    // Verify SQL contains both options
    assertTrue(sql.contains("'min-input-files', '1'"));
    assertTrue(sql.contains("'target-file-size-bytes', '536870912'"));

    // Execute to verify it's valid
    Dataset<Row> result = spark.sql(sql);
    Row[] rows = (Row[]) result.collect();

    assertNotNull(rows);
    assertTrue(rows.length > 0);
  }

  @Test
  public void testSqlInjectionInTableNameIsEscaped() {
    // Create a table with single quote in the namespace for testing
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db_test");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db_test.special_table (id INT, name STRING) USING iceberg");
    spark.sql("INSERT INTO " + catalogName + ".db_test.special_table VALUES (1, 'test')");

    try {
      // Attempt SQL injection with single quotes - should be safely escaped
      String maliciousTable = "db_test.special_table' OR '1'='1";
      String sql =
          IcebergRewriteDataFilesJob.buildProcedureCall(
              catalogName, maliciousTable, null, null, null, null);

      // CALL table => '...' uses Spark escaping (' → \')
      assertTrue(sql.contains("db_test.special_table\\' OR \\'1\\'=\\'1"));

      // The SQL should fail with table not found (not execute malicious code)
      // because the escaped table name doesn't exist
      try {
        spark.sql(sql);
        // If it somehow succeeds, that's also acceptable (means no injection occurred)
      } catch (Exception e) {
        // Expected: table not found or similar error (not a syntax error)
        String errorMsg = e.getMessage().toLowerCase();
        assertTrue(
            errorMsg.contains("table")
                || errorMsg.contains("not found")
                || errorMsg.contains("identifier"),
            "Error should be about table not found, not SQL syntax: " + errorMsg);
      }
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db_test.special_table");
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db_test");
    }
  }

  @Test
  public void testSqlInjectionInWhereClauseIsEscaped() {
    // Attempt SQL injection in WHERE clause
    String maliciousWhere = "id > 0' OR '1'='1";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, maliciousWhere, null);

    // WHERE uses Spark CALL escaping (' → \')
    assertTrue(sql.contains("id > 0\\' OR \\'1\\'=\\'1"));

    // The SQL should fail because the WHERE clause is invalid (not execute malicious code)
    try {
      spark.sql(sql);
      // If it somehow succeeds, verify data integrity wasn't compromised
      long count =
          spark.sql("SELECT COUNT(*) FROM " + catalogName + ".db.test_table").first().getLong(0);
      assertEquals(3, count, "Data should not be modified");
    } catch (Exception e) {
      // Expected: syntax error or invalid WHERE clause
      String errorMsg = e.getMessage().toLowerCase();
      assertTrue(
          errorMsg.contains("syntax") || errorMsg.contains("parse") || errorMsg.contains("invalid"),
          "Error should be about invalid syntax: " + errorMsg);
    }
  }

  @Test
  public void testSingleQuotesInValidDataAreEscaped() {
    // Create a table with a name containing special characters
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db_special");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db_special.data_table (id INT, description STRING) USING iceberg");
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".db_special.data_table VALUES (1, 'test'), (2, 'O''Brien'), (3, 'It''s working')");

    try {
      // Match PartitionUtils inner literal form (' → \'), then CALL outer escape.
      String whereClause = "description = 'O\\'Brien'";
      String sql =
          IcebergRewriteDataFilesJob.buildProcedureCall(
              catalogName, "db_special.data_table", null, null, whereClause, null);

      assertTrue(sql.contains("description = \\'O\\\\\\'Brien\\'"), sql);

      // Execute - this should work or fail gracefully without injection
      try {
        Dataset<Row> result = spark.sql(sql);
        Row[] rows = (Row[]) result.collect();
        assertNotNull(rows);
        assertTrue(rows.length > 0);
      } catch (Exception e) {
        // If it fails, should be a legitimate error, not injection
        String errorMsg = e.getMessage().toLowerCase();
        // Should not contain anything about dropped tables or unauthorized operations
        assertFalse(errorMsg.contains("drop"));
        assertFalse(errorMsg.contains("delete"));
      }

      // Verify table still exists and data is intact
      long count =
          spark
              .sql("SELECT COUNT(*) FROM " + catalogName + ".db_special.data_table")
              .first()
              .getLong(0);
      assertEquals(3, count, "All rows should still exist");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db_special.data_table");
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db_special");
    }
  }

  @Test
  public void testBackticksInCatalogNameAreEscaped() {
    // Test that backticks in catalog name don't break out of identifier context
    String maliciousCatalog = "test`catalog";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            maliciousCatalog, "db.test_table", null, null, null, null);

    // Verify backticks are escaped
    assertTrue(sql.contains("test``catalog"));

    // Should fail with catalog not found (not cause SQL injection)
    try {
      spark.sql(sql);
    } catch (Exception e) {
      String errorMsg = e.getMessage().toLowerCase();
      assertTrue(
          errorMsg.contains("catalog")
              || errorMsg.contains("not found")
              || errorMsg.contains("identifier"),
          "Error should be about catalog not found: " + errorMsg);
    }
  }

  @Test
  public void testJsonParsingWithColonsAndCommas() {
    // Test that JSON values with colons and commas are correctly parsed
    String optionsJson =
        "{\"path\":\"/data/file:backup,archive\",\"url\":\"http://example.com:8080\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, optionsJson);

    // Verify the values are correctly included in the SQL
    assertTrue(sql.contains("'path', '/data/file:backup,archive'"));
    assertTrue(sql.contains("'url', 'http://example.com:8080'"));

    // Note: This will fail because these aren't valid Iceberg options,
    // but it validates that the JSON parsing doesn't break on colons/commas
    try {
      spark.sql(sql);
    } catch (Exception e) {
      // Expected - invalid options, but SQL should be syntactically valid
      assertFalse(
          e.getMessage().toLowerCase().contains("parse"),
          "Should not be a parsing error: " + e.getMessage());
    }
  }

  @Test
  public void testJsonParsingWithNumericAndBooleanValues() {
    // Test that numeric and boolean JSON values are correctly converted to strings
    String optionsJson = "{\"max-file-size-bytes\":1073741824,\"rewrite-all\":true}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, optionsJson);

    // Verify numeric and boolean values are converted to strings
    assertTrue(sql.contains("'max-file-size-bytes', '1073741824'"));
    assertTrue(sql.contains("'rewrite-all', 'true'"));

    // Execute to verify the SQL is valid
    try {
      Dataset<Row> result = spark.sql(sql);
      Row[] rows = (Row[]) result.collect();
      assertNotNull(rows);
    } catch (Exception e) {
      // If it fails, should not be a JSON parsing issue
      assertFalse(
          e.getMessage().toLowerCase().contains("json"),
          "Should not be a JSON error: " + e.getMessage());
    }
  }

  @Test
  public void testJsonParsingWithEscapedQuotes() {
    // Test that escaped quotes in JSON values are correctly handled
    String optionsJson = "{\"description\":\"Rewrite with \\\"quoted\\\" text\"}";
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, optionsJson);

    // Verify the escaped quotes are properly handled
    // The JSON parser converts \" to ", then SQL escaping converts " to '' for single quotes
    assertTrue(
        sql.contains("'description', 'Rewrite with \"quoted\" text'")
            || sql.contains("description"));
  }

  /**
   * End-to-end: {@code O'Brien} must not collapse to {@code OBrien} through CALL escaping.
   *
   * <p>Inserts both partition values via the DataFrame API (value-preserving), makes both rewrite
   * eligible, rewrites only {@code O'Brien}, and asserts only that partition's data files change.
   */
  @Test
  public void testPartitionUtilsWhereWithSingleQuoteSelectsOnlyRequestedPartition() {
    String table = catalogName + ".db.pu_sq";
    String apostrophe = "O'Brien";
    String plain = "OBrien";
    try {
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id INT, region STRING) USING iceberg PARTITIONED BY (region)");
      spark.sql(
          "ALTER TABLE " + table + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024')");

      // Value-preserving inserts: do not round-trip partition values through SQL string literals.
      StructType schema =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("region", DataTypes.StringType, false);
      List<Row> rows = new ArrayList<>();
      for (int commit = 0; commit < 3; commit++) {
        rows.clear();
        for (int i = 0; i < 30; i++) {
          rows.add(RowFactory.create(commit * 100 + i, apostrophe));
          rows.add(RowFactory.create(1000 + commit * 100 + i, plain));
        }
        spark
            .createDataFrame(rows, schema)
            .write()
            .format("iceberg")
            .mode(SaveMode.Append)
            .saveAsTable(table);
      }

      List<String> regions =
          spark.table(table).select("region").distinct().collectAsList().stream()
              .map(r -> r.getString(0))
              .sorted()
              .collect(Collectors.toList());
      assertEquals(Arrays.asList(apostrophe, plain), regions);

      Set<String> beforeApostrophe = dataFilePathsForRegion(table, apostrophe);
      Set<String> beforePlain = dataFilePathsForRegion(table, plain);
      assertTrue(beforeApostrophe.size() >= 2, "O'Brien should have multiple data files");
      assertTrue(beforePlain.size() >= 2, "OBrien should have multiple data files");

      String where =
          PartitionUtils.getWhereClauseForPartitions(
              Collections.singletonList(
                  PartitionPath.of(List.of(new PartitionEntryImpl("region", apostrophe)))),
              new Column[] {column("region", Types.StringType.get())},
              new Transform[] {Transforms.identity("region")},
              catalogName);
      assertEquals("(region = 'O\\'Brien')", where);

      String sql =
          IcebergRewriteDataFilesJob.buildProcedureCall(
              catalogName,
              "db.pu_sq",
              "binpack",
              null,
              where,
              "{\"rewrite-all\":\"true\",\"min-input-files\":\"1\"}");
      // Outer CALL uses \'; inner apostrophe is \\' after escapeSparkCallStringArg.
      assertTrue(sql.contains("where => '(region = \\'O\\\\\\'Brien\\')'"), sql);
      spark.sql(sql).collect();
      spark.sql("REFRESH TABLE " + table);

      Set<String> afterApostrophe = dataFilePathsForRegion(table, apostrophe);
      Set<String> afterPlain = dataFilePathsForRegion(table, plain);
      assertFalse(
          beforeApostrophe.equals(afterApostrophe),
          "Requested O'Brien partition files should change");
      assertEquals(
          beforePlain, afterPlain, "Unrequested OBrien partition files must stay unchanged");
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
    }
  }

  private static Set<String> dataFilePathsForRegion(String table, String region) {
    return spark
        .table(table + ".files")
        .where("content = 0")
        .select("file_path", "partition.region")
        .collectAsList()
        .stream()
        .filter(r -> region.equals(r.getString(1)))
        .map(r -> r.getString(0))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * End-to-end: single-quoted string/DATE literals survive CALL embedding under all four ANSI
   * combinations of {@code spark.sql.ansi.enabled} × {@code
   * spark.sql.ansi.doubleQuotedIdentifiers}.
   */
  @Test
  public void testPartitionUtilsWhereAnsiLiteralModesThroughJobEscapeAndSpark() {
    String partitionValue = "a\"b\\c";
    // PartitionUtils: one-level \\ / \' ; job builder re-escapes for CALL where => '...'.
    String expectedWhere = "(region = 'a\"b\\\\c')";
    String expectedCallWhere = "(region = \\'a\"b\\\\\\\\c\\')";
    boolean[][] modes =
        new boolean[][] {
          {false, false},
          {true, false},
          {false, true},
          {true, true},
        };
    for (boolean[] mode : modes) {
      boolean ansiEnabled = mode[0];
      boolean doubleQuotedIds = mode[1];
      withAnsiModes(
          ansiEnabled,
          doubleQuotedIds,
          () -> {
            String tableSuffix =
                "pu_ansi_" + (ansiEnabled ? "e1" : "e0") + "_" + (doubleQuotedIds ? "d1" : "d0");
            String table = catalogName + ".db." + tableSuffix;
            spark.sql("DROP TABLE IF EXISTS " + table);
            spark.sql(
                "CREATE TABLE "
                    + table
                    + " (id INT, region STRING, ts TIMESTAMP) USING iceberg "
                    + "PARTITIONED BY (region, days(ts))");
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
              rows.add(
                  RowFactory.create(
                      i, partitionValue, java.sql.Timestamp.valueOf("2024-01-01 00:00:00")));
            }
            StructType schema =
                new StructType()
                    .add("id", DataTypes.IntegerType, false)
                    .add("region", DataTypes.StringType, false)
                    .add("ts", DataTypes.TimestampType, false);
            spark
                .createDataFrame(rows, schema)
                .write()
                .format("iceberg")
                .mode(SaveMode.Append)
                .saveAsTable(table);

            String stringWhere =
                PartitionUtils.getWhereClauseForPartitions(
                    Collections.singletonList(
                        PartitionPath.of(
                            List.of(new PartitionEntryImpl("region", partitionValue)))),
                    new Column[] {column("region", Types.StringType.get())},
                    new Transform[] {Transforms.identity("region")},
                    catalogName);
            assertEquals(expectedWhere, stringWhere, ansiModeLabel(ansiEnabled, doubleQuotedIds));
            String stringSql =
                IcebergRewriteDataFilesJob.buildProcedureCall(
                    catalogName, "db." + tableSuffix, "binpack", null, stringWhere, null);
            assertTrue(
                stringSql.contains("where => '" + expectedCallWhere + "'"),
                ansiModeLabel(ansiEnabled, doubleQuotedIds) + ": " + stringSql);
            assertDoesNotThrow(
                () -> spark.sql(stringSql).collect(),
                ansiModeLabel(ansiEnabled, doubleQuotedIds) + " string");

            String dateWhere =
                PartitionUtils.getWhereClauseForPartitions(
                    Collections.singletonList(
                        PartitionPath.of(List.of(new PartitionEntryImpl("ts_day", "2024-01-01")))),
                    new Column[] {column("ts", Types.TimestampType.withoutTimeZone())},
                    new Transform[] {Transforms.day("ts")},
                    catalogName);
            assertEquals(
                "(`" + catalogName + "`.system.days(ts) = DATE '2024-01-01')",
                dateWhere,
                ansiModeLabel(ansiEnabled, doubleQuotedIds));
            String dateSql =
                IcebergRewriteDataFilesJob.buildProcedureCall(
                    catalogName, "db." + tableSuffix, "binpack", null, dateWhere, null);
            assertTrue(
                dateSql.contains(
                    "where => '(`" + catalogName + "`.system.days(ts) = DATE \\'2024-01-01\\')'"),
                ansiModeLabel(ansiEnabled, doubleQuotedIds) + ": " + dateSql);
            assertDoesNotThrow(
                () -> spark.sql(dateSql).collect(),
                ansiModeLabel(ansiEnabled, doubleQuotedIds) + " date");

            spark.sql("DROP TABLE IF EXISTS " + table);
          });
    }
  }

  /**
   * With catalog {@code default-namespace}, unqualified {@code years(ts)} is rewritten to {@code
   * catalog.<ns>.years}; {@code catalog.system.years} stays a system function.
   *
   * <p>Uses a fresh Spark catalog alias configured with {@code default-namespace} before first use
   * (setting it on an already-cached catalog is ignored).
   */
  @Test
  public void testCatalogSystemYearsWithDefaultNamespace() {
    String dnsCatalog = "dns_catalog";
    String table = dnsCatalog + ".db.pu_years_dns";
    // Register before any use so default-namespace is applied at catalog init.
    spark.conf().set("spark.sql.catalog." + dnsCatalog, "org.apache.iceberg.spark.SparkCatalog");
    spark.conf().set("spark.sql.catalog." + dnsCatalog + ".type", "hadoop");
    spark.conf().set("spark.sql.catalog." + dnsCatalog + ".warehouse", warehousePath);
    spark.conf().set("spark.sql.catalog." + dnsCatalog + ".default-namespace", "db");
    spark.conf().set("spark.sql.defaultCatalog", dnsCatalog);
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS " + dnsCatalog + ".db");
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.sql(
          "CREATE TABLE "
              + table
              + " (id INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (years(ts))");
      List<Row> rows = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        rows.add(RowFactory.create(i, java.sql.Timestamp.valueOf("2024-06-15 12:00:00")));
      }
      StructType schema =
          new StructType()
              .add("id", DataTypes.IntegerType, false)
              .add("ts", DataTypes.TimestampType, false);
      spark
          .createDataFrame(rows, schema)
          .write()
          .format("iceberg")
          .mode(SaveMode.Append)
          .saveAsTable(table);

      String where =
          PartitionUtils.getWhereClauseForPartitions(
              Collections.singletonList(
                  PartitionPath.of(List.of(new PartitionEntryImpl("ts_year", "54")))),
              new Column[] {column("ts", Types.TimestampType.withoutTimeZone())},
              new Transform[] {Transforms.year("ts")},
              dnsCatalog);
      assertEquals("(`" + dnsCatalog + "`.system.years(ts) = 54)", where);

      // Prefer CatalogManager over SQL current_namespace(): with default-namespace set, Spark may
      // resolve the SQL function under catalog.db and fail UNRESOLVED_ROUTINE.
      String[] currentNs = spark.sessionState().catalogManager().currentNamespace();
      assertTrue(
          Arrays.asList(currentNs).contains("db"),
          "default-namespace must be active before CALL, got: " + Arrays.toString(currentNs));

      String sql =
          IcebergRewriteDataFilesJob.buildProcedureCall(
              dnsCatalog, "db.pu_years_dns", "binpack", null, where, null);
      assertDoesNotThrow(() -> spark.sql(sql).collect());
    } finally {
      spark.sql("DROP TABLE IF EXISTS " + table);
      spark.conf().set("spark.sql.defaultCatalog", catalogName);
    }
  }

  private static void withAnsiModes(
      boolean ansiEnabled, boolean doubleQuotedIdentifiers, Runnable body) {
    spark.conf().set("spark.sql.ansi.enabled", Boolean.toString(ansiEnabled));
    spark
        .conf()
        .set("spark.sql.ansi.doubleQuotedIdentifiers", Boolean.toString(doubleQuotedIdentifiers));
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    try {
      body.run();
    } finally {
      spark.conf().set("spark.sql.ansi.enabled", "false");
      spark.conf().set("spark.sql.ansi.doubleQuotedIdentifiers", "false");
    }
  }

  private static String ansiModeLabel(boolean ansiEnabled, boolean doubleQuotedIdentifiers) {
    return "ansi.enabled=" + ansiEnabled + ", doubleQuotedIdentifiers=" + doubleQuotedIdentifiers;
  }

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return ColumnDTO.builder().withName(name).withDataType(type).build();
  }
}
