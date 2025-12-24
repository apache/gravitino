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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db");
      spark.stop();
    }
  }

  @Test
  public void testBuildProcedureCallGeneratesValidSQL() {
    String sql =
        IcebergRewriteDataFilesJob.buildProcedureCall(
            catalogName, "db.test_table", null, null, null, null);

    assertNotNull(sql);
    assertTrue(sql.startsWith("CALL " + catalogName + ".system.rewrite_data_files("));
    assertTrue(sql.contains("table => 'db.test_table'"));
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
}
