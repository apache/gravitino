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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests orphan cleanup against real Iceberg tables in local Spark. */
public class TestIcebergRemoveOrphanFilesJobWithSpark {
  @TempDir static Path tempDir;
  private static SparkSession spark;

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("TestRemoveOrphanFiles")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.test_catalog.type", "hadoop")
            .config("spark.sql.catalog.test_catalog.warehouse", tempDir.toString())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE test_catalog.db");
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /** Verifies dry run and deletion preserve live and recent files. */
  @Test
  public void testDryRunAndDeletionPreserveLiveAndRecentFiles() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.cleanup (id INT) USING iceberg");
    spark.sql("INSERT INTO test_catalog.db.cleanup VALUES (1), (2)");
    Path data = tempDir.resolve("db/cleanup/data");
    // Age referenced files too, so survival depends on metadata references, not the cutoff.
    try (Stream<Path> files = Files.list(data)) {
      for (Path file : files.collect(Collectors.toList())) {
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS)));
      }
    }
    Path old = Files.write(data.resolve("old-orphan.parquet"), new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS)));
    Path recent = Files.write(data.resolve("recent-orphan.parquet"), new byte[] {1});
    assertEquals(1, execute("cleanup", null, null, true).size());
    assertTrue(Files.exists(old));
    assertEquals(1, execute("cleanup", null, null, false).size());
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(recent));
    assertEquals(2, spark.table("test_catalog.db.cleanup").count());
    assertEquals(0, execute("cleanup", null, null, false).size());
  }

  /** Verifies TMS executeWithMetrics measures removed file size; CLI execute does not. */
  @Test
  public void testRemovedFileSizeMeasuredBeforeDelete() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.bytes (id INT) USING iceberg");
    Path root = tempDir.resolve("db/bytes");
    byte[] payload = new byte[] {1, 2, 3, 4, 5, 6, 7};
    Path old = Files.write(root.resolve("old-orphan.bin"), payload);
    Files.setLastModifiedTime(old, FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS)));
    assertEquals(1, execute("bytes", null, null, true).size());
    IcebergRemoveOrphanFilesJob.OrphanCleanupResult dry =
        executeWithMetrics("bytes", null, null, true);
    assertEquals(1, dry.filesRemoved());
    assertEquals(payload.length, dry.bytesReclaimed());
    assertTrue(Files.exists(old));
    IcebergRemoveOrphanFilesJob.OrphanCleanupResult deleted =
        executeWithMetrics("bytes", null, null, false);
    assertEquals(1, deleted.filesRemoved());
    assertEquals(payload.length, deleted.bytesReclaimed());
    assertFalse(Files.exists(old));
  }

  /** Verifies blank locations default to the table root without CLI argument parsing. */
  @Test
  public void testBlankLocationDefaultsToTableRoot() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.blank_location (id INT) USING iceberg");
    Path root = tempDir.resolve("db/blank_location");
    Path old = Files.write(root.resolve("old-orphan"), new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS)));
    for (String location : new String[] {"", " ", "\t", "\u2003"}) {
      assertEquals(1, execute("blank_location", null, location, true).size());
      assertTrue(Files.exists(old));
    }
    assertEquals(1, execute("blank_location", null, "", false).size());
    assertFalse(Files.exists(old));
  }

  /** Verifies custom location and outside rejection. */
  @Test
  public void testCustomLocationAndOutsideRejection() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.scoped (id INT) USING iceberg");
    Path root = tempDir.resolve("db/scoped");
    Path sub = Files.createDirectories(root.resolve("staged"));
    Path inside = Files.write(sub.resolve("old"), new byte[] {1});
    Path outside = Files.write(root.resolve("old"), new byte[] {1});
    FileTime old = FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS));
    Files.setLastModifiedTime(inside, old);
    Files.setLastModifiedTime(outside, old);
    assertEquals(1, execute("scoped", null, sub.toString(), false).size());
    assertFalse(Files.exists(inside));
    assertTrue(Files.exists(outside));
    assertThrows(
        IllegalArgumentException.class, () -> execute("scoped", null, tempDir.toString(), false));
    assertTrue(Files.exists(outside));
    Path link = root.resolve("escape");
    Files.createSymbolicLink(link, tempDir);
    assertThrows(
        IllegalArgumentException.class, () -> execute("scoped", null, link.toString(), false));
  }

  /** Verifies UTC cutoffs from the policy adapter retain their meaning in other session zones. */
  @Test
  public void testUtcCutoffWithNonUtcSession() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.utc_cutoff (id INT) USING iceberg");
    Path root = tempDir.resolve("db/utc_cutoff");
    Instant cutoff = Instant.now().minus(4, ChronoUnit.DAYS).truncatedTo(ChronoUnit.SECONDS);
    Path older = Files.write(root.resolve("older"), new byte[] {1});
    Path newer = Files.write(root.resolve("newer"), new byte[] {1});
    Files.setLastModifiedTime(older, FileTime.from(cutoff.minusSeconds(60)));
    Files.setLastModifiedTime(newer, FileTime.from(cutoff.plusSeconds(60)));
    String olderThan = cutoff.toString().replace("T", " ");
    String timezone = spark.conf().get("spark.sql.session.timeZone");
    spark.conf().set("spark.sql.session.timeZone", "America/Los_Angeles");
    try {
      assertEquals(1, execute("utc_cutoff", olderThan, null, false).size());
      assertFalse(Files.exists(older));
      assertTrue(Files.exists(newer));
    } finally {
      spark.conf().set("spark.sql.session.timeZone", timezone);
    }
  }

  /** Verifies invalid inputs fail before deletion. */
  @Test
  public void testInvalidInputsFailBeforeDeletion() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.invalid (id INT) USING iceberg");
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.parseDryRun("yes"));
    assertThrows(
        IOException.class,
        () -> execute("invalid", null, tempDir.resolve("db/invalid/missing").toString(), false));
    assertThrows(
        IllegalArgumentException.class,
        () -> execute("invalid", "2999-01-01 00:00:00", null, false));
    spark.conf().set("spark.sql.parser.escapedStringLiterals", "true");
    try {
      assertThrows(IllegalArgumentException.class, () -> execute("invalid", null, null, false));
    } finally {
      spark.conf().set("spark.sql.parser.escapedStringLiterals", "false");
    }
    assertEquals(1, IcebergRemoveOrphanFilesJob.run(new String[] {}));
    assertEquals(1, IcebergRemoveOrphanFilesJob.run(new String[] {"--catalog", "test_catalog"}));
    assertEquals(
        1,
        IcebergRemoveOrphanFilesJob.run(
            new String[] {
              "--catalog", "test_catalog", "--table", "db.invalid", "--dry-run", "yes"
            }));
    assertEquals(
        1,
        IcebergRemoveOrphanFilesJob.run(
            new String[] {
              "--catalog", "test_catalog", "--table", "db.invalid", "--spark-conf", "{bad}"
            }));
  }

  /** Verifies explicit cutoff and quoted table name. */
  @Test
  public void testExplicitCutoffAndQuotedTableName() throws Exception {
    Path location = tempDir.resolve("db/quo'te");
    spark.sql("CREATE TABLE test_catalog.db.`quo'te` (id INT) USING iceberg");
    Path old = Files.write(location.resolve("old"), new byte[] {1});
    Path newer = Files.write(location.resolve("newer"), new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.from(Instant.parse("2023-01-01T00:00:00Z")));
    Files.setLastModifiedTime(newer, FileTime.from(Instant.parse("2025-01-01T00:00:00Z")));
    String table = "db.`quo'te`";
    assertEquals(
        1,
        IcebergRemoveOrphanFilesJob.execute(
                spark, "test_catalog", table, "2024-01-01 00:00:00", null, true)
            .size());
    assertTrue(Files.exists(old));
    assertEquals(
        1,
        IcebergRemoveOrphanFilesJob.execute(
                spark, "test_catalog", table, "2024-01-01 00:00:00", null, false)
            .size());
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(newer));
  }

  private static List<Row> execute(String table, String olderThan, String location, boolean dryRun)
      throws IOException {
    return IcebergRemoveOrphanFilesJob.execute(
        spark, "test_catalog", "db." + table, olderThan, location, dryRun);
  }

  private static IcebergRemoveOrphanFilesJob.OrphanCleanupResult executeWithMetrics(
      String table, String olderThan, String location, boolean dryRun) throws IOException {
    return IcebergRemoveOrphanFilesJob.executeWithMetrics(
        spark, "test_catalog", "db." + table, olderThan, location, dryRun);
  }
}
