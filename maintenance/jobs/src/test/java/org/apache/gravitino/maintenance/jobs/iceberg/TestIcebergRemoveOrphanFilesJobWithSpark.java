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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergRemoveOrphanFilesJobWithSpark {
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

  @Test
  void testDryRunAndDeletionPreserveLiveAndRecentFiles() throws Exception {
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
    Map<String, String> args = args("cleanup");
    args.put("dry-run", "true");
    assertEquals(1, IcebergRemoveOrphanFilesJob.execute(spark, args));
    assertTrue(Files.exists(old));
    args.put("dry-run", "false");
    assertEquals(1, IcebergRemoveOrphanFilesJob.execute(spark, args));
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(recent));
    assertEquals(2, spark.table("test_catalog.db.cleanup").count());
    assertEquals(0, IcebergRemoveOrphanFilesJob.execute(spark, args));
  }

  @Test
  void testCustomLocationAndOutsideRejection() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.scoped (id INT) USING iceberg");
    Path root = tempDir.resolve("db/scoped");
    Path sub = Files.createDirectories(root.resolve("staged"));
    Path inside = Files.write(sub.resolve("old"), new byte[] {1});
    Path outside = Files.write(root.resolve("old"), new byte[] {1});
    FileTime old = FileTime.from(Instant.now().minus(5, ChronoUnit.DAYS));
    Files.setLastModifiedTime(inside, old);
    Files.setLastModifiedTime(outside, old);
    Map<String, String> args = args("scoped");
    args.put("location", sub.toString());
    assertEquals(1, IcebergRemoveOrphanFilesJob.execute(spark, args));
    assertFalse(Files.exists(inside));
    assertTrue(Files.exists(outside));
    args.put("location", tempDir.toString());
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.execute(spark, args));
    assertTrue(Files.exists(outside));
    Path link = root.resolve("escape");
    Files.createSymbolicLink(link, tempDir);
    args.put("location", link.toString());
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.execute(spark, args));
  }

  @Test
  void testInvalidInputsFailBeforeDeletion() throws Exception {
    spark.sql("CREATE TABLE test_catalog.db.invalid (id INT) USING iceberg");
    Map<String, String> options = args("invalid");
    options.put("dry-run", "yes");
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.execute(spark, options));
    options.remove("dry-run");
    options.put("location", tempDir.resolve("db/invalid/missing").toString());
    assertThrows(IOException.class, () -> IcebergRemoveOrphanFilesJob.execute(spark, options));
    options.remove("location");
    options.put("older-than", "2999-01-01 00:00:00");
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.execute(spark, options));
    options.remove("older-than");
    spark.conf().set("spark.sql.parser.escapedStringLiterals", "true");
    try {
      assertThrows(
          IllegalArgumentException.class,
          () -> IcebergRemoveOrphanFilesJob.execute(spark, options));
    } finally {
      spark.conf().set("spark.sql.parser.escapedStringLiterals", "false");
    }
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRemoveOrphanFilesJob.main(new String[] {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> IcebergRemoveOrphanFilesJob.main(new String[] {"--catalog", "test_catalog"}));
  }

  @Test
  void testExplicitCutoffAndQuotedTableName() throws Exception {
    Path location = tempDir.resolve("db/quo'te");
    spark.sql("CREATE TABLE test_catalog.db.`quo'te` (id INT) USING iceberg");
    Path old = Files.write(location.resolve("old"), new byte[] {1});
    Path newer = Files.write(location.resolve("newer"), new byte[] {1});
    Files.setLastModifiedTime(old, FileTime.from(Instant.parse("2023-01-01T00:00:00Z")));
    Files.setLastModifiedTime(newer, FileTime.from(Instant.parse("2025-01-01T00:00:00Z")));
    Map<String, String> options = args("`quo'te`");
    options.put("older-than", "2024-01-01 00:00:00");
    options.put("dry-run", "true");
    assertEquals(1, IcebergRemoveOrphanFilesJob.execute(spark, options));
    assertTrue(Files.exists(old));
    options.put("dry-run", "false");
    assertEquals(1, IcebergRemoveOrphanFilesJob.execute(spark, options));
    assertFalse(Files.exists(old));
    assertTrue(Files.exists(newer));
  }

  private static Map<String, String> args(String table) {
    Map<String, String> args = new HashMap<>();
    args.put("catalog", "test_catalog");
    args.put("table", "db." + table);
    return args;
  }
}
