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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for IcebergUpdateStatsJob with a real Spark+Iceberg runtime. */
public class TestIcebergUpdateStatsJobWithSpark {

  @TempDir static File tempDir;

  private static SparkSession spark;
  private static String catalogName;

  @BeforeAll
  public static void setUp() {
    String warehousePath = new File(tempDir, "warehouse").getAbsolutePath();
    catalogName = "test_catalog";

    spark =
        SparkSession.builder()
            .appName("TestIcebergUpdateStatsJob")
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".type", "hadoop")
            .config("spark.sql.catalog." + catalogName + ".warehouse", warehousePath)
            .getOrCreate();

    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".db");
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.non_partitioned (id INT, name STRING) USING iceberg");
    spark.sql(
        "INSERT INTO " + catalogName + ".db.non_partitioned VALUES (1, 'A'), (2, 'B'), (3, 'C')");

    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".db.partitioned (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)");
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".db.partitioned VALUES "
            + "(1, '2026-01-01'), (2, '2026-01-01'), (3, '2026-01-02')");
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.non_partitioned");
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".db.partitioned");
      spark.sql("DROP NAMESPACE IF EXISTS " + catalogName + ".db");
      spark.stop();
    }
  }

  @Test
  public void testUpdateNonPartitionedTableStatistics() {
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();

    IcebergUpdateStatsJob.updateStatistics(
        spark, updater, catalogName, "db.non_partitioned", 100_000L);

    assertEquals(NameIdentifier.of(catalogName, "db", "non_partitioned"), updater.tableIdentifier);
    assertNotNull(updater.tableStatistics);
    assertEquals(8, updater.tableStatistics.size());
    assertTrue(updater.partitionStatistics.isEmpty());

    Map<String, Object> stats =
        updater.tableStatistics.stream()
            .collect(Collectors.toMap(StatisticEntry::name, stat -> stat.value().value()));
    assertTrue((Long) stats.get("custom-file_count") > 0L);
    assertTrue((Double) stats.get("custom-datafile_mse") >= 0D);
    assertTrue((Long) stats.get("custom-total_size") > 0L);
  }

  @Test
  public void testUpdatePartitionedTableStatistics() {
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();

    IcebergUpdateStatsJob.updateStatistics(spark, updater, catalogName, "db.partitioned", 100_000L);

    assertEquals(NameIdentifier.of(catalogName, "db", "partitioned"), updater.tableIdentifier);
    assertTrue(updater.tableStatistics.isEmpty());
    assertFalse(updater.partitionStatistics.isEmpty());
    assertEquals(2, updater.partitionStatistics.size());

    updater.partitionStatistics.forEach(
        (partitionPath, statistics) -> {
          assertEquals(1, partitionPath.entries().size());
          assertEquals("ds", partitionPath.entries().get(0).partitionName());
          Map<String, Object> statMap =
              statistics.stream()
                  .collect(Collectors.toMap(StatisticEntry::name, stat -> stat.value().value()));
          assertTrue(statMap.containsKey("custom-datafile_mse"));
          assertTrue((Long) statMap.get("custom-file_count") > 0L);
        });
  }

  private static final class RecordingStatisticsUpdater implements StatisticsUpdater {
    private NameIdentifier tableIdentifier;
    private List<StatisticEntry<?>> tableStatistics = List.of();
    private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics = Map.of();

    @Override
    public String name() {
      return "recording-updater";
    }

    @Override
    public void initialize(OptimizerEnv optimizerEnv) {}

    @Override
    public void updateTableStatistics(
        NameIdentifier tableIdentifier, List<StatisticEntry<?>> tableStatistics) {
      this.tableIdentifier = tableIdentifier;
      this.tableStatistics = tableStatistics;
    }

    @Override
    public void updatePartitionStatistics(
        NameIdentifier tableIdentifier,
        Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
      this.tableIdentifier = tableIdentifier;
      this.partitionStatistics = partitionStatistics;
    }

    @Override
    public void close() {}
  }
}
