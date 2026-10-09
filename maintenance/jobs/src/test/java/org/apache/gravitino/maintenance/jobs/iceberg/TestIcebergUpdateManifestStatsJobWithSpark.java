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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises manifest collection through Spark and resolved job-template arguments. */
public class TestIcebergUpdateManifestStatsJobWithSpark {
  private static final String CATALOG_NAME = "manifest_catalog";
  @TempDir File tempDir;
  private SparkSession spark;

  @BeforeEach
  void setUp() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("TestIcebergUpdateManifestStatsJob")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG_NAME + ".type", "hadoop")
            .config(
                "spark.sql.catalog." + CATALOG_NAME + ".warehouse",
                new File(tempDir, "warehouse").getAbsolutePath())
            .getOrCreate();
    spark.sql("CREATE NAMESPACE " + CATALOG_NAME + ".db");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testManifestStatisticsAcrossPartitionEvolution() {
    String name = CATALOG_NAME + ".db.manifest_evolution";
    spark.sql("CREATE TABLE " + name + " (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)");
    try {
      IcebergManifestStatistics empty =
          IcebergUpdateManifestStatsJob.collectManifestStatistics(
              spark, CATALOG_NAME, "db.manifest_evolution", null);
      assertEquals(0L, empty.count());
      assertEquals(0D, empty.averageSize());
      int oldSpec = empty.specId();
      spark.sql("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("ALTER TABLE " + name + " ADD PARTITION FIELD bucket(4, id)");
      IcebergManifestStatistics newEmpty =
          IcebergUpdateManifestStatsJob.collectManifestStatistics(
              spark, CATALOG_NAME, "db.manifest_evolution", null);
      assertTrue(newEmpty.specId() != oldSpec);
      assertEquals(0L, newEmpty.count());
      assertEquals(0D, newEmpty.averageSize());
      spark.sql("INSERT INTO " + name + " VALUES (3, 'c')");
      for (int spec : new int[] {oldSpec, newEmpty.specId()}) {
        Row expected =
            spark
                .sql(
                    "SELECT COUNT(*), AVG(length) FROM "
                        + name
                        + ".manifests WHERE partition_spec_id = "
                        + spec)
                .first();
        IcebergManifestStatistics actual =
            IcebergUpdateManifestStatsJob.collectManifestStatistics(
                spark, CATALOG_NAME, "db.manifest_evolution", spec);
        assertEquals(spec, actual.specId());
        assertEquals(expected.getLong(0), actual.count());
        assertEquals(expected.getDouble(1), actual.averageSize());
      }
      assertThrows(
          IllegalArgumentException.class,
          () ->
              IcebergUpdateManifestStatsJob.collectManifestStatistics(
                  spark, CATALOG_NAME, "db.manifest_evolution", Integer.MAX_VALUE));
      RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.manifest_evolution", oldSpec);
      assertEquals(2, updater.manifestStatistics.size());
      Map<String, StatisticValue<?>> values = new HashMap<>();
      updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
      assertEquals(
          oldSpec, IcebergManifestStatistics.fromStatistics(values, oldSpec).get().specId());
      assertFalse(IcebergManifestStatistics.fromStatistics(values, newEmpty.specId()).isPresent());
    } finally {
      spark.sql("DROP TABLE " + name);
    }
  }

  @Test
  void testPublishesEmptyUnpartitionedTable() {
    String name = CATALOG_NAME + ".db.empty";
    spark.sql("CREATE TABLE " + name + " (id INT) USING iceberg");
    try {
      RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.empty", null);
      assertEquals(NameIdentifier.of(CATALOG_NAME, "db", "empty"), updater.identifier);
      assertEquals(1, updater.mergeCalls);
      Map<String, StatisticValue<?>> values = new HashMap<>();
      updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
      IcebergManifestStatistics result = IcebergManifestStatistics.fromStatistics(values, 0).get();
      assertEquals(0L, result.count());
      assertEquals(0D, result.averageSize());
    } finally {
      spark.sql("DROP TABLE " + name);
    }
  }

  @Test
  void testTemplateWithOmittedOptionalValues() throws Exception {
    runTemplateWithOmittedSpec(false);
  }

  @Test
  void testTemplateWithOmittedSpecAndExplicitSparkConfig() throws Exception {
    runTemplateWithOmittedSpec(true);
  }

  private void runTemplateWithOmittedSpec(boolean explicitSparkConfig) throws Exception {
    spark.sql(
        "CREATE TABLE "
            + CATALOG_NAME
            + ".db.template_defaults (id INT, ds STRING) "
            + "USING iceberg PARTITIONED BY (ds)");
    spark.sql(
        "ALTER TABLE " + CATALOG_NAME + ".db.template_defaults ADD PARTITION FIELD bucket(2, id)");
    int resolved =
        IcebergUpdateManifestStatsJob.collectManifestStatistics(
                spark, CATALOG_NAME, "db.template_defaults", null)
            .specId();
    assertTrue(resolved > 0);
    Map<String, String> conf = new HashMap<>();
    conf.put("catalog_name", CATALOG_NAME);
    conf.put("table_identifier", "db.template_defaults");
    conf.put(
        "updater_options",
        "{\"statistics_updater\":\"recording-updater\","
            + "\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"test\"}");
    if (explicitSparkConfig) {
      conf.put("spark_conf", "{}");
    }
    String[] arguments =
        new IcebergUpdateManifestStatsJob()
            .jobTemplate().arguments().stream()
                .map(
                    argument -> {
                      String value = argument;
                      for (Map.Entry<String, String> entry : conf.entrySet()) {
                        value = value.replace("{{" + entry.getKey() + "}}", entry.getValue());
                      }
                      return value;
                    })
                .toArray(String[]::new);
    IcebergUpdateManifestStatsJob.main(arguments);
    RecordingStatisticsUpdater updater = RecordingStatisticsUpdater.lastCreated;
    assertEquals(1, updater.mergeCalls);
    Map<String, StatisticValue<?>> values = new HashMap<>();
    updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
    assertTrue(IcebergManifestStatistics.fromStatistics(values, resolved).isPresent());
    assertTrue(updater.closed);
    assertTrue(spark.sparkContext().isStopped());
  }

  /** Service-loaded updater that records the real job entry point's published measurement. */
  public static final class RecordingStatisticsUpdater implements StatisticsUpdater {
    private static RecordingStatisticsUpdater lastCreated;
    private boolean closed;
    private NameIdentifier identifier;

    private int mergeCalls;
    private List<StatisticEntry<?>> manifestStatistics = Collections.emptyList();

    /** Records the instance created by the job's provider loader. */
    public RecordingStatisticsUpdater() {
      lastCreated = this;
    }

    @Override
    public String name() {
      return "recording-updater";
    }

    @Override
    public void initialize(OptimizerEnv env) {}

    @Override
    public void mergeTableStatistics(
        NameIdentifier identifier, List<StatisticEntry<?>> statistics) {
      this.identifier = identifier;
      this.manifestStatistics = statistics;
      mergeCalls++;
    }

    @Override
    public void updateTableStatistics(
        NameIdentifier identifier, List<StatisticEntry<?>> statistics) {
      throw new AssertionError("Manifest statistics must use an atomic merge");
    }

    @Override
    public void updatePartitionStatistics(
        NameIdentifier identifier, Map<PartitionPath, List<StatisticEntry<?>>> statistics) {
      throw new AssertionError("Manifest statistics must be table-level statistics");
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
