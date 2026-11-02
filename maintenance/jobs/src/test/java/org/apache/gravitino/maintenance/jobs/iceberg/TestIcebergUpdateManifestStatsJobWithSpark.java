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
import java.lang.reflect.Proxy;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.IcebergManifestStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests manifest statistics collection against real Spark and Iceberg tables. */
public class TestIcebergUpdateManifestStatsJobWithSpark {
  private static final String CATALOG_NAME = "manifest_catalog";
  @TempDir static File tempDir;
  private static SparkSession spark;

  /** Starts Spark with a local Iceberg catalog for the tests. */
  @BeforeAll
  public static void setUp() {
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

  /** Stops the shared Spark session. */
  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /** Verifies all specs are measured together and published in one replacement update. */
  @Test
  public void testManifestStatisticsAcrossPartitionEvolution() throws Exception {
    String name = CATALOG_NAME + ".db.manifest_evolution";
    spark.sql("CREATE TABLE " + name + " (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)");
    try {
      spark.sql("INSERT INTO " + name + " VALUES (1, 'a'), (2, 'b')");
      spark.sql("ALTER TABLE " + name + " ADD PARTITION FIELD bucket(4, id)");
      spark.sql("INSERT INTO " + name + " VALUES (3, 'c')");
      spark.sql("ALTER TABLE " + name + " ADD PARTITION FIELD truncate(2, ds)");
      Table table = Spark3Util.loadIcebergTable(spark, name);
      Snapshot pinned = table.currentSnapshot();
      AtomicInteger snapshotReads = new AtomicInteger();
      AtomicInteger manifestReads = new AtomicInteger();
      Snapshot snapshot =
          (Snapshot)
              Proxy.newProxyInstance(
                  Snapshot.class.getClassLoader(),
                  new Class<?>[] {Snapshot.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("allManifests")) {
                      manifestReads.incrementAndGet();
                    }
                    return method.invoke(pinned, args);
                  });
      Table countedTable =
          (Table)
              Proxy.newProxyInstance(
                  Table.class.getClassLoader(),
                  new Class<?>[] {Table.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("currentSnapshot")) {
                      snapshotReads.incrementAndGet();
                      return snapshot;
                    }
                    return method.invoke(table, args);
                  });
      IcebergUpdateManifestStatsJob.collectManifestStatistics(countedTable);
      assertEquals(1, snapshotReads.get());
      assertEquals(1, manifestReads.get());
      RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.manifest_evolution", "test", tempDir.toPath());
      assertEquals(1, updater.updateCalls);
      assertEquals(3, updater.manifestStatistics.size());
      Map<String, StatisticValue<?>> values = new HashMap<>();
      updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
      for (Row expected :
          spark
              .sql(
                  "SELECT partition_spec_id, COUNT(*), AVG(length) FROM "
                      + name
                      + ".manifests GROUP BY partition_spec_id")
              .collectAsList()) {
        IcebergManifestStatistics actual =
            IcebergManifestStatistics.fromStatistics(values, expected.getInt(0)).get();
        assertEquals(expected.getLong(1), actual.count());
        assertEquals(expected.getDouble(2), actual.averageSize());
      }
      IcebergManifestStatistics empty = IcebergManifestStatistics.fromStatistics(values, 2).get();
      assertEquals(0L, empty.count());
      assertEquals(0D, empty.averageSize());
      assertFalse(IcebergManifestStatistics.fromStatistics(values, Integer.MAX_VALUE).isPresent());
      long snapshotId =
          spark
              .sql("SELECT snapshot_id FROM " + name + ".snapshots ORDER BY committed_at DESC")
              .first()
              .getLong(0);
      assertEquals(snapshotId, values.get(IcebergManifestStatistics.SNAPSHOT_ID).value());
    } finally {
      spark.sql("DROP TABLE " + name);
    }
  }

  /** Verifies an empty table publishes a complete zero-valued measurement. */
  @Test
  public void testPublishesEmptyUnpartitionedTable() throws Exception {
    String name = CATALOG_NAME + ".db.empty";
    spark.sql("CREATE TABLE " + name + " (id INT) USING iceberg");
    try {
      RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.empty", "test", tempDir.toPath());
      assertEquals(NameIdentifier.of(CATALOG_NAME, "db", "empty"), updater.identifier);
      assertEquals(1, updater.updateCalls);
      Map<String, StatisticValue<?>> values = new HashMap<>();
      updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
      IcebergManifestStatistics result = IcebergManifestStatistics.fromStatistics(values, 0).get();
      assertEquals(0L, result.count());
      assertEquals(0D, result.averageSize());
      assertEquals(-1L, values.get(IcebergManifestStatistics.SNAPSHOT_ID).value());
    } finally {
      spark.sql("DROP TABLE " + name);
    }
  }

  /** Verifies uncoordinated publications replace complete measurements and can be rerun. */
  @Test
  public void testPublicationWithoutCoordination() throws Exception {
    String name = CATALOG_NAME + ".db.uncoordinated";
    spark.sql("CREATE TABLE " + name + " (id INT) USING iceberg");
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
    try {
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.uncoordinated", "test", null);
      assertEquals(1, updater.updateCalls);
      assertEquals(3, updater.manifestStatistics.size());
      spark.sql("INSERT INTO " + name + " VALUES (1)");
      updater.fail = true;
      assertThrows(
          IllegalStateException.class,
          () ->
              IcebergUpdateManifestStatsJob.updateStatistics(
                  spark, updater, CATALOG_NAME, "db.uncoordinated", "test", null));
      updater.fail = false;
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.uncoordinated", "test", null);
      assertEquals(3, updater.updateCalls);
      Map<String, StatisticValue<?>> values = new HashMap<>();
      updater.manifestStatistics.forEach(stat -> values.put(stat.name(), stat.value()));
      assertEquals(3, values.size());
      assertEquals(
          Spark3Util.loadIcebergTable(spark, name).currentSnapshot().snapshotId(),
          values.get(IcebergManifestStatistics.SNAPSHOT_ID).value());
      assertEquals(1L, IcebergManifestStatistics.fromStatistics(values, 0).get().count());
    } finally {
      spark.sql("DROP TABLE " + name);
    }
  }

  /** Verifies a collection failure can be rerun without manually recovering the lock file. */
  @Test
  public void testCollectionFailureCanBeRerun() throws Exception {
    Path directory = Files.createDirectory(tempDir.toPath().resolve("collection-failure"));
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IcebergUpdateManifestStatsJob.updateStatistics(
                spark, updater, CATALOG_NAME, "db.retry_collection", "test", directory));
    assertEquals(0, updater.updateCalls);
    spark.sql("CREATE TABLE " + CATALOG_NAME + ".db.retry_collection (id INT) USING iceberg");
    try {
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.retry_collection", "test", directory);
      assertEquals(1, updater.updateCalls);
    } finally {
      spark.sql("DROP TABLE " + CATALOG_NAME + ".db.retry_collection");
    }
  }

  /** Verifies separate job processes share the lock and failed writes block later cycles. */
  @Test
  public void testSharedCoordinationAndFailedWriteRecovery() throws Exception {
    String name = CATALOG_NAME + ".db.coordinated";
    spark.sql("CREATE TABLE " + name + " (id INT) USING iceberg");
    Path directory = Files.createDirectory(tempDir.toPath().resolve("coordination"));
    RecordingStatisticsUpdater updater = new RecordingStatisticsUpdater();
    try {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              IcebergUpdateManifestStatsJob.updateStatistics(
                  spark, updater, CATALOG_NAME, "db.coordinated", "test", Paths.get("relative")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              IcebergUpdateManifestStatsJob.updateStatistics(
                  spark,
                  updater,
                  CATALOG_NAME,
                  "db.coordinated",
                  "test",
                  directory.resolve("missing")));
      assertEquals(0, updater.updateCalls);
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.coordinated", "test", directory);
      Path lockPath;
      try (Stream<Path> files = Files.list(directory)) {
        lockPath = files.findFirst().get();
      }
      Path ready = directory.resolve("ready");
      Process otherHost =
          new ProcessBuilder(
                  new File(System.getProperty("java.home"), "bin/java").toString(), "-cp",
                  System.getProperty("java.class.path"), LockHolder.class.getName(),
                  lockPath.toString(), ready.toString())
              .redirectErrorStream(true)
              .redirectOutput(directory.resolve("lock-holder.log").toFile())
              .start();
      try {
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> Files.exists(ready));
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergUpdateManifestStatsJob.updateStatistics(
                    spark, updater, CATALOG_NAME, "db.coordinated", "test", directory));
        assertEquals(1, updater.updateCalls);
        // Independent table targets do not share a coordination lock.
        spark.sql("CREATE TABLE " + CATALOG_NAME + ".db.independent (id INT) USING iceberg");
        IcebergUpdateManifestStatsJob.updateStatistics(
            spark, updater, CATALOG_NAME, "db.independent", "test", directory);
        assertEquals(2, updater.updateCalls);
      } finally {
        otherHost.destroyForcibly();
        assertTrue(otherHost.waitFor(10, TimeUnit.SECONDS));
      }
      updater.fail = true;
      assertThrows(
          IllegalStateException.class,
          () ->
              IcebergUpdateManifestStatsJob.updateStatistics(
                  spark, updater, CATALOG_NAME, "db.coordinated", "test", directory));
      assertEquals(3, updater.updateCalls);
      updater.fail = false;
      IllegalStateException unfinished =
          assertThrows(
              IllegalStateException.class,
              () ->
                  IcebergUpdateManifestStatsJob.updateStatistics(
                      spark, updater, CATALOG_NAME, "db.coordinated", "test", directory));
      assertTrue(unfinished.getMessage().contains("requires recovery"));
      assertEquals(3, updater.updateCalls);
      // Recovery is explicit and preserves the shared inode, after the prior write has ended.
      try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.WRITE);
          FileLock lock = channel.lock()) {
        channel.truncate(0);
        channel.force(true);
      }
      IcebergUpdateManifestStatsJob.updateStatistics(
          spark, updater, CATALOG_NAME, "db.coordinated", "test", directory);
      assertEquals(4, updater.updateCalls);
      assertEquals(0, Files.size(lockPath));
    } finally {
      spark.sql("DROP TABLE " + name);
      spark.sql("DROP TABLE IF EXISTS " + CATALOG_NAME + ".db.independent");
    }
  }

  /** Holds the shared filesystem lock from a separate JVM to model another job host. */
  public static class LockHolder {
    /**
     * Holds the lock until this test subprocess is terminated.
     *
     * @param args lock path and readiness path
     * @throws Exception if coordination fails
     */
    public static void main(String[] args) throws Exception {
      try (FileChannel channel = FileChannel.open(Paths.get(args[0]), StandardOpenOption.WRITE);
          FileLock lock = channel.lock()) {
        Files.write(Paths.get(args[1]), new byte[] {1});
        new CountDownLatch(1).await();
      }
    }
  }

  private static final class RecordingStatisticsUpdater implements StatisticsUpdater {
    private NameIdentifier identifier;
    private int updateCalls;
    private boolean fail;
    private List<StatisticEntry<?>> manifestStatistics = Collections.emptyList();

    @Override
    public String name() {
      return "recording-updater";
    }

    @Override
    public void initialize(OptimizerEnv env) {}

    @Override
    public void updateTableStatistics(
        NameIdentifier identifier, List<StatisticEntry<?>> statistics) {
      this.identifier = identifier;
      this.manifestStatistics = statistics;
      updateCalls++;
      if (fail) {
        throw new IllegalStateException("Ambiguous PUT failure");
      }
    }

    @Override
    public void updatePartitionStatistics(
        NameIdentifier identifier, Map<PartitionPath, List<StatisticEntry<?>>> statistics) {
      throw new AssertionError("Manifest statistics must be table-level statistics");
    }

    @Override
    public void close() {}
  }
}
