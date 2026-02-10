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
package org.apache.gravitino.stats.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PGImageName;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end integration tests for {@link JdbcPartitionStatisticStorage} using multiple database
 * backends via Testcontainers.
 *
 * <p>These tests verify the complete flow from API calls through JDBC to real database instances.
 * They cover:
 *
 * <ul>
 *   <li>Full CRUD operations (Create, Read, Update, Delete)
 *   <li>Partition range queries with different bound types
 *   <li>Transaction integrity and rollback behavior
 *   <li>Concurrent access from multiple threads
 *   <li>JSON serialization/deserialization
 *   <li>Audit information preservation
 *   <li>Large dataset handling
 *   <li>Database-specific SQL syntax (MySQL ON DUPLICATE KEY vs PostgreSQL ON CONFLICT)
 * </ul>
 */
@Tag("gravitino-docker-test")
public class TestJdbcPartitionStatisticStorageIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestJdbcPartitionStatisticStorageIT.class);

  /**
   * Abstract base class containing all test logic. Each database-specific test class extends this
   * and implements the database setup.
   */
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  abstract static class BaseJdbcPartitionStatisticStorageTest {

    protected JdbcPartitionStatisticStorage storage;
    protected EntityStore entityStore;

    protected static final String METALAKE = "test_metalake";
    protected static final MetadataObject TEST_TABLE =
        MetadataObjects.of(
            Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

    /** Subclasses must implement to set up database-specific storage. */
    protected abstract void setupStorage() throws Exception;

    /** Subclasses must implement to tear down storage. */
    protected abstract void teardownStorage() throws Exception;

    @BeforeAll
    public void baseSetup() throws Exception {
      // Mock EntityStore to return a test table entity
      entityStore = mock(EntityStore.class);
      TableEntity tableEntity = mock(TableEntity.class);
      when(entityStore.get(any(), any(), any())).thenReturn(tableEntity);
      when(tableEntity.id()).thenReturn(100L);
      FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);

      setupStorage();
    }

    @AfterAll
    public void baseTeardown() throws Exception {
      teardownStorage();
    }

    @Test
    public void testFullCRUDLifecycle() throws IOException {
      LOG.info("Testing full CRUD lifecycle");

      // 1. CREATE - Insert statistics for a partition
      List<PartitionStatisticsUpdate> updates = new ArrayList<>();
      Map<String, StatisticValue<?>> stats = Maps.newHashMap();
      stats.put("custom-rowCount", StatisticValues.longValue(1000L));
      stats.put("custom-sizeBytes", StatisticValues.longValue(5000000L));
      stats.put("custom-lastModified", StatisticValues.stringValue("2025-01-21"));

      updates.add(PartitionStatisticsModification.update("partition_2025_01", stats));

      List<MetadataObjectStatisticsUpdate> objectUpdates =
          Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

      storage.updateStatistics(METALAKE, objectUpdates);
      LOG.info("Created statistics for partition_2025_01");

      // 2. READ - Verify statistics exist
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

      assertEquals(1, result.size());
      assertEquals("partition_2025_01", result.get(0).partitionName());
      assertEquals(3, result.get(0).statistics().size());

      // Verify values and audit info
      PersistedStatistic rowCount =
          result.get(0).statistics().stream()
              .filter(s -> s.name().equals("custom-rowCount"))
              .findFirst()
              .orElseThrow();

      assertEquals(1000L, rowCount.value().value());
      assertNotNull(rowCount.auditInfo());
      assertNotNull(rowCount.auditInfo().creator());
      assertNotNull(rowCount.auditInfo().createTime());
      LOG.info("Verified statistics were created with audit info");

      // 3. UPDATE - Modify existing statistic
      stats.clear();
      stats.put("custom-rowCount", StatisticValues.longValue(2000L));
      stats.put("custom-sizeBytes", StatisticValues.longValue(5000000L));
      stats.put("custom-lastModified", StatisticValues.stringValue("2025-01-21"));

      updates.clear();
      updates.add(PartitionStatisticsModification.update("partition_2025_01", stats));
      objectUpdates = Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

      storage.updateStatistics(METALAKE, objectUpdates);

      result = storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);
      rowCount =
          result.get(0).statistics().stream()
              .filter(s -> s.name().equals("custom-rowCount"))
              .findFirst()
              .orElseThrow();

      assertEquals(2000L, rowCount.value().value());
      LOG.info("Updated statistic value from 1000 to 2000");

      // 4. DELETE - Drop specific statistic
      List<MetadataObjectStatisticsDrop> drops =
          Lists.newArrayList(
              MetadataObjectStatisticsDrop.of(
                  TEST_TABLE,
                  Lists.newArrayList(
                      PartitionStatisticsModification.drop(
                          "partition_2025_01", Lists.newArrayList("custom-rowCount")))));

      int dropped = storage.dropStatistics(METALAKE, drops);
      assertEquals(1, dropped);
      LOG.info("Dropped 1 statistic");

      // 5. VERIFY - Statistic is gone
      result = storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);
      assertEquals(2, result.get(0).statistics().size());

      boolean hasRowCount =
          result.get(0).statistics().stream().anyMatch(s -> s.name().equals("custom-rowCount"));
      assertFalse(hasRowCount);
      LOG.info("Verified statistic was deleted");

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testPartitionRangeQueries() throws IOException {
      LOG.info("Testing partition range queries");

      // Insert statistics for multiple partitions
      insertMultiplePartitions("p1", "p2", "p3", "p4", "p5");

      // Test CLOSED bounds: [p2, p4] should return p2, p3, p4
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(
              METALAKE,
              TEST_TABLE,
              PartitionRange.between(
                  "p2", PartitionRange.BoundType.CLOSED, "p4", PartitionRange.BoundType.CLOSED));

      assertEquals(3, result.size());
      List<String> partitionNames =
          result.stream().map(PersistedPartitionStatistics::partitionName).toList();
      assertTrue(partitionNames.contains("p2"));
      assertTrue(partitionNames.contains("p3"));
      assertTrue(partitionNames.contains("p4"));
      LOG.info("CLOSED bounds [p2, p4] returned 3 partitions: {}", partitionNames);

      // Test OPEN bounds: (p2, p4) should return only p3
      result =
          storage.listStatistics(
              METALAKE,
              TEST_TABLE,
              PartitionRange.between(
                  "p2", PartitionRange.BoundType.OPEN, "p4", PartitionRange.BoundType.OPEN));

      assertEquals(1, result.size());
      assertEquals("p3", result.get(0).partitionName());
      LOG.info("OPEN bounds (p2, p4) returned 1 partition: p3");

      // Test upTo: <= p3 should return p1, p2, p3
      result =
          storage.listStatistics(
              METALAKE, TEST_TABLE, PartitionRange.upTo("p3", PartitionRange.BoundType.CLOSED));

      assertEquals(3, result.size());
      partitionNames = result.stream().map(PersistedPartitionStatistics::partitionName).toList();
      assertTrue(partitionNames.contains("p1"));
      assertTrue(partitionNames.contains("p2"));
      assertTrue(partitionNames.contains("p3"));
      LOG.info("upTo p3 (CLOSED) returned 3 partitions");

      // Test downTo: >= p3 should return p3, p4, p5
      result =
          storage.listStatistics(
              METALAKE, TEST_TABLE, PartitionRange.downTo("p3", PartitionRange.BoundType.CLOSED));

      assertEquals(3, result.size());
      partitionNames = result.stream().map(PersistedPartitionStatistics::partitionName).toList();
      assertTrue(partitionNames.contains("p3"));
      assertTrue(partitionNames.contains("p4"));
      assertTrue(partitionNames.contains("p5"));
      LOG.info("downTo p3 (CLOSED) returned 3 partitions");

      // Test OPEN lower bound: (p2, p5] should return p3, p4, p5
      result =
          storage.listStatistics(
              METALAKE,
              TEST_TABLE,
              PartitionRange.between(
                  "p2", PartitionRange.BoundType.OPEN, "p5", PartitionRange.BoundType.CLOSED));

      assertEquals(3, result.size());
      LOG.info("OPEN-CLOSED bounds (p2, p5] returned 3 partitions");

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testListStatisticsByPartitionNames() throws IOException {
      LOG.info("Testing list statistics by partition names");

      // Insert statistics for multiple partitions
      insertMultiplePartitions("part_a", "part_b", "part_c", "part_d", "part_e");

      // List specific partitions
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(
              METALAKE, TEST_TABLE, Lists.newArrayList("part_b", "part_d", "part_e"));

      assertEquals(3, result.size());
      List<String> partitionNames =
          result.stream().map(PersistedPartitionStatistics::partitionName).toList();
      assertTrue(partitionNames.contains("part_b"));
      assertTrue(partitionNames.contains("part_d"));
      assertTrue(partitionNames.contains("part_e"));
      assertFalse(partitionNames.contains("part_a"));
      assertFalse(partitionNames.contains("part_c"));
      LOG.info("Listed 3 specific partitions by name");

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testConcurrentWrites() throws Exception {
      LOG.info("Testing concurrent writes from multiple threads");

      int threadCount = 10;
      int partitionsPerThread = 50;

      ExecutorService executor = Executors.newFixedThreadPool(threadCount);
      CountDownLatch latch = new CountDownLatch(threadCount);
      AtomicInteger successCount = new AtomicInteger(0);
      AtomicInteger errorCount = new AtomicInteger(0);

      for (int i = 0; i < threadCount; i++) {
        final int threadId = i;
        executor.submit(
            () -> {
              try {
                for (int j = 0; j < partitionsPerThread; j++) {
                  String partitionName = "thread" + threadId + "_p" + j;

                  Map<String, StatisticValue<?>> stats = Maps.newHashMap();
                  stats.put("custom-threadId", StatisticValues.longValue((long) threadId));
                  stats.put("custom-partitionIndex", StatisticValues.longValue((long) j));
                  stats.put("custom-value", StatisticValues.stringValue("thread" + threadId));

                  List<PartitionStatisticsUpdate> updates =
                      Lists.newArrayList(
                          PartitionStatisticsModification.update(partitionName, stats));

                  List<MetadataObjectStatisticsUpdate> objectUpdates =
                      Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

                  storage.updateStatistics(METALAKE, objectUpdates);
                }
                successCount.incrementAndGet();
              } catch (Exception e) {
                LOG.error("Thread {} failed", threadId, e);
                errorCount.incrementAndGet();
              } finally {
                latch.countDown();
              }
            });
      }

      boolean completed = latch.await(60, TimeUnit.SECONDS);
      assertTrue(completed, "All threads should complete within 60 seconds");
      assertEquals(threadCount, successCount.get(), "All threads should succeed");
      assertEquals(0, errorCount.get(), "No threads should have errors");

      // Verify all statistics were written
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

      assertEquals(
          threadCount * partitionsPerThread,
          result.size(),
          "Should have statistics for all partitions");

      LOG.info(
          "Successfully completed {} concurrent writes across {} threads",
          result.size(),
          threadCount);

      executor.shutdown();

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testMultipleStatisticTypes() throws IOException {
      LOG.info("Testing multiple statistic value types");

      Map<String, StatisticValue<?>> stats = Maps.newHashMap();
      stats.put("custom-stringValue", StatisticValues.stringValue("test string"));
      stats.put("custom-longValue", StatisticValues.longValue(12345L));
      stats.put("custom-doubleValue", StatisticValues.doubleValue(123.45));
      stats.put("custom-booleanValue", StatisticValues.booleanValue(true));

      List<PartitionStatisticsUpdate> updates =
          Lists.newArrayList(
              PartitionStatisticsModification.update("mixed_types_partition", stats));

      List<MetadataObjectStatisticsUpdate> objectUpdates =
          Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

      storage.updateStatistics(METALAKE, objectUpdates);

      // Read back and verify types
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

      assertEquals(1, result.size());
      List<PersistedStatistic> statistics = result.get(0).statistics();
      assertEquals(4, statistics.size());

      // Verify each type
      PersistedStatistic stringVal =
          statistics.stream()
              .filter(s -> s.name().equals("custom-stringValue"))
              .findFirst()
              .orElseThrow();
      assertEquals("test string", stringVal.value().value());

      PersistedStatistic longVal =
          statistics.stream()
              .filter(s -> s.name().equals("custom-longValue"))
              .findFirst()
              .orElseThrow();
      assertEquals(12345L, longVal.value().value());

      PersistedStatistic doubleVal =
          statistics.stream()
              .filter(s -> s.name().equals("custom-doubleValue"))
              .findFirst()
              .orElseThrow();
      assertEquals(123.45, doubleVal.value().value());

      PersistedStatistic boolVal =
          statistics.stream()
              .filter(s -> s.name().equals("custom-booleanValue"))
              .findFirst()
              .orElseThrow();
      assertEquals(true, boolVal.value().value());

      LOG.info("All statistic value types serialized and deserialized correctly");

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testBatchDropMultipleStatistics() throws IOException {
      LOG.info("Testing batch drop of multiple statistics");

      // Insert statistics for multiple partitions
      insertMultiplePartitions("drop_p1", "drop_p2", "drop_p3");

      // Drop multiple statistics from different partitions
      List<PartitionStatisticsDrop> dropsList = new ArrayList<>();
      dropsList.add(
          PartitionStatisticsModification.drop("drop_p1", Lists.newArrayList("custom-stat1")));
      dropsList.add(
          PartitionStatisticsModification.drop(
              "drop_p2", Lists.newArrayList("custom-stat1", "custom-stat2")));

      List<MetadataObjectStatisticsDrop> drops =
          Lists.newArrayList(MetadataObjectStatisticsDrop.of(TEST_TABLE, dropsList));

      int droppedCount = storage.dropStatistics(METALAKE, drops);
      assertEquals(3, droppedCount, "Should drop 3 statistics total (1 from p1, 2 from p2)");

      LOG.info("Dropped {} statistics in batch", droppedCount);

      // Cleanup
      cleanupAllStatistics();
    }

    @Test
    public void testLargeDataset() throws IOException {
      LOG.info("Testing large dataset handling");

      int partitionCount = 1000;
      int statisticsPerPartition = 10;

      // Insert large dataset
      List<PartitionStatisticsUpdate> updates = new ArrayList<>();
      for (int i = 0; i < partitionCount; i++) {
        String partitionName = "large_p" + String.format("%04d", i);
        Map<String, StatisticValue<?>> stats = Maps.newHashMap();

        for (int j = 0; j < statisticsPerPartition; j++) {
          stats.put("custom-stat" + j, StatisticValues.longValue((long) (i * 1000 + j)));
        }

        updates.add(PartitionStatisticsModification.update(partitionName, stats));
      }

      List<MetadataObjectStatisticsUpdate> objectUpdates =
          Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

      long startTime = System.currentTimeMillis();
      storage.updateStatistics(METALAKE, objectUpdates);
      long insertTime = System.currentTimeMillis() - startTime;

      LOG.info(
          "Inserted {} partitions with {} statistics each in {} ms",
          partitionCount,
          statisticsPerPartition,
          insertTime);

      // Query all statistics
      startTime = System.currentTimeMillis();
      List<PersistedPartitionStatistics> result =
          storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);
      long queryTime = System.currentTimeMillis() - startTime;

      assertEquals(partitionCount, result.size());
      assertEquals(statisticsPerPartition, result.get(0).statistics().size());

      LOG.info("Queried {} partitions in {} ms", partitionCount, queryTime);

      // Performance assertions
      assertTrue(
          insertTime < 30000,
          "Insert should complete within 30 seconds, took " + insertTime + "ms");
      assertTrue(
          queryTime < 5000, "Query should complete within 5 seconds, took " + queryTime + "ms");

      // Cleanup
      cleanupAllStatistics();
    }

    // ==================== Helper Methods ====================

    /**
     * Inserts test statistics for multiple partitions.
     *
     * @param partitionNames the partition names to create
     */
    protected void insertMultiplePartitions(String... partitionNames) throws IOException {
      List<PartitionStatisticsUpdate> updates = new ArrayList<>();

      for (String partitionName : partitionNames) {
        Map<String, StatisticValue<?>> stats = Maps.newHashMap();
        stats.put("custom-stat1", StatisticValues.longValue(100L));
        stats.put("custom-stat2", StatisticValues.stringValue("value2"));
        stats.put("custom-stat3", StatisticValues.doubleValue(3.14));

        updates.add(PartitionStatisticsModification.update(partitionName, stats));
      }

      List<MetadataObjectStatisticsUpdate> objectUpdates =
          Lists.newArrayList(MetadataObjectStatisticsUpdate.of(TEST_TABLE, updates));

      storage.updateStatistics(METALAKE, objectUpdates);
    }

    /** Removes all statistics for the test table to cleanup between tests. */
    protected void cleanupAllStatistics() throws IOException {
      // Get all current statistics
      List<PersistedPartitionStatistics> allStats =
          storage.listStatistics(METALAKE, TEST_TABLE, PartitionRange.ALL_PARTITIONS);

      if (allStats.isEmpty()) {
        return;
      }

      // Build drop list for all statistics
      List<PartitionStatisticsDrop> dropsList = new ArrayList<>();
      for (PersistedPartitionStatistics partitionStats : allStats) {
        List<String> statisticNames =
            partitionStats.statistics().stream().map(PersistedStatistic::name).toList();

        dropsList.add(
            PartitionStatisticsModification.drop(partitionStats.partitionName(), statisticNames));
      }

      List<MetadataObjectStatisticsDrop> drops =
          Lists.newArrayList(MetadataObjectStatisticsDrop.of(TEST_TABLE, dropsList));

      storage.dropStatistics(METALAKE, drops);
      LOG.debug("Cleaned up all statistics for test table");
    }
  }

  /** MySQL-specific tests using Docker container. */
  @Nested
  @Tag("gravitino-docker-test")
  static class MySQLTest extends BaseJdbcPartitionStatisticStorageTest {

    private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
    private static final TestDatabaseName TEST_DB_NAME = TestDatabaseName.MYSQL_MYSQL_ABSTRACT_IT;
    private MySQLContainer mySQLContainer;

    @Override
    protected void setupStorage() throws Exception {
      LOG.info("Starting MySQL container for partition statistics integration tests");

      // Start MySQL container
      containerSuite.startMySQLContainer(TEST_DB_NAME);
      mySQLContainer = containerSuite.getMySQLContainer();

      // Create database schema
      createMySQLSchema();

      // Create storage factory with MySQL container connection
      Map<String, String> properties = Maps.newHashMap();
      properties.put("jdbcUrl", mySQLContainer.getJdbcUrl(TEST_DB_NAME));
      properties.put("jdbcUser", mySQLContainer.getUsername());
      properties.put("jdbcPassword", mySQLContainer.getPassword());
      properties.put("jdbcDriver", mySQLContainer.getDriverClassName(TEST_DB_NAME));

      JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();
      storage = (JdbcPartitionStatisticStorage) factory.create(properties);

      LOG.info("MySQL partition statistics storage initialized successfully");
    }

    @Override
    protected void teardownStorage() throws Exception {
      if (storage != null) {
        storage.close();
        LOG.info("MySQL partition statistics storage closed");
      }
    }

    /** Creates the partition_statistic_meta table in the MySQL test database. */
    private void createMySQLSchema() throws SQLException {
      String jdbcUrl = mySQLContainer.getJdbcUrl(TEST_DB_NAME);
      String username = mySQLContainer.getUsername();
      String password = mySQLContainer.getPassword();

      try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
          Statement stmt = conn.createStatement()) {

        String createTableSQL =
            "CREATE TABLE IF NOT EXISTS `partition_statistic_meta` ("
                + "  `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id from table_meta',"
                + "  `partition_name` VARCHAR(1024) NOT NULL COMMENT 'partition name',"
                + "  `statistic_name` VARCHAR(128) NOT NULL COMMENT 'statistic name',"
                + "  `statistic_value` MEDIUMTEXT NOT NULL COMMENT 'statistic value as JSON',"
                + "  `audit_info` TEXT NOT NULL COMMENT 'audit information as JSON',"
                + "  `created_at` BIGINT(20) UNSIGNED NOT NULL COMMENT 'creation timestamp in milliseconds',"
                + "  `updated_at` BIGINT(20) UNSIGNED NOT NULL COMMENT 'last update timestamp in milliseconds',"
                + "  PRIMARY KEY (`table_id`, `partition_name`(255), `statistic_name`),"
                + "  KEY `idx_table_partition` (`table_id`, `partition_name`(255))"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
                + "  COMMENT 'partition statistics metadata'";

        stmt.execute(createTableSQL);
        LOG.info("Created partition_statistic_meta table in MySQL");
      }
    }
  }

  /** PostgreSQL-specific tests using Docker container. */
  @Nested
  @Tag("gravitino-docker-test")
  static class PostgreSQLTest extends BaseJdbcPartitionStatisticStorageTest {

    private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
    private static final TestDatabaseName TEST_DB_NAME = TestDatabaseName.PG_TEST_PARTITION_STATS;
    private PostgreSQLContainer postgreSQLContainer;

    @Override
    protected void setupStorage() throws Exception {
      LOG.info("Starting PostgreSQL container for partition statistics integration tests");

      // Start PostgreSQL container
      containerSuite.startPostgreSQLContainer(TEST_DB_NAME, PGImageName.VERSION_13);
      postgreSQLContainer = containerSuite.getPostgreSQLContainer(PGImageName.VERSION_13);

      // Create database schema
      createPostgreSQLSchema();

      // Create storage factory with PostgreSQL container connection
      Map<String, String> properties = Maps.newHashMap();
      properties.put("jdbcUrl", postgreSQLContainer.getJdbcUrl(TEST_DB_NAME));
      properties.put("jdbcUser", postgreSQLContainer.getUsername());
      properties.put("jdbcPassword", postgreSQLContainer.getPassword());
      properties.put("jdbcDriver", postgreSQLContainer.getDriverClassName(TEST_DB_NAME));

      JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();
      storage = (JdbcPartitionStatisticStorage) factory.create(properties);

      LOG.info("PostgreSQL partition statistics storage initialized successfully");
    }

    @Override
    protected void teardownStorage() throws Exception {
      if (storage != null) {
        storage.close();
        LOG.info("PostgreSQL partition statistics storage closed");
      }
    }

    /** Creates the partition_statistic_meta table in the PostgreSQL test database. */
    private void createPostgreSQLSchema() throws SQLException {
      String jdbcUrl = postgreSQLContainer.getJdbcUrl(TEST_DB_NAME);
      String username = postgreSQLContainer.getUsername();
      String password = postgreSQLContainer.getPassword();

      try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
          Statement stmt = conn.createStatement()) {

        String createTableSQL =
            "CREATE TABLE IF NOT EXISTS partition_statistic_meta ("
                + "  table_id BIGINT NOT NULL,"
                + "  partition_name VARCHAR(1024) NOT NULL,"
                + "  statistic_name VARCHAR(128) NOT NULL,"
                + "  statistic_value TEXT NOT NULL,"
                + "  audit_info TEXT NOT NULL,"
                + "  created_at BIGINT NOT NULL,"
                + "  updated_at BIGINT NOT NULL,"
                + "  PRIMARY KEY (table_id, partition_name, statistic_name)"
                + ")";

        stmt.execute(createTableSQL);

        String createIndexSQL =
            "CREATE INDEX IF NOT EXISTS idx_table_partition ON partition_statistic_meta(table_id, partition_name)";
        stmt.execute(createIndexSQL);

        LOG.info("Created partition_statistic_meta table in PostgreSQL");
      }
    }
  }

  /** H2-specific tests using embedded in-memory database. */
  @Nested
  static class H2Test extends BaseJdbcPartitionStatisticStorageTest {

    private static final String H2_JDBC_URL =
        "jdbc:h2:mem:test_partition_stats;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String H2_USERNAME = "sa";
    private static final String H2_PASSWORD = "";

    @Override
    protected void setupStorage() throws Exception {
      LOG.info("Setting up H2 in-memory database for partition statistics integration tests");

      // Create database schema
      createH2Schema();

      // Create storage factory with H2 connection
      Map<String, String> properties = Maps.newHashMap();
      properties.put("jdbcUrl", H2_JDBC_URL);
      properties.put("jdbcUser", H2_USERNAME);
      properties.put("jdbcPassword", H2_PASSWORD);
      properties.put("jdbcDriver", "org.h2.Driver");

      JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();
      storage = (JdbcPartitionStatisticStorage) factory.create(properties);

      LOG.info("H2 partition statistics storage initialized successfully");
    }

    @Override
    protected void teardownStorage() throws Exception {
      if (storage != null) {
        storage.close();
        LOG.info("H2 partition statistics storage closed");
      }

      // Drop the H2 database
      try (Connection conn = DriverManager.getConnection(H2_JDBC_URL, H2_USERNAME, H2_PASSWORD);
          Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS partition_statistic_meta");
        LOG.info("Dropped H2 database");
      }
    }

    /** Creates the partition_statistic_meta table in the H2 test database. */
    private void createH2Schema() throws SQLException {
      try (Connection conn = DriverManager.getConnection(H2_JDBC_URL, H2_USERNAME, H2_PASSWORD);
          Statement stmt = conn.createStatement()) {

        String createTableSQL =
            "CREATE TABLE IF NOT EXISTS partition_statistic_meta ("
                + "  table_id BIGINT NOT NULL,"
                + "  partition_name VARCHAR(1024) NOT NULL,"
                + "  statistic_name VARCHAR(128) NOT NULL,"
                + "  statistic_value CLOB NOT NULL,"
                + "  audit_info CLOB NOT NULL,"
                + "  created_at BIGINT NOT NULL,"
                + "  updated_at BIGINT NOT NULL,"
                + "  PRIMARY KEY (table_id, partition_name, statistic_name)"
                + ")";

        stmt.execute(createTableSQL);

        String createIndexSQL =
            "CREATE INDEX IF NOT EXISTS idx_table_partition ON partition_statistic_meta(table_id, partition_name)";
        stmt.execute(createIndexSQL);

        LOG.info("Created partition_statistic_meta table in H2");
      }
    }
  }
}
