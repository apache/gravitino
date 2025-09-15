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

package org.apache.gravitino.stats;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.storage.MemoryPartitionStatsStorageFactory;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestStatisticManager {
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");

  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final String METALAKE = "metalake_for_stat_test";

  private static final String CATALOG = "catalog_for_stat_test";

  private static final String SCHEMA = "schema_for_stat_test";

  private static final String TABLE = "table_for_stat_test";

  private static final String COLUMN = "column_for_stat_test";
  private static final Config config = Mockito.mock(Config.class);

  private static EntityStore entityStore;

  private static IdGenerator idGenerator;

  @BeforeAll
  public static void setUp() throws IOException, IllegalAccessException {
    idGenerator = new RandomIdGenerator();

    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");
    Mockito.when(config.get(Configs.CACHE_LOCK_SEGMENTS)).thenReturn(16);
    Mockito.when(config.get(Configs.PARTITION_STATS_STORAGE_FACTORY_CLASS))
        .thenReturn(MemoryPartitionStatsStorageFactory.class.getCanonicalName());

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE)
            .withVersion(SchemaVersion.V_0_1)
            .withComment("Test metalake")
            .withAuditInfo(audit)
            .build();
    entityStore.put(metalake, false /* overwritten */);

    CatalogEntity catalog =
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG)
            .withNamespace(Namespace.of(METALAKE))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withComment("Test catalog")
            .withAuditInfo(audit)
            .build();
    entityStore.put(catalog, false /* overwritten */);

    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(SCHEMA)
            .withNamespace(Namespace.of(METALAKE, CATALOG))
            .withComment("Test schema")
            .withAuditInfo(audit)
            .build();
    entityStore.put(schema, false /* overwritten */);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(idGenerator.nextId())
            .withName(COLUMN)
            .withPosition(0)
            .withComment("Test column")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(audit)
            .build();

    TableEntity table =
        TableEntity.builder()
            .withId(idGenerator.nextId())
            .withName(TABLE)
            .withColumns(Lists.newArrayList(column))
            .withNamespace(Namespace.of(METALAKE, CATALOG, SCHEMA))
            .withAuditInfo(audit)
            .build();
    entityStore.put(table, false /* overwritten */);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }

    FileUtils.deleteDirectory(new File(JDBC_STORE_PATH));
  }

  @Test
  public void testStatisticLifeCycle() {
    StatisticManager statisticManager = new StatisticManager(entityStore, idGenerator, config);

    MetadataObject tableObject =
        MetadataObjects.of(Lists.newArrayList(CATALOG, SCHEMA, TABLE), MetadataObject.Type.TABLE);
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    // Update statistics
    stats.put("a", StatisticValues.stringValue("1"));
    stats.put("b", StatisticValues.longValue(1L));
    stats.put("c", StatisticValues.doubleValue(1.0));
    stats.put("d", StatisticValues.booleanValue(true));
    stats.put(
        "e",
        StatisticValues.listValue(
            Lists.newArrayList(
                StatisticValues.stringValue("1"), StatisticValues.stringValue("2"))));

    Map<String, StatisticValue<?>> map = Maps.newHashMap();
    map.put("x", StatisticValues.stringValue("1"));
    map.put("y", StatisticValues.longValue(2L));
    stats.put("f", StatisticValues.objectValue(map));
    statisticManager.updateStatistics(METALAKE, tableObject, stats);

    List<Statistic> statistics = statisticManager.listStatistics(METALAKE, tableObject);
    Assertions.assertEquals(6, statistics.size());
    for (Statistic statistic : statistics) {
      Assertions.assertTrue(
          stats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = stats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }

    // Update partial statistics
    Map<String, StatisticValue<?>> expectStats = Maps.newHashMap();
    expectStats.putAll(stats);
    stats.clear();
    stats.put("f", StatisticValues.longValue(2L));
    stats.put("x", StatisticValues.longValue(2L));

    expectStats.put("f", StatisticValues.longValue(2L));
    expectStats.put("x", StatisticValues.longValue(2));

    statisticManager.updateStatistics(METALAKE, tableObject, stats);
    statistics = statisticManager.listStatistics(METALAKE, tableObject);
    Assertions.assertEquals(7, statistics.size());
    for (Statistic statistic : statistics) {
      Assertions.assertTrue(
          expectStats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = expectStats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }

    // Drop statistics
    expectStats.remove("a");
    expectStats.remove("b");
    expectStats.remove("c");
    List<String> statNames = Lists.newArrayList("a", "b", "c");
    statisticManager.dropStatistics(METALAKE, tableObject, statNames);
    statistics = statisticManager.listStatistics(METALAKE, tableObject);
    Assertions.assertEquals(4, statistics.size());

    for (Statistic statistic : statistics) {
      Assertions.assertTrue(
          expectStats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = expectStats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }

    // List not-existed metadata object statistics
    MetadataObject notExistObject =
        MetadataObjects.of(
            Lists.newArrayList(CATALOG, SCHEMA, "not-exist"), MetadataObject.Type.TABLE);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> statisticManager.listStatistics(METALAKE, notExistObject));

    // Update not-existed metadata object statistics
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> statisticManager.updateStatistics(METALAKE, notExistObject, stats));

    // Drop statistics
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> statisticManager.dropStatistics(METALAKE, notExistObject, statNames));
  }

  @Test
  public void testPartitionStatisticLifeCycle() {
    StatisticManager statisticManager = new StatisticManager(entityStore, idGenerator, config);

    MetadataObject tableObject =
        MetadataObjects.of(Lists.newArrayList(CATALOG, SCHEMA, TABLE), MetadataObject.Type.TABLE);
    Map<String, StatisticValue<?>> stats = Maps.newHashMap();
    // Update statistics
    stats.put("a", StatisticValues.stringValue("1"));
    stats.put("b", StatisticValues.longValue(1L));
    stats.put("c", StatisticValues.doubleValue(1.0));
    stats.put("d", StatisticValues.booleanValue(true));
    stats.put(
        "e",
        StatisticValues.listValue(
            Lists.newArrayList(
                StatisticValues.stringValue("1"), StatisticValues.stringValue("2"))));

    Map<String, StatisticValue<?>> map = Maps.newHashMap();
    map.put("x", StatisticValues.stringValue("1"));
    map.put("y", StatisticValues.longValue(2L));
    stats.put("f", StatisticValues.objectValue(map));
    List<PartitionStatisticsUpdate> partitionStatistics = Lists.newArrayList();
    partitionStatistics.add(PartitionStatisticsModification.update("p0", stats));
    statisticManager.updatePartitionStatistics(METALAKE, tableObject, partitionStatistics);

    List<PartitionStatistics> statistics =
        statisticManager.listPartitionStatistics(
            METALAKE,
            tableObject,
            PartitionRange.between(
                "p0", PartitionRange.BoundType.CLOSED, "p1", PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, statistics.size());
    PartitionStatistics listedPartitionStats = statistics.get(0);
    Assertions.assertEquals(6, listedPartitionStats.statistics().length);
    for (Statistic statistic : listedPartitionStats.statistics()) {
      Assertions.assertTrue(
          stats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = stats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }

    // Update partial statistics
    Map<String, StatisticValue<?>> expectStats = Maps.newHashMap();
    expectStats.putAll(stats);
    stats.clear();
    stats.put("f", StatisticValues.longValue(2L));
    stats.put("x", StatisticValues.longValue(2L));
    partitionStatistics.clear();
    partitionStatistics.add(PartitionStatisticsModification.update("p0", stats));

    expectStats.put("f", StatisticValues.longValue(2L));
    expectStats.put("x", StatisticValues.longValue(2));

    statisticManager.updatePartitionStatistics(METALAKE, tableObject, partitionStatistics);
    statistics =
        statisticManager.listPartitionStatistics(
            METALAKE,
            tableObject,
            PartitionRange.between(
                "p0", PartitionRange.BoundType.CLOSED, "p1", PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, statistics.size());
    listedPartitionStats = statistics.get(0);
    Assertions.assertEquals(7, listedPartitionStats.statistics().length);
    for (Statistic statistic : listedPartitionStats.statistics()) {
      Assertions.assertTrue(
          expectStats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = expectStats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }

    // Drop statistics
    expectStats.remove("a");
    expectStats.remove("b");
    expectStats.remove("c");
    List<String> statNames = Lists.newArrayList("a", "b", "c");
    List<PartitionStatisticsDrop> partitionStatisticsToDrop = Lists.newArrayList();
    partitionStatisticsToDrop.add(PartitionStatisticsModification.drop("p0", statNames));
    statisticManager.dropPartitionStatistics(METALAKE, tableObject, partitionStatisticsToDrop);
    statistics =
        statisticManager.listPartitionStatistics(
            METALAKE,
            tableObject,
            PartitionRange.between(
                "p0", PartitionRange.BoundType.CLOSED, "p1", PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, statistics.size());
    listedPartitionStats = statistics.get(0);
    Assertions.assertEquals(4, listedPartitionStats.statistics().length);

    for (Statistic statistic : listedPartitionStats.statistics()) {
      Assertions.assertTrue(
          expectStats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = expectStats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }
  }
}
