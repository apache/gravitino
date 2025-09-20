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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class TestLancePartitionStatisticStorage {

  @Test
  public void testLancePartitionStatisticStorage() throws Exception {
    PartitionStatisticStorageFactory factory = new LancePartitionStatisticStorageFactory();

    // Prepare table entity
    String metalakeName = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema";
    String tableName = "table";

    MetadataObject metadataObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);

    EntityStore entityStore = mock(EntityStore.class);
    TableEntity tableEntity = mock(TableEntity.class);
    when(entityStore.get(any(), any(), any())).thenReturn(tableEntity);
    when(tableEntity.id()).thenReturn(1L);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);

    String location = Files.createTempDirectory("lance_stats_test").toString();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("location", location);

    LancePartitionStatisticStorage storage =
        (LancePartitionStatisticStorage) factory.create(properties);

    int count = 100;
    int partitions = 10;
    Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> originData =
        generateData(metadataObject, count, partitions);
    Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToUpdate =
        convertData(originData);

    List<MetadataObjectStatisticsUpdate> objectUpdates = Lists.newArrayList();
    for (Map.Entry<MetadataObject, List<PartitionStatisticsUpdate>> entry :
        statisticsToUpdate.entrySet()) {
      MetadataObject metadata = entry.getKey();
      List<PartitionStatisticsUpdate> updates = entry.getValue();
      objectUpdates.add(MetadataObjectStatisticsUpdate.of(metadata, updates));
    }
    storage.updateStatistics(metalakeName, objectUpdates);

    String fromPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 0);
    String toPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 1);

    List<PersistedPartitionStatistics> listedStats =
        storage.listStatistics(
            metalakeName,
            metadataObject,
            PartitionRange.between(
                fromPartitionName,
                PartitionRange.BoundType.CLOSED,
                toPartitionName,
                PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, listedStats.size());

    String targetPartitionName = "partition00";
    for (PersistedPartitionStatistics persistStat : listedStats) {
      String partitionName = persistStat.partitionName();
      List<PersistedStatistic> stats = persistStat.statistics();
      Assertions.assertEquals(targetPartitionName, partitionName);
      Assertions.assertEquals(10, stats.size());

      for (PersistedStatistic statistic : stats) {
        String statisticName = statistic.name();
        StatisticValue<?> statisticValue = statistic.value();

        Assertions.assertTrue(
            originData.get(metadataObject).get(targetPartitionName).containsKey(statisticName));
        Assertions.assertEquals(
            originData.get(metadataObject).get(targetPartitionName).get(statisticName).value(),
            statisticValue.value());
        Assertions.assertNotNull(statistic.auditInfo());
      }
    }

    // Drop one statistic from partition00
    List<MetadataObjectStatisticsDrop> tableStatisticsToDrop =
        Lists.newArrayList(
            MetadataObjectStatisticsDrop.of(
                metadataObject,
                Lists.newArrayList(
                    PartitionStatisticsModification.drop(
                        targetPartitionName, Lists.newArrayList("statistic0")))));

    storage.dropStatistics(metalakeName, tableStatisticsToDrop);

    listedStats =
        storage.listStatistics(
            metalakeName,
            metadataObject,
            PartitionRange.between(
                fromPartitionName,
                PartitionRange.BoundType.CLOSED,
                toPartitionName,
                PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, listedStats.size());

    for (PersistedPartitionStatistics partitionStat : listedStats) {
      String partitionName = partitionStat.partitionName();
      List<PersistedStatistic> stats = partitionStat.statistics();
      Assertions.assertEquals(targetPartitionName, partitionName);
      Assertions.assertEquals(9, stats.size());

      for (PersistedStatistic statistic : stats) {
        String statisticName = statistic.name();
        StatisticValue<?> statisticValue = statistic.value();

        Assertions.assertTrue(
            originData.get(metadataObject).get(targetPartitionName).containsKey(statisticName));
        Assertions.assertEquals(
            originData.get(metadataObject).get(targetPartitionName).get(statisticName).value(),
            statisticValue.value());
        Assertions.assertNotNull(statistic.auditInfo());
      }

      // Drop one statistics from partition01 and partition02
      tableStatisticsToDrop =
          Lists.newArrayList(
              MetadataObjectStatisticsDrop.of(
                  metadataObject,
                  Lists.newArrayList(
                      PartitionStatisticsModification.drop(
                          "partition01", Lists.newArrayList("statistic1")),
                      PartitionStatisticsModification.drop(
                          "partition02", Lists.newArrayList("statistic2")))));
      storage.dropStatistics(metalakeName, tableStatisticsToDrop);

      listedStats =
          storage.listStatistics(
              metalakeName,
              metadataObject,
              PartitionRange.between(
                  fromPartitionName,
                  PartitionRange.BoundType.CLOSED,
                  "partition03",
                  PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(3, listedStats.size());
      for (PersistedPartitionStatistics persistPartStat : listedStats) {
        stats = persistPartStat.statistics();
        Assertions.assertEquals(9, stats.size());
        for (PersistedStatistic statistic : stats) {
          partitionName = persistPartStat.partitionName();
          String statisticName = statistic.name();
          StatisticValue<?> statisticValue = statistic.value();

          Assertions.assertTrue(
              originData.get(metadataObject).get(partitionName).containsKey(statisticName));
          Assertions.assertEquals(
              originData.get(metadataObject).get(partitionName).get(statisticName).value(),
              statisticValue.value());
          Assertions.assertNotNull(statistic.auditInfo());
        }
      }
    }

    FileUtils.deleteDirectory(new File(location + "/" + tableEntity.id() + ".lance"));
    storage.close();
  }

  @Test
  public void testLancePartitionStatisticStorageWithCache() throws Exception {
    PartitionStatisticStorageFactory factory = new LancePartitionStatisticStorageFactory();

    // Prepare table entity
    String metalakeName = "metalake";
    String catalogName = "catalog";
    String schemaName = "schema";
    String tableName = "table";

    MetadataObject metadataObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogName, schemaName, tableName), MetadataObject.Type.TABLE);

    EntityStore entityStore = mock(EntityStore.class);
    TableEntity tableEntity = mock(TableEntity.class);
    when(entityStore.get(any(), any(), any())).thenReturn(tableEntity);
    when(tableEntity.id()).thenReturn(1L);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);

    String location = Files.createTempDirectory("lance_stats_test").toString();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("location", location);
    properties.put("datasetCacheSize", "1000");

    LancePartitionStatisticStorage storage =
        (LancePartitionStatisticStorage) factory.create(properties);

    int count = 100;
    int partitions = 10;
    Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> originData =
        generateData(metadataObject, count, partitions);
    Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToUpdate =
        convertData(originData);

    List<MetadataObjectStatisticsUpdate> objectUpdates = Lists.newArrayList();
    for (Map.Entry<MetadataObject, List<PartitionStatisticsUpdate>> entry :
        statisticsToUpdate.entrySet()) {
      MetadataObject metadata = entry.getKey();
      List<PartitionStatisticsUpdate> updates = entry.getValue();
      objectUpdates.add(MetadataObjectStatisticsUpdate.of(metadata, updates));
    }
    storage.updateStatistics(metalakeName, objectUpdates);
    Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());

    String fromPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 0);
    String toPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 1);

    List<PersistedPartitionStatistics> listedStats =
        storage.listStatistics(
            metalakeName,
            metadataObject,
            PartitionRange.between(
                fromPartitionName,
                PartitionRange.BoundType.CLOSED,
                toPartitionName,
                PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, listedStats.size());
    Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());

    String targetPartitionName = "partition00";
    for (PersistedPartitionStatistics persistStat : listedStats) {
      String partitionName = persistStat.partitionName();
      List<PersistedStatistic> stats = persistStat.statistics();
      Assertions.assertEquals(targetPartitionName, partitionName);
      Assertions.assertEquals(10, stats.size());

      for (PersistedStatistic statistic : stats) {
        String statisticName = statistic.name();
        StatisticValue<?> statisticValue = statistic.value();

        Assertions.assertTrue(
            originData.get(metadataObject).get(targetPartitionName).containsKey(statisticName));
        Assertions.assertEquals(
            originData.get(metadataObject).get(targetPartitionName).get(statisticName).value(),
            statisticValue.value());
        Assertions.assertNotNull(statistic.auditInfo());
      }
    }

    // Drop one statistic from partition00
    List<MetadataObjectStatisticsDrop> tableStatisticsToDrop =
        Lists.newArrayList(
            MetadataObjectStatisticsDrop.of(
                metadataObject,
                Lists.newArrayList(
                    PartitionStatisticsModification.drop(
                        targetPartitionName, Lists.newArrayList("statistic0")))));

    storage.dropStatistics(metalakeName, tableStatisticsToDrop);
    Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());

    listedStats =
        storage.listStatistics(
            metalakeName,
            metadataObject,
            PartitionRange.between(
                fromPartitionName,
                PartitionRange.BoundType.CLOSED,
                toPartitionName,
                PartitionRange.BoundType.OPEN));
    Assertions.assertEquals(1, listedStats.size());
    Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());

    for (PersistedPartitionStatistics partitionStat : listedStats) {
      String partitionName = partitionStat.partitionName();
      List<PersistedStatistic> stats = partitionStat.statistics();
      Assertions.assertEquals(targetPartitionName, partitionName);
      Assertions.assertEquals(9, stats.size());

      for (PersistedStatistic statistic : stats) {
        String statisticName = statistic.name();
        StatisticValue<?> statisticValue = statistic.value();

        Assertions.assertTrue(
            originData.get(metadataObject).get(targetPartitionName).containsKey(statisticName));
        Assertions.assertEquals(
            originData.get(metadataObject).get(targetPartitionName).get(statisticName).value(),
            statisticValue.value());
        Assertions.assertNotNull(statistic.auditInfo());
      }

      // Drop one statistics from partition01 and partition02
      tableStatisticsToDrop =
          Lists.newArrayList(
              MetadataObjectStatisticsDrop.of(
                  metadataObject,
                  Lists.newArrayList(
                      PartitionStatisticsModification.drop(
                          "partition01", Lists.newArrayList("statistic1")),
                      PartitionStatisticsModification.drop(
                          "partition02", Lists.newArrayList("statistic2")))));
      storage.dropStatistics(metalakeName, tableStatisticsToDrop);
      Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());

      listedStats =
          storage.listStatistics(
              metalakeName,
              metadataObject,
              PartitionRange.between(
                  fromPartitionName,
                  PartitionRange.BoundType.CLOSED,
                  "partition03",
                  PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(3, listedStats.size());
      Assertions.assertEquals(1, storage.getDatasetCache().estimatedSize());
      for (PersistedPartitionStatistics persistPartStat : listedStats) {
        stats = persistPartStat.statistics();
        Assertions.assertEquals(9, stats.size());
        for (PersistedStatistic statistic : stats) {
          partitionName = persistPartStat.partitionName();
          String statisticName = statistic.name();
          StatisticValue<?> statisticValue = statistic.value();

          Assertions.assertTrue(
              originData.get(metadataObject).get(partitionName).containsKey(statisticName));
          Assertions.assertEquals(
              originData.get(metadataObject).get(partitionName).get(statisticName).value(),
              statisticValue.value());
          Assertions.assertNotNull(statistic.auditInfo());
        }
      }
    }

    FileUtils.deleteDirectory(new File(location + "/" + tableEntity.id() + ".lance"));
    storage.close();
  }

  private Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> generateData(
      MetadataObject metadataObject, int count, int partitions) {
    Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate =
        Maps.newHashMap();
    for (int index = 0; index < count; index++) {
      String partitionName =
          "partition"
              + String.format("%0" + String.valueOf(partitions).length() + "d", index % partitions);
      statisticsToUpdate
          .computeIfAbsent(metadataObject, k -> Maps.newHashMap())
          .computeIfAbsent(partitionName, kp -> Maps.newHashMap())
          .put("statistic" + index, StatisticValues.stringValue("value" + index));
    }
    return statisticsToUpdate;
  }

  private static Map<MetadataObject, List<PartitionStatisticsUpdate>> convertData(
      Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate) {
    Map<MetadataObject, List<PartitionStatisticsUpdate>> newData = Maps.newHashMap();
    for (Map.Entry<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> tableStatistic :
        statisticsToUpdate.entrySet()) {
      List<PartitionStatisticsUpdate> list = Lists.newArrayList();
      newData.put(tableStatistic.getKey(), list);
      for (Map.Entry<String, Map<String, StatisticValue<?>>> partitionStatistic :
          tableStatistic.getValue().entrySet()) {
        String partitionName = partitionStatistic.getKey();
        Map<String, StatisticValue<?>> stats = partitionStatistic.getValue();
        PartitionStatisticsUpdate update =
            PartitionStatisticsModification.update(partitionName, stats);
        list.add(update);
      }
    }
    return newData;
  }
}
