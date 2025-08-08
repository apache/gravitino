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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

public class MemoryPartitionStatsStorageFactory implements PartitionStatisticStorageFactory {

  @Override
  public PartitionStatisticStorage open(Map<String, String> properties) {
    return new MemoryPartitionStatsStorage();
  }

  public static class MemoryPartitionStatsStorage implements PartitionStatisticStorage {
    private static final Map<MetadataContainerKey, MetadataObjectStatisticsContainer>
        totalStatistics = Maps.newConcurrentMap();

    private MemoryPartitionStatsStorage() {}

    @Override
    public List<PersistedPartitionStatistics> listStatistics(
        String metalake, MetadataObject metadataObject, PartitionRange range) {
      MetadataObjectStatisticsContainer tableStats =
          totalStatistics.get(new MetadataContainerKey(metalake, metadataObject));

      if (tableStats == null) {
        return Lists.newArrayList();
      }

      synchronized (tableStats) {
        Map<String, Map<String, StatisticValue<?>>> resultStats = Maps.newHashMap();
        for (PersistedPartitionStatistics partitionStat :
            tableStats.partitionStatistics().values()) {
          String partitionName = partitionStat.partitionName();
          boolean lowerBoundSatisfied =
              range
                  .lowerPartitionName()
                  .flatMap(
                      fromPartitionName ->
                          range
                              .lowerBoundType()
                              .map(
                                  type -> {
                                    if (type == PartitionRange.BoundType.OPEN) {
                                      return partitionName.compareTo(fromPartitionName) > 0;
                                    } else {
                                      return partitionName.compareTo(fromPartitionName) >= 0;
                                    }
                                  }))
                  .orElse(true);

          boolean upperBoundSatisfied =
              range
                  .upperPartitionName()
                  .flatMap(
                      toPartitionName ->
                          range
                              .upperBoundType()
                              .map(
                                  type -> {
                                    if (type == PartitionRange.BoundType.OPEN) {
                                      return partitionName.compareTo(toPartitionName) < 0;
                                    } else {
                                      return partitionName.compareTo(toPartitionName) <= 0;
                                    }
                                  }))
                  .orElse(true);

          if (lowerBoundSatisfied && upperBoundSatisfied) {
            resultStats.put(partitionName, Maps.newHashMap(partitionStat.statistics()));
          }
        }
        return resultStats.entrySet().stream()
            .map(entry -> PersistedPartitionStatistics.of(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
      }
    }

    @Override
    public void updateStatistics(String metalake, List<MetadataObjectStatisticsUpdate> updates) {
      for (MetadataObjectStatisticsUpdate update : updates) {
        MetadataObject metadataObject = update.metadataObject();
        MetadataObjectStatisticsContainer tableStats =
            totalStatistics.computeIfAbsent(
                new MetadataContainerKey(metalake, metadataObject),
                key -> new MetadataObjectStatisticsContainer(Maps.newHashMap()));

        List<PartitionStatisticsUpdate> stats = update.partitionUpdates();
        synchronized (tableStats) {
          for (PartitionStatisticsUpdate updatePartStat : stats) {
            String partitionName = updatePartStat.partitionName();
            Map<String, StatisticValue<?>> partitionStats = updatePartStat.statistics();
            PersistedPartitionStatistics existedPartitionStats =
                tableStats
                    .partitionStatistics()
                    .computeIfAbsent(
                        partitionName,
                        k -> PersistedPartitionStatistics.of(partitionName, new HashMap<>()));
            for (Map.Entry<String, StatisticValue<?>> statEntry : partitionStats.entrySet()) {
              String statName = statEntry.getKey();
              StatisticValue<?> statValue = statEntry.getValue();
              existedPartitionStats.statistics().put(statName, statValue);
            }
          }
        }
      }
    }

    @Override
    public List<PersistedPartitionStatistics> listStatistics(
        String metalake, MetadataObject metadataObject, List<String> partitionNames) {
      throw new UnsupportedOperationException(
          "Don't support listing statistics by partition names");
    }

    @Override
    public void appendStatistics(
        String metalake, List<MetadataObjectStatisticsUpdate> statisticsToAppend) {
      throw new UnsupportedOperationException("Don't support appending statistics");
    }

    @Override
    public void dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> drops) {
      for (MetadataObjectStatisticsDrop drop : drops) {
        MetadataObject metadataObject = drop.metadataObject();
        List<PartitionStatisticsDrop> partitionsToDrop = drop.drops();
        MetadataObjectStatisticsContainer tableStats =
            totalStatistics.computeIfAbsent(
                new MetadataContainerKey(metalake, metadataObject),
                key -> new MetadataObjectStatisticsContainer(Maps.newHashMap()));

        synchronized (tableStats) {
          for (PartitionStatisticsDrop partStats : partitionsToDrop) {
            if (tableStats.partitionStatistics().containsKey(partStats.partitionName())) {
              PersistedPartitionStatistics persistedPartitionStatistics =
                  tableStats.partitionStatistics().get(partStats.partitionName());
              for (String statName : partStats.statisticNames()) {
                persistedPartitionStatistics.statistics().remove(statName);
              }
              if (persistedPartitionStatistics.statistics().isEmpty()) {
                tableStats.partitionStatistics().remove(partStats.partitionName());
              }
            }
          }
        }
      }
    }

    @Override
    public void close() throws IOException {}

    private static class MetadataContainerKey {
      private final String metalake;
      private final MetadataObject metadataObject;

      private MetadataContainerKey(String metalake, MetadataObject metadataObject) {
        this.metalake = metalake;
        this.metadataObject = metadataObject;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetadataContainerKey)) return false;
        MetadataContainerKey that = (MetadataContainerKey) o;
        return Objects.equals(metalake, that.metalake)
            && Objects.equals(metadataObject, that.metadataObject);
      }

      @Override
      public int hashCode() {
        return Objects.hash(metalake, metadataObject);
      }
    }

    private static class MetadataObjectStatisticsContainer {

      private final Map<String, PersistedPartitionStatistics> partitionStatistics;

      private MetadataObjectStatisticsContainer(
          Map<String, PersistedPartitionStatistics> partitionStatistics) {
        this.partitionStatistics = partitionStatistics;
      }

      public Map<String, PersistedPartitionStatistics> partitionStatistics() {
        return partitionStatistics;
      }
    }
  }
}
