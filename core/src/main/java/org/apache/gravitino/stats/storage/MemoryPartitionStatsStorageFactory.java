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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

public class MemoryPartitionStatsStorageFactory implements PartitionStatisticStorageFactory {

  @Override
  public PartitionStatisticStorage open(String metalake, Map<String, String> properties) {
    return new MemoryPartitionStatsStorage(metalake, properties);
  }

  public static class MemoryPartitionStatsStorage implements PartitionStatisticStorage {
    private static final Map<
            String, Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>>>
        totalStatistics = Maps.newConcurrentMap();

    private final String metalake;
    private final Map<String, String> properties;

    private MemoryPartitionStatsStorage(String metalake, Map<String, String> properties) {
      this.metalake = metalake;
      this.properties = properties;
    }

    @Override
    public Map<String, Map<String, StatisticValue<?>>> listStatistics(
        MetadataObject metadataObject, PartitionRange range) {
      Map<String, Map<String, StatisticValue<?>>> tableStats =
          totalStatistics
              .computeIfAbsent(metalake, k -> Maps.newConcurrentMap())
              .computeIfAbsent(metadataObject, k -> Maps.newConcurrentMap());
      Map<String, Map<String, StatisticValue<?>>> resultStats = Maps.newHashMap();

      for (Map.Entry<String, Map<String, StatisticValue<?>>> entry : tableStats.entrySet()) {
        String partitionName = entry.getKey();
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

        if (lowerBoundSatisfied && upperBoundSatisfied && entry.getValue() != null) {
          resultStats.put(partitionName, entry.getValue());
        }
      }
      return resultStats;
    }

    @Override
    public String metalake() {
      return metalake;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public void updateStatistics(
        Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToUpdate) {
      for (Map.Entry<MetadataObject, List<PartitionStatisticsUpdate>> entry :
          statisticsToUpdate.entrySet()) {
        MetadataObject metadataObject = entry.getKey();
        List<PartitionStatisticsUpdate> stats = entry.getValue();
        Map<String, Map<String, StatisticValue<?>>> innerMap =
            totalStatistics
                .computeIfAbsent(metalake, k -> Maps.newConcurrentMap())
                .computeIfAbsent(metadataObject, k -> Maps.newConcurrentMap());
        for (PartitionStatisticsUpdate partStat : stats) {
          String partitionName = partStat.partitionName();
          Map<String, StatisticValue<?>> partitionStats = partStat.statistics();
          Map<String, StatisticValue<?>> nestedInnerMap =
              innerMap.computeIfAbsent(partitionName, k -> Maps.newConcurrentMap());
          for (Map.Entry<String, StatisticValue<?>> statEntry : partitionStats.entrySet()) {
            String statName = statEntry.getKey();
            StatisticValue<?> statValue = statEntry.getValue();
            nestedInnerMap.put(statName, statValue);
          }
        }
      }
    }

    @Override
    public Map<String, Map<String, StatisticValue<?>>> listStatistics(
        MetadataObject metadataObject, List<String> partitionNames) {
      throw new UnsupportedOperationException("Don't support listing statistics by partition names");
    }

    @Override
    public void appendStatistics(
        Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToAppend) {
      throw new UnsupportedOperationException("Don't support appending statistics");
    }

    @Override
    public void dropStatistics(
        Map<MetadataObject, List<PartitionStatisticsDrop>> partitionStatisticsToDrop) {
      for (Map.Entry<MetadataObject, List<PartitionStatisticsDrop>> entry :
          partitionStatisticsToDrop.entrySet()) {
        MetadataObject metadataObject = entry.getKey();
        List<PartitionStatisticsDrop> partitionsToDrop = entry.getValue();
        Map<String, Map<String, StatisticValue<?>>> innerMap =
            totalStatistics
                .computeIfAbsent(metalake, k -> Maps.newConcurrentMap())
                .computeIfAbsent(metadataObject, k -> Maps.newConcurrentMap());
        for (PartitionStatisticsDrop partStats : partitionsToDrop) {
          for (String statName : partStats.statisticNames()) {
            if (innerMap.containsKey(partStats.partitionName())) {
              innerMap.get(partStats.partitionName()).remove(statName);
            }
          }
        }
      }
    }

    @Override
    public void close() throws IOException {}
  }
}
