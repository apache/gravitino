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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.file.stats.PartitionStatisticFile;
import org.apache.gravitino.storage.file.stats.PartitionStatisticFileFactory;

public class MemoryPartitionStatsFileFactory implements PartitionStatisticFileFactory {

  @Override
  public PartitionStatisticFile open(
      String metalake,
      MetadataObject metadataObject,
      String location,
      Map<String, String> properties)
      throws IOException {
    return new MemoryPartitionStatsFile();
  }

  public static class MemoryPartitionStatsFile implements PartitionStatisticFile {
    private static final Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>>
        totalStatistics = Maps.newConcurrentMap();

    @Override
    public String location() {
      return "/tmp";
    }

    @Override
    public String format() {
      return "memory";
    }

    @Override
    public Map<String, Map<String, StatisticValue<?>>> listStatistics(
        MetadataObject metadataObject, String fromPartitionName, String toPartitionName) {
      return totalStatistics.get(metadataObject);
    }

    @Override
    public void updateStatistics(
        Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate) {
      for (Map.Entry<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> entry :
          statisticsToUpdate.entrySet()) {
        MetadataObject metadataObject = entry.getKey();
        Map<String, Map<String, StatisticValue<?>>> stats = entry.getValue();
        Map<String, Map<String, StatisticValue<?>>> innerMap =
            totalStatistics.computeIfAbsent(metadataObject, k -> Maps.newHashMap());
        for (Map.Entry<String, Map<String, StatisticValue<?>>> innerEntry : stats.entrySet()) {
          String partitionName = innerEntry.getKey();
          Map<String, StatisticValue<?>> partitionStats = innerEntry.getValue();
          Map<String, StatisticValue<?>> nestedInnerMap =
              innerMap.computeIfAbsent(partitionName, k -> Maps.newHashMap());
          for (Map.Entry<String, StatisticValue<?>> statEntry : partitionStats.entrySet()) {
            String statName = statEntry.getKey();
            StatisticValue<?> statValue = statEntry.getValue();
            nestedInnerMap.put(statName, statValue);
          }
        }
      }
    }

    @Override
    public void dropStatistics(
        Map<MetadataObject, Map<String, List<String>>> partitionStatisticsToDrop) {
      for (Map.Entry<MetadataObject, Map<String, List<String>>> entry :
          partitionStatisticsToDrop.entrySet()) {
        MetadataObject metadataObject = entry.getKey();
        Map<String, List<String>> partitionsToDrop = entry.getValue();
        Map<String, Map<String, StatisticValue<?>>> innerMap = totalStatistics.get(metadataObject);
        if (innerMap != null) {
          for (Map.Entry<String, List<String>> innerEntry : partitionsToDrop.entrySet()) {
            Map<String, StatisticValue<?>> nestedInnerMap = innerMap.get(innerEntry.getKey());
            for (String statName : innerEntry.getValue()) {
              if (nestedInnerMap != null) {
                nestedInnerMap.remove(statName);
              }
            }
          }
        }
      }
    }

    @Override
    public void close() throws IOException {}
  }
}
