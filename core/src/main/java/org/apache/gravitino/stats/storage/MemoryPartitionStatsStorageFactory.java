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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPartitionStatsStorageFactory implements PartitionStatisticStorageFactory {
  public static final Logger LOG =
      LoggerFactory.getLogger(MemoryPartitionStatsStorageFactory.class);

  @Override
  public PartitionStatisticStorage create(Map<String, String> properties) {
    LOG.warn(
        "The memory partition stats storage is only used for the tests,"
            + "you shouldn't use it in the production environment.");
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

      Map<String, List<PersistedStatistic>> resultStats = Maps.newHashMap();
      for (PersistedPartitionStatistics partitionStat : tableStats.partitionStatistics().values()) {
        String partitionName = partitionStat.partitionName();
        boolean lowerBoundSatisfied =
            isBoundSatisfied(
                range.lowerPartitionName(),
                range.lowerBoundType(),
                partitionName,
                BoundDirection.LOWER);

        boolean upperBoundSatisfied =
            isBoundSatisfied(
                range.upperPartitionName(),
                range.upperBoundType(),
                partitionName,
                BoundDirection.UPPER);

        if (lowerBoundSatisfied && upperBoundSatisfied) {
          resultStats.put(partitionName, Lists.newArrayList(partitionStat.statistics()));
        }
      }
      return resultStats.entrySet().stream()
          .map(entry -> PersistedPartitionStatistics.of(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());
    }

    private static boolean isBoundSatisfied(
        Optional<String> boundPartitionName,
        Optional<PartitionRange.BoundType> boundPartitionType,
        String partitionName,
        BoundDirection boundDirection) {
      return boundPartitionName
          .flatMap(
              targetPartitionName ->
                  boundPartitionType.map(
                      type -> boundDirection.compare(targetPartitionName, partitionName, type)))
          .orElse(true);
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

        for (PartitionStatisticsUpdate updatePartStat : stats) {
          String partitionName = updatePartStat.partitionName();
          Map<String, StatisticValue<?>> partitionStats = updatePartStat.statistics();
          PersistedPartitionStatistics existedPartitionStats =
              tableStats
                  .partitionStatistics()
                  .computeIfAbsent(
                      partitionName,
                      k -> PersistedPartitionStatistics.of(partitionName, Lists.newArrayList()));

          Set<String> existedStats = Sets.newHashSet();
          // Update existed stats
          existedPartitionStats
              .statistics()
              .replaceAll(
                  stat -> {
                    String statName = stat.name();
                    if (partitionStats.containsKey(statName)) {
                      existedStats.add(statName);
                      StatisticValue<?> newValue = partitionStats.get(statName);
                      AuditInfo auditInfo =
                          AuditInfo.builder()
                              .withCreator(stat.auditInfo().creator())
                              .withCreateTime(stat.auditInfo().createTime())
                              .withLastModifiedTime(Instant.now())
                              .withLastModifier(PrincipalUtils.getCurrentUserName())
                              .build();
                      return PersistedStatistic.of(statName, newValue, auditInfo);
                    }
                    return stat;
                  });

          // Add new stats
          for (Map.Entry<String, StatisticValue<?>> statEntry : partitionStats.entrySet()) {
            String statName = statEntry.getKey();
            if (!existedStats.contains(statName)) {
              AuditInfo auditInfo =
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentUserName())
                      .withCreateTime(Instant.now())
                      .withLastModifiedTime(Instant.now())
                      .withLastModifier(PrincipalUtils.getCurrentUserName())
                      .build();
              PersistedStatistic newStat =
                  PersistedStatistic.of(statName, statEntry.getValue(), auditInfo);
              existedPartitionStats.statistics().add(newStat);
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
    public int dropStatistics(String metalake, List<MetadataObjectStatisticsDrop> drops) {
      int deleteCount = 0;
      for (MetadataObjectStatisticsDrop drop : drops) {
        MetadataObject metadataObject = drop.metadataObject();
        List<PartitionStatisticsDrop> partitionsToDrop = drop.drops();
        MetadataObjectStatisticsContainer tableStats =
            totalStatistics.computeIfAbsent(
                new MetadataContainerKey(metalake, metadataObject),
                key -> new MetadataObjectStatisticsContainer(Maps.newHashMap()));

        for (PartitionStatisticsDrop partStats : partitionsToDrop) {
          if (tableStats.partitionStatistics().containsKey(partStats.partitionName())) {
            PersistedPartitionStatistics persistedPartitionStatistics =
                tableStats.partitionStatistics().get(partStats.partitionName());
            Set<String> statsNamesToDelete = Sets.newHashSet(partStats.statisticNames());
            int originCount = persistedPartitionStatistics.statistics().size();
            persistedPartitionStatistics
                .statistics()
                .removeIf(
                    persistedStatistic -> statsNamesToDelete.contains(persistedStatistic.name()));

            deleteCount =
                deleteCount + (originCount - persistedPartitionStatistics.statistics().size());
            if (persistedPartitionStatistics.statistics().isEmpty()) {
              tableStats.partitionStatistics().remove(partStats.partitionName());
            }
          }
        }

        if (tableStats.partitionStatistics().isEmpty()) {
          totalStatistics.remove(new MetadataContainerKey(metalake, metadataObject));
        }
      }
      return deleteCount;
    }

    @Override
    public void close() throws IOException {
      totalStatistics.clear();
    }

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

    public enum BoundDirection {
      LOWER {
        @Override
        boolean compare(
            String targetPartitionName, String partitionName, PartitionRange.BoundType type) {
          int result = targetPartitionName.compareTo(partitionName);
          return type == PartitionRange.BoundType.OPEN ? result < 0 : result <= 0;
        }
      },
      UPPER {
        @Override
        boolean compare(
            String targetPartitionName, String partitionName, PartitionRange.BoundType type) {
          int result = targetPartitionName.compareTo(partitionName);
          return type == PartitionRange.BoundType.OPEN ? result > 0 : result >= 0;
        }
      };

      abstract boolean compare(
          String targetPartitionName, String partitionName, PartitionRange.BoundType boundaryType);
    }
  }
}
