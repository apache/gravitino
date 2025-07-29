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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.file.stats.PartitionStatisticFile;
import org.apache.gravitino.storage.file.stats.PartitionStatisticFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionStatisticManager {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatisticManager.class);
  private static final String OPTIONS_PREFIX = "gravitino.stats.partition.option.";
  private final PartitionStatisticFileFactory factory;
  private final String location;
  private final Map<String, String> options;

  public PartitionStatisticManager(Config config) {
    String className = config.get(Configs.PARTITION_STATS_FILE_FACTORY_CLASS_NAME);
    this.location = config.get(Configs.PARTITION_STATS_FILE_LOCATION);
    this.options = config.getConfigsWithPrefix(OPTIONS_PREFIX);
    try {
      this.factory =
          (PartitionStatisticFileFactory)
              Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error(
          "Failed to create and initialize partition statistics file factory by name {}.",
          className,
          e);
      throw new RuntimeException(
          "Failed to create and initialize partition statistics file factory: " + className, e);
    }
  }

  public boolean dropPartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      Map<String, List<String>> partitionStatistics) {
    try (PartitionStatisticFile file = factory.open(metalake, metadataObject, location, options)) {
      Map<MetadataObject, Map<String, List<String>>> statsToDrop = Maps.newHashMap();
      statsToDrop.put(metadataObject, partitionStatistics);
      file.dropStatistics(statsToDrop);
      return true;
    } catch (IOException ioe) {
      LOG.error(
          "Failed to drop partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(
          "Failed to drop partition statistics for " + metadataObject + " in metalake " + metalake,
          ioe);
    }
  }

  public void updatePartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      Map<String, Map<String, StatisticValue<?>>> partitionStatistics) {
    try (PartitionStatisticFile file = factory.open(metalake, metadataObject, location, options)) {
      Map<MetadataObject, Map<String, Map<String, StatisticValue<?>>>> partitionStats =
          Maps.newHashMap();
      partitionStats.put(metadataObject, partitionStatistics);
      file.updateStatistics(partitionStats);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to update partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(
          "Failed to update partition statistics for "
              + metadataObject
              + " in metalake "
              + metalake,
          ioe);
    }
  }

  public Map<String, List<Statistic>> listPartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      String fromPartitionName,
      String toPartitionName) {
    try (PartitionStatisticFile file = factory.open(metalake, metadataObject, location, options)) {
      Map<String, Map<String, StatisticValue<?>>> stats =
          file.listStatistics(metadataObject, fromPartitionName, toPartitionName);
      Map<String, List<Statistic>> listedStats = Maps.newHashMap();
      stats.forEach(
          (key, value) -> {
            List<Statistic> statisticList = Lists.newArrayList();
            for (Map.Entry<String, StatisticValue<?>> entry : value.entrySet()) {
              statisticList.add(new CustomStatistic(entry.getKey(), entry.getValue()));
            }
            listedStats.put(key, statisticList);
          });
      return listedStats;
    } catch (IOException ioe) {
      LOG.error(
          "Failed to list partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(
          "Failed to list partition statistics for " + metadataObject + " in metalake " + metalake,
          ioe);
    }
  }

  private static class CustomStatistic implements Statistic {
    private final String name;
    private final StatisticValue<?> value;

    public CustomStatistic(String name, StatisticValue<?> value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Optional<StatisticValue<?>> value() {
      return Optional.of(value);
    }

    @Override
    public boolean reserved() {
      return false;
    }

    @Override
    public boolean modifiable() {
      return true;
    }
  }
}
