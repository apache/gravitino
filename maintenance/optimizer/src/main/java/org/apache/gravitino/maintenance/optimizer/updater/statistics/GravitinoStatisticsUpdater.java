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

package org.apache.gravitino.maintenance.optimizer.updater.statistics;

import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.updater.StatisticsUpdater;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

/** Statistics updater that persists table/partition statistics to Gravitino. */
public class GravitinoStatisticsUpdater implements StatisticsUpdater {

  public static final String NAME = "gravitino-statistics-updater";
  private GravitinoClient gravitinoClient;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
  }

  @Override
  public void updateTableStatistics(
      NameIdentifier tableIdentifier, List<StatisticEntry<?>> tableStatistics) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    ensureInitialized();
    doUpdateTableStatistics(tableIdentifier, getTableStatisticsMap(tableStatistics));
  }

  @Override
  public void updatePartitionStatistics(
      NameIdentifier tableIdentifier,
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    ensureInitialized();
    doUpdatePartitionStatistics(
        tableIdentifier, getPartitionStatisticsUpdates(partitionStatistics));
  }

  private void doUpdateTableStatistics(
      NameIdentifier tableIdentifier, Map<String, StatisticValue<?>> tableStatisticsMap) {
    if (tableStatisticsMap.isEmpty()) {
      return;
    }
    gravitinoClient
        .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier))
        .supportsStatistics()
        .updateStatistics(tableStatisticsMap);
  }

  private Map<String, StatisticValue<?>> getTableStatisticsMap(List<StatisticEntry<?>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return Map.of();
    }
    return toStatisticValueMap(statistics, "table statistics");
  }

  private List<PartitionStatisticsUpdate> getPartitionStatisticsUpdates(
      Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      return List.of();
    }
    return partitionStatistics.entrySet().stream()
        .map(
            entry -> {
              Preconditions.checkArgument(
                  entry.getKey() != null, "partition path must not be null");
              return new PartitionStatisticsUpdate() {
                @Override
                public String partitionName() {
                  return PartitionUtils.encodePartitionPath(entry.getKey());
                }

                @Override
                public Map<String, StatisticValue<?>> statistics() {
                  return toStatisticValueMap(entry.getValue(), "partition statistics");
                }
              };
            })
        .collect(Collectors.toList());
  }

  private Map<String, StatisticValue<?>> toStatisticValueMap(
      List<StatisticEntry<?>> statistics, String context) {
    Preconditions.checkArgument(statistics != null, "%s list must not be null", context);
    Map<String, StatisticValue<?>> result = new LinkedHashMap<>();
    for (StatisticEntry<?> statistic : statistics) {
      Preconditions.checkArgument(statistic != null, "%s entry must not be null", context);
      Preconditions.checkArgument(statistic.name() != null, "%s name must not be null", context);
      Preconditions.checkArgument(statistic.value() != null, "%s value must not be null", context);
      result.put(statistic.name(), statistic.value());
    }
    return result;
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        gravitinoClient != null,
        "GravitinoStatisticsUpdater has not been initialized. Call initialize(optimizerEnv) first.");
  }

  private void doUpdatePartitionStatistics(
      NameIdentifier tableIdentifier, List<PartitionStatisticsUpdate> partitionStatisticsUpdates) {
    if (partitionStatisticsUpdates.isEmpty()) {
      return;
    }
    gravitinoClient
        .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier))
        .supportsPartitionStatistics()
        .updatePartitionStatistics(partitionStatisticsUpdates);
  }

  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
