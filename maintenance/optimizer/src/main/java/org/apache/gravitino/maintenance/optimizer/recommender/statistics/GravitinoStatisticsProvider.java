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

package org.apache.gravitino.maintenance.optimizer.recommender.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.recommender.SupportTableStatistics;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

/** Statistics provider that reads table and partition statistics from Gravitino. */
public class GravitinoStatisticsProvider implements SupportTableStatistics {

  public static final String NAME = "gravitino-statistics-provider";
  private GravitinoClient gravitinoClient;

  /**
   * Initializes the provider with a Gravitino client derived from the optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
  }

  /**
   * Returns table-level statistics for the given table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return list of statistics entries
   */
  @Override
  public List<StatisticEntry<?>> tableStatistics(NameIdentifier tableIdentifier) {
    IdentifierUtils.requireTableIdentifierNormalized(tableIdentifier);
    Table t =
        gravitinoClient
            .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    List<Statistic> statistics = t.supportsStatistics().listStatistics();
    return statistics.stream()
        .filter(statistic -> statistic.value().isPresent())
        .map(
            statistic ->
                (StatisticEntry<?>)
                    new StatisticEntryImpl(statistic.name(), statistic.value().get()))
        .collect(Collectors.toList());
  }

  /**
   * Returns partition-level statistics for the given table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return statistics grouped by partition path
   */
  @Override
  public Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics(
      NameIdentifier tableIdentifier) {
    IdentifierUtils.requireTableIdentifierNormalized(tableIdentifier);
    Table t =
        gravitinoClient
            .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
            .asTableCatalog()
            .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
    List<PartitionStatistics> partitionStatistics =
        t.supportsPartitionStatistics().listPartitionStatistics(PartitionRange.ALL_PARTITIONS);

    Map<PartitionPath, List<StatisticEntry<?>>> statisticsByPartition = new LinkedHashMap<>();
    partitionStatistics.forEach(
        statistic -> toPartitionStatistics(statistic, statisticsByPartition));
    return statisticsByPartition;
  }

  private void toPartitionStatistics(
      PartitionStatistics partitionStatistics,
      Map<PartitionPath, List<StatisticEntry<?>>> statisticsByPartition) {
    PartitionPath partitions =
        PartitionUtils.decodePartitionPath(partitionStatistics.partitionName());
    Arrays.stream(partitionStatistics.statistics())
        .filter(statistic -> statistic.value().isPresent())
        .forEach(
            statistic ->
                statisticsByPartition
                    .computeIfAbsent(partitions, key -> new ArrayList<>())
                    .add(new StatisticEntryImpl<>(statistic.name(), statistic.value().get())));
  }

  /**
   * Returns the provider name for configuration lookup.
   *
   * @return provider name
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * Closes the underlying Gravitino client.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
