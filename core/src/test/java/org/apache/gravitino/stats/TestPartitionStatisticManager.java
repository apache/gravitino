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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionStatisticManager {

  private static final String METALAKE = "metalake_for_stat_test";

  private static final String CATALOG = "catalog_for_stat_test";

  private static final String SCHEMA = "schema_for_stat_test";

  private static final String TABLE = "table_for_stat_test";

  @Test
  public void testStatisticLifeCycle() {
    Config config = mock(Config.class);
    when(config.get(Configs.PARTITION_STATS_FILE_FACTORY_CLASS_NAME))
        .thenReturn(MemoryPartitionStatsFileFactory.class.getCanonicalName());
    PartitionStatisticManager statisticManager = new PartitionStatisticManager(config);

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
    Map<String, Map<String, StatisticValue<?>>> partitionStatistics = Maps.newHashMap();
    partitionStatistics.put("p0", stats);
    statisticManager.updatePartitionStatistics(METALAKE, tableObject, partitionStatistics);

    Map<String, List<Statistic>> statistics =
        statisticManager.listPartitionStatistics(METALAKE, tableObject, "p0", "p1");
    Assertions.assertEquals(1, statistics.size());
    List<Statistic> listedPartitionStats = statistics.get("p0");
    Assertions.assertEquals(6, listedPartitionStats.size());
    for (Statistic statistic : listedPartitionStats) {
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
    partitionStatistics.put("p0", stats);

    expectStats.put("f", StatisticValues.longValue(2L));
    expectStats.put("x", StatisticValues.longValue(2));

    statisticManager.updatePartitionStatistics(METALAKE, tableObject, partitionStatistics);
    statistics = statisticManager.listPartitionStatistics(METALAKE, tableObject, "p0", "p1");
    Assertions.assertEquals(1, statistics.size());
    listedPartitionStats = statistics.get("p0");
    Assertions.assertEquals(7, listedPartitionStats.size());
    for (Statistic statistic : listedPartitionStats) {
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
    Map<String, List<String>> partitionStatisticsToDrop = Maps.newHashMap();
    partitionStatisticsToDrop.put("p0", statNames);
    statisticManager.dropPartitionStatistics(METALAKE, tableObject, partitionStatisticsToDrop);
    statistics = statisticManager.listPartitionStatistics(METALAKE, tableObject, "p0", "p1");
    Assertions.assertEquals(1, statistics.size());
    listedPartitionStats = statistics.get("p0");
    Assertions.assertEquals(4, listedPartitionStats.size());

    for (Statistic statistic : listedPartitionStats) {
      Assertions.assertTrue(
          expectStats.containsKey(statistic.name()),
          "Statistic name should be in the updated stats: " + statistic.name());
      StatisticValue<?> value = expectStats.get(statistic.name());
      Assertions.assertEquals(
          value, statistic.value().get(), "Statistic value type mismatch: " + statistic.name());
    }
  }
}
