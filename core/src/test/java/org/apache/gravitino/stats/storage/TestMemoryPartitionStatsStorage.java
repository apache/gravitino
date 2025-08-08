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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryPartitionStatsStorage {

  @Test
  public void testMemoryPartitionStatsStorage() throws IOException {
    MemoryPartitionStatsStorageFactory factory = new MemoryPartitionStatsStorageFactory();
    try (PartitionStatisticStorage storage = factory.open("testMetalake", Maps.newHashMap())) {
      MetadataObject metadataObject =
          MetadataObjects.of(
              Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

      Map<String, Map<String, StatisticValue<?>>> stats =
          storage.listStatistics(
              metadataObject, PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(0, stats.size());

      Map<String, StatisticValue<?>> statistics = Maps.newHashMap();
      statistics.put("k1", StatisticValues.stringValue("v1"));
      PartitionStatisticsUpdate update = PartitionStatisticsUpdate.of("p0", statistics);
      Map<MetadataObject, List<PartitionStatisticsUpdate>> statisticsToUpdate = Maps.newHashMap();
      statisticsToUpdate.put(metadataObject, Lists.newArrayList(update));
      storage.updateStatistics(statisticsToUpdate);

      stats =
          storage.listStatistics(
              metadataObject, PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(1, stats.size());
      Assertions.assertTrue(stats.containsKey("p0"));
      Map<String, StatisticValue<?>> partitionStats = stats.get("p0");
      Assertions.assertEquals(1, partitionStats.size());
      Assertions.assertTrue(partitionStats.containsKey("k1"));
      StatisticValue<?> value = partitionStats.get("k1");
      Assertions.assertEquals(StatisticValues.stringValue("v1"), value);

      PartitionStatisticsDrop drop = PartitionStatisticsDrop.of("p0", Lists.newArrayList("k1"));
      List<PartitionStatisticsDrop> drops = Lists.newArrayList(drop);

      Map<MetadataObject, List<PartitionStatisticsDrop>> statisticsToDrop = Maps.newHashMap();
      statisticsToDrop.put(metadataObject, drops);
      storage.dropStatistics(statisticsToDrop);

      stats =
          storage.listStatistics(
              metadataObject, PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(1, stats.size());
      Assertions.assertTrue(stats.containsKey("p0"));
      partitionStats = stats.get("p0");
      Assertions.assertEquals(0, partitionStats.size());
    }
  }
}
