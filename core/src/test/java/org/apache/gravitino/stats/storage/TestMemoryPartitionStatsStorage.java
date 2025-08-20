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
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryPartitionStatsStorage {

  @Test
  public void testMemoryPartitionStatsStorage() throws IOException {
    MemoryPartitionStatsStorageFactory factory = new MemoryPartitionStatsStorageFactory();
    try (PartitionStatisticStorage storage = factory.create(Maps.newHashMap())) {
      MetadataObject metadataObject =
          MetadataObjects.of(
              Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

      List<PersistedPartitionStatistics> stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(0, stats.size());

      Map<String, StatisticValue<?>> statistics = Maps.newHashMap();
      statistics.put("k1", StatisticValues.stringValue("v1"));
      PartitionStatisticsUpdate updateP0 = PartitionStatisticsModification.update("p0", statistics);
      PartitionStatisticsUpdate updateP1 = PartitionStatisticsModification.update("p1", statistics);
      MetadataObjectStatisticsUpdate metadataObjectStatisticsUpdate =
          MetadataObjectStatisticsUpdate.of(metadataObject, Lists.newArrayList(updateP0, updateP1));
      storage.updateStatistics("metalake", Lists.newArrayList(metadataObjectStatisticsUpdate));

      // case 1: closed lower bound
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.downTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(2, stats.size());

      // case 2: open upper bound
      stats =
          storage.listStatistics(
              "metalake", metadataObject, PartitionRange.upTo("p0", PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(0, stats.size());

      // case 3: open lower bound
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.downTo("p0", PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(1, stats.size());

      // case 4: closed upper bound
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(1, stats.size());
      List<PersistedStatistic> partitionStats = stats.get(0).statistics();

      Assertions.assertEquals(1, partitionStats.size());
      Assertions.assertEquals(partitionStats.get(0).name(), "k1");
      StatisticValue<?> value = partitionStats.get(0).value();
      Assertions.assertEquals(StatisticValues.stringValue("v1"), value);

      // case 5: between p0 and p1 with [closed, open)
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.between(
                  "p0", PartitionRange.BoundType.CLOSED, "p1", PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(1, stats.size());

      // case 6: between p0 and p1 with (open, closed]
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.between(
                  "p0", PartitionRange.BoundType.OPEN, "p1", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(1, stats.size());

      // case 7: between p0 and p1 with (open, open)
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.between(
                  "p0", PartitionRange.BoundType.OPEN, "p1", PartitionRange.BoundType.OPEN));
      Assertions.assertEquals(0, stats.size());

      // case 8: between p0 and p1 with [closed, closed]
      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.between(
                  "p0", PartitionRange.BoundType.CLOSED, "p1", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(2, stats.size());

      PartitionStatisticsDrop drop =
          PartitionStatisticsModification.drop("p0", Lists.newArrayList("k1"));
      List<PartitionStatisticsDrop> drops = Lists.newArrayList(drop);

      List<MetadataObjectStatisticsDrop> partitionStatisticsToDrop =
          Lists.newArrayList(MetadataObjectStatisticsDrop.of(metadataObject, drops));
      storage.dropStatistics("metalake", partitionStatisticsToDrop);

      stats =
          storage.listStatistics(
              "metalake",
              metadataObject,
              PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
      Assertions.assertEquals(0, stats.size());
    }
  }
}
