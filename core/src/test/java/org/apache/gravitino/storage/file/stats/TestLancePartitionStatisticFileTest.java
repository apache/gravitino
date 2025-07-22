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
package org.apache.gravitino.storage.file.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Test;

public class TestLancePartitionStatisticFileTest extends TestJDBCBackend {

  @Test
  public void testLancePartitionStatisticFile() throws IOException {
    PartitionStatisticFileFactory factory = new LancePartitionStatisticFileFactory();

    // Prepare table entity
    String metalakeName = "metalake";
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
    backend.insert(catalog, false);

    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog"),
            "schema",
            auditInfo);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("metalake", "catalog", "schema"),
            "table",
            auditInfo);
    backend.insert(table, false);

    MetadataObject metadataObject =
        MetadataObjects.of(
            Lists.newArrayList(catalog.name(), schema.name(), table.name()),
            MetadataObject.Type.TABLE);

    // String location = "/tmp/test/" + "test-" + new Random().nextInt() + ".lance";

    int count = 10000000;
    String location = "s3://rorytest2025725/test/" + "test-" + count + ".lance";

    PartitionStatisticFile file =
        factory.open(metalakeName, metadataObject, location, Maps.newHashMap());

    int partitions = count / 10;
    long start;
    long end;
    Map<Long, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate =
        generateData(1L, count, partitions);
    start = System.currentTimeMillis();
    file.updateStatistics(statisticsToUpdate);
    end = System.currentTimeMillis();
    System.out.println("Time cost " + (end - start));

    start = System.currentTimeMillis();
    String fromPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 0);
    String toPartitionName =
        "partition" + String.format("%0" + String.valueOf(partitions).length() + "d", 1);
    Map<String, Map<String, List<StatisticValue<?>>>> stats =
        file.listStatistics(1L, fromPartitionName, toPartitionName);
    end = System.currentTimeMillis();
    System.out.println("Time cost " + (end - start));
    System.out.println("Fetch results: " + stats.size());
    for (Map.Entry<String, Map<String, List<StatisticValue<?>>>> entry : stats.entrySet()) {
      System.out.println("Partition: " + entry.getKey());
      for (Map.Entry<String, List<StatisticValue<?>>> statEntry : entry.getValue().entrySet()) {
        System.out.println("  Statistic: " + statEntry.getKey() + " -> " + statEntry.getValue());
      }
    }

    /*
    start = System.currentTimeMillis();
    Map<Long, Map<String, List<String>>> partitionStatisticsToDrop = Maps.newHashMap();
    partitionStatisticsToDrop
        .computeIfAbsent(1L, k -> Maps.newHashMap())
        .put("partition0", Lists.newArrayList("statistic0"));
    file.dropStatistics(partitionStatisticsToDrop);
    end = System.currentTimeMillis();
    System.out.println("Time cost " + (end - start));
     */

    /**
     * Map<Long, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate =
     * Maps.newHashMap(); statisticsToUpdate .computeIfAbsent(table.id(), k -> Maps.newHashMap())
     * .computeIfAbsent("partition1", kp -> Maps.newHashMap()) .put("statistic1",
     * StatisticValues.stringValue("value1")); file.updateStatistics(statisticsToUpdate);
     *
     * <p>statisticsToUpdate = Maps.newHashMap(); statisticsToUpdate .computeIfAbsent(table.id(), k
     * -> Maps.newHashMap()) .computeIfAbsent("partition2", kp -> Maps.newHashMap())
     * .put("statistic1", StatisticValues.stringValue("value1"));
     * file.updateStatistics(statisticsToUpdate);
     *
     * <p>List<String> partitionNames = Lists.newArrayList("partition1", "partition2"); Map<String,
     * Map<String, List<StatisticValue<?>>>> listedStats = file.listStatistics(table.id(),
     * partitionNames); Assertions.assertEquals(2, listedStats.size()); Map<String,
     * List<StatisticValue<?>>> listPartitionStats = listedStats.get("partition1");
     * List<StatisticValue<?>> list = listPartitionStats.get("statistic1");
     * Assertions.assertEquals(1, list.size());
     * Assertions.assertEquals(StatisticValues.stringValue("value1"), list.get(0));
     *
     * <p>Map<Long, Map<String, List<String>>> partitionStatisticsToDrop = Maps.newHashMap();
     * partitionStatisticsToDrop .computeIfAbsent(table.id(), k -> Maps.newHashMap())
     * .put("partition1", Lists.newArrayList("statistic1", "statistic2"));
     * file.dropStatistics(partitionStatisticsToDrop);
     *
     * <p>listedStats = file.listStatistics(table.id(), partitionNames); Assertions.assertEquals(1,
     * listedStats.size());
     */
    file.close();
  }

  private Map<Long, Map<String, Map<String, StatisticValue<?>>>> generateData(
      Long tableId, int count, int partitions) {
    Map<Long, Map<String, Map<String, StatisticValue<?>>>> statisticsToUpdate = Maps.newHashMap();
    for (int index = 0; index < count; index++) {
      String partitionName =
          "partition"
              + String.format("%0" + String.valueOf(partitions).length() + "d", index % partitions);
      statisticsToUpdate
          .computeIfAbsent(tableId, k -> Maps.newHashMap())
          .computeIfAbsent(partitionName, kp -> Maps.newHashMap())
          .put("statistic" + index, StatisticValues.stringValue("value" + index));
    }
    return statisticsToUpdate;
  }
}
