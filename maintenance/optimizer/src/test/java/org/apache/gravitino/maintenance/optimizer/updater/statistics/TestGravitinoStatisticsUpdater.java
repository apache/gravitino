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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoStatisticsUpdater {

  @Test
  void testUpdateTableStatisticsWithoutInitializeFails() {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    IllegalStateException exception =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                updater.updateTableStatistics(
                    NameIdentifier.of("catalog", "db", "table"), List.of()));
    Assertions.assertTrue(exception.getMessage().contains("has not been initialized"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUpdateTableStatisticsDuplicateNameLastWins() throws Exception {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    GravitinoClient client = Mockito.mock(GravitinoClient.class, Mockito.RETURNS_DEEP_STUBS);
    setClient(updater, client);
    NameIdentifier tableIdentifier = NameIdentifier.of("catalog", "db", "table");

    updater.updateTableStatistics(
        tableIdentifier,
        List.of(stat("row_count", 10L), stat("row_count", 20L), stat("size", 30L)));

    ArgumentCaptor<Map<String, StatisticValue<?>>> captor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(
            client
                .loadCatalog("catalog")
                .asTableCatalog()
                .loadTable(NameIdentifier.of("db", "table"))
                .supportsStatistics())
        .updateStatistics(captor.capture());
    Map<String, StatisticValue<?>> result = captor.getValue();
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(20L, result.get("row_count").value());
    Assertions.assertEquals(30L, result.get("size").value());
  }

  @Test
  void testUpdatePartitionStatisticsNullPartitionPathFails() throws Exception {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    GravitinoClient client = Mockito.mock(GravitinoClient.class, Mockito.RETURNS_DEEP_STUBS);
    setClient(updater, client);

    Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics = new HashMap<>();
    partitionStatistics.put(null, List.of(stat("s1", 1L)));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                updater.updatePartitionStatistics(
                    NameIdentifier.of("catalog", "db", "table"), partitionStatistics));
    Assertions.assertTrue(exception.getMessage().contains("partition path must not be null"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testUpdatePartitionStatisticsDuplicateNameLastWins() throws Exception {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    GravitinoClient client = Mockito.mock(GravitinoClient.class, Mockito.RETURNS_DEEP_STUBS);
    setClient(updater, client);
    NameIdentifier tableIdentifier = NameIdentifier.of("catalog", "db", "table");
    PartitionPath partitionPath = PartitionPath.of(List.of(new PartitionEntryImpl("p", "1")));

    updater.updatePartitionStatistics(
        tableIdentifier,
        Map.of(partitionPath, List.of(stat("s1", 1L), stat("s1", 2L), stat("s2", 3L))));

    ArgumentCaptor<List<PartitionStatisticsUpdate>> captor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(
            client
                .loadCatalog("catalog")
                .asTableCatalog()
                .loadTable(NameIdentifier.of("db", "table"))
                .supportsPartitionStatistics())
        .updatePartitionStatistics(captor.capture());
    List<PartitionStatisticsUpdate> updates = captor.getValue();
    Assertions.assertEquals(1, updates.size());
    Assertions.assertEquals(
        PartitionUtils.encodePartitionPath(partitionPath), updates.get(0).partitionName());
    Assertions.assertEquals(2L, updates.get(0).statistics().get("s1").value());
    Assertions.assertEquals(3L, updates.get(0).statistics().get("s2").value());
  }

  private StatisticEntry<?> stat(String name, long value) {
    return new StatisticEntryImpl<>(name, StatisticValues.longValue(value));
  }

  private void setClient(GravitinoStatisticsUpdater updater, GravitinoClient client)
      throws ReflectiveOperationException {
    Field field = GravitinoStatisticsUpdater.class.getDeclaredField("gravitinoClient");
    field.setAccessible(true);
    field.set(updater, client);
  }
}
