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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.StatisticEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.util.PartitionUtils;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.stats.SupportsStatistics;
import org.awaitility.Awaitility;
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
    updater.setGravitinoClientForTest(client);
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
    updater.setGravitinoClientForTest(client);

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
    updater.setGravitinoClientForTest(client);
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

  @Test
  void testTableStatisticConflictRetriesSameValues() {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    SupportsStatistics statistics = mockTableStatistics(updater);
    Map<String, StatisticValue<?>> values = Map.of("row_count", StatisticValues.longValue(10L));
    Mockito.doThrow(new OptimisticLockException("first conflict"))
        .doThrow(new OptimisticLockException("second conflict"))
        .doNothing()
        .when(statistics)
        .updateStatistics(values);

    updater.updateTableStatistics(
        NameIdentifier.of("catalog", "db", "table"), List.of(stat("row_count", 10L)));

    Mockito.verify(statistics, Mockito.times(3)).updateStatistics(values);
  }

  @Test
  void testTableStatisticConflictStopsAtAttemptLimit() {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    SupportsStatistics statistics = mockTableStatistics(updater);
    OptimisticLockException conflict = new OptimisticLockException("persistent conflict");
    Mockito.doThrow(conflict).when(statistics).updateStatistics(Mockito.anyMap());

    Assertions.assertSame(
        conflict,
        Assertions.assertThrows(
            OptimisticLockException.class,
            () ->
                updater.updateTableStatistics(
                    NameIdentifier.of("catalog", "db", "table"), List.of(stat("row_count", 10L)))));
    Mockito.verify(statistics, Mockito.times(3)).updateStatistics(Mockito.anyMap());
  }

  @Test
  void testTableStatisticNonConflictIsNotRetried() {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    SupportsStatistics statistics = mockTableStatistics(updater);
    IllegalArgumentException failure = new IllegalArgumentException("statistic is not modifiable");
    Mockito.doThrow(failure).when(statistics).updateStatistics(Mockito.anyMap());

    Assertions.assertSame(
        failure,
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                updater.updateTableStatistics(
                    NameIdentifier.of("catalog", "db", "table"), List.of(stat("row_count", 10L)))));
    Mockito.verify(statistics).updateStatistics(Mockito.anyMap());
  }

  @Test
  void testTableStatisticConflictHonorsExistingInterruption() {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    SupportsStatistics statistics = mockTableStatistics(updater);
    OptimisticLockException conflict = new OptimisticLockException("conflict during cancellation");
    Mockito.doThrow(conflict).when(statistics).updateStatistics(Mockito.anyMap());
    Thread.currentThread().interrupt();
    try {
      Assertions.assertSame(
          conflict,
          Assertions.assertThrows(
              OptimisticLockException.class,
              () ->
                  updater.updateTableStatistics(
                      NameIdentifier.of("catalog", "db", "table"),
                      List.of(stat("row_count", 10L)))));
      Assertions.assertTrue(Thread.currentThread().isInterrupted());
      Mockito.verify(statistics).updateStatistics(Mockito.anyMap());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testTableStatisticInterruptionDuringBackoffStopsRetry() throws Exception {
    GravitinoStatisticsUpdater updater = new GravitinoStatisticsUpdater();
    SupportsStatistics statistics = mockTableStatistics(updater);
    OptimisticLockException conflict = new OptimisticLockException("conflict before interruption");
    Mockito.doThrow(conflict).when(statistics).updateStatistics(Mockito.anyMap());
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
    Thread worker =
        new Thread(
            () -> {
              try {
                updater.updateTableStatistics(
                    NameIdentifier.of("catalog", "db", "table"), List.of(stat("row_count", 10L)));
              } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
              }
            });
    worker.start();
    try {
      Awaitility.await()
          .pollInterval(1, TimeUnit.MILLISECONDS)
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> worker.getState() == Thread.State.TIMED_WAITING);
      worker.interrupt();
      worker.join(5000L);
      Assertions.assertFalse(worker.isAlive());
      Assertions.assertSame(conflict, failure.get());
      Assertions.assertTrue(interrupted.get());
      Assertions.assertInstanceOf(InterruptedException.class, conflict.getSuppressed()[0]);
      Mockito.verify(statistics).updateStatistics(Mockito.anyMap());
    } finally {
      worker.interrupt();
      worker.join(5000L);
    }
  }

  private SupportsStatistics mockTableStatistics(GravitinoStatisticsUpdater updater) {
    GravitinoClient client = Mockito.mock(GravitinoClient.class, Mockito.RETURNS_DEEP_STUBS);
    updater.setGravitinoClientForTest(client);
    return client
        .loadCatalog("catalog")
        .asTableCatalog()
        .loadTable(NameIdentifier.of("db", "table"))
        .supportsStatistics();
  }

  private StatisticEntry<?> stat(String name, long value) {
    return new StatisticEntryImpl<>(name, StatisticValues.longValue(value));
  }
}
