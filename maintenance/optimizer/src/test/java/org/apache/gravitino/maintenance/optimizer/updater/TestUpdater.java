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

package org.apache.gravitino.maintenance.optimizer.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Comparator;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestUpdater {

  @AfterEach
  void tearDown() {
    StatisticsUpdaterForTest.reset();
    MetricsUpdaterForTest.reset();
  }

  @Test
  void testUpdateStatistics() {
    OptimizerEnv optimizerEnv =
        mockOptimizerEnv(StatisticsUpdaterForTest.NAME, MetricsUpdaterForTest.NAME);
    NameIdentifier identifier = NameIdentifier.of("catalog", "schema", "table");

    createUpdater(optimizerEnv)
        .update(StatisticsCalculatorForTest.NAME, List.of(identifier), UpdateType.STATISTICS);

    StatisticsUpdaterForTest statisticsUpdater = selectStatisticsUpdater();
    MetricsUpdaterForTest metricsUpdater = selectMetricsUpdater();
    assertNotNull(statisticsUpdater);
    assertNotNull(metricsUpdater);
    assertEquals(1, statisticsUpdater.tableUpdates());
    assertEquals(1, statisticsUpdater.partitionUpdates());
    assertEquals(0, metricsUpdater.tableUpdates());
    assertEquals(0, metricsUpdater.jobUpdates());
  }

  @Test
  void testUpdateMetricsWithJobStatistics() {
    OptimizerEnv optimizerEnv =
        mockOptimizerEnv(StatisticsUpdaterForTest.NAME, MetricsUpdaterForTest.NAME);
    NameIdentifier identifier = NameIdentifier.of("catalog", "schema", "table");

    createUpdater(optimizerEnv)
        .update(StatisticsCalculatorForTest.NAME, List.of(identifier), UpdateType.METRICS);

    StatisticsUpdaterForTest statisticsUpdater = selectStatisticsUpdater();
    MetricsUpdaterForTest metricsUpdater = selectMetricsUpdater();
    assertNotNull(statisticsUpdater);
    assertNotNull(metricsUpdater);
    assertEquals(0, statisticsUpdater.tableUpdates());
    assertEquals(0, statisticsUpdater.partitionUpdates());
    assertEquals(2, metricsUpdater.tableUpdates());
    assertEquals(1, metricsUpdater.jobUpdates());
  }

  @Test
  void testUpdateAllStatistics() {
    OptimizerEnv optimizerEnv =
        mockOptimizerEnv(StatisticsUpdaterForTest.NAME, MetricsUpdaterForTest.NAME);

    createUpdater(optimizerEnv).updateAll(StatisticsCalculatorForTest.NAME, UpdateType.STATISTICS);

    StatisticsUpdaterForTest statisticsUpdater = selectStatisticsUpdater();
    MetricsUpdaterForTest metricsUpdater = selectMetricsUpdater();
    assertNotNull(statisticsUpdater);
    assertNotNull(metricsUpdater);
    assertEquals(1, statisticsUpdater.tableUpdates());
    assertEquals(1, statisticsUpdater.partitionUpdates());
    assertEquals(0, metricsUpdater.tableUpdates());
    assertEquals(0, metricsUpdater.jobUpdates());
  }

  @Test
  void testUpdateAllMetricsWithJobStatistics() {
    OptimizerEnv optimizerEnv =
        mockOptimizerEnv(StatisticsUpdaterForTest.NAME, MetricsUpdaterForTest.NAME);

    createUpdater(optimizerEnv).updateAll(StatisticsCalculatorForTest.NAME, UpdateType.METRICS);

    StatisticsUpdaterForTest statisticsUpdater = selectStatisticsUpdater();
    MetricsUpdaterForTest metricsUpdater = selectMetricsUpdater();
    assertNotNull(statisticsUpdater);
    assertNotNull(metricsUpdater);
    assertEquals(0, statisticsUpdater.tableUpdates());
    assertEquals(0, statisticsUpdater.partitionUpdates());
    assertEquals(2, metricsUpdater.tableUpdates());
    assertEquals(1, metricsUpdater.jobUpdates());
  }

  @Test
  void testCloseClosesProviders() throws Exception {
    OptimizerEnv optimizerEnv =
        mockOptimizerEnv(StatisticsUpdaterForTest.NAME, MetricsUpdaterForTest.NAME);

    Updater updater = createUpdater(optimizerEnv);
    updater.close();

    StatisticsUpdaterForTest statisticsUpdater = selectStatisticsUpdater();
    MetricsUpdaterForTest metricsUpdater = selectMetricsUpdater();
    assertNotNull(statisticsUpdater);
    assertNotNull(metricsUpdater);
    assertEquals(1, statisticsUpdater.closeCalls());
    assertEquals(1, metricsUpdater.closeCalls());
  }

  private Updater createUpdater(OptimizerEnv optimizerEnv) {
    return new Updater(optimizerEnv);
  }

  private StatisticsUpdaterForTest selectStatisticsUpdater() {
    return StatisticsUpdaterForTest.instances().stream()
        .max(
            Comparator.comparingInt(
                updater ->
                    updater.tableUpdates() + updater.partitionUpdates() + updater.closeCalls()))
        .orElse(null);
  }

  private MetricsUpdaterForTest selectMetricsUpdater() {
    return MetricsUpdaterForTest.instances().stream()
        .max(
            Comparator.comparingInt(
                updater -> updater.tableUpdates() + updater.jobUpdates() + updater.closeCalls()))
        .orElse(null);
  }

  private OptimizerEnv mockOptimizerEnv(String statisticsUpdater, String metricsUpdater) {
    OptimizerConfig config = Mockito.mock(OptimizerConfig.class);
    Mockito.when(config.get(OptimizerConfig.STATISTICS_UPDATER_CONFIG))
        .thenReturn(statisticsUpdater);
    Mockito.when(config.get(OptimizerConfig.METRICS_UPDATER_CONFIG)).thenReturn(metricsUpdater);
    OptimizerEnv optimizerEnv = Mockito.mock(OptimizerEnv.class);
    Mockito.when(optimizerEnv.config()).thenReturn(config);
    return optimizerEnv;
  }
}
