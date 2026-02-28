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

package org.apache.gravitino.maintenance.optimizer.common.util;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobSubmitter;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.StrategyProvider;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.ConsoleMonitorCallback;
import org.apache.gravitino.maintenance.optimizer.monitor.callback.MonitorCallbackForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.TableJobRelationProviderForTest;
import org.apache.gravitino.maintenance.optimizer.monitor.job.dummy.DummyJobProvider;
import org.apache.gravitino.maintenance.optimizer.monitor.metrics.MetricsProviderForTest;
import org.apache.gravitino.maintenance.optimizer.recommender.job.GravitinoJobSubmitter;
import org.apache.gravitino.maintenance.optimizer.recommender.job.NoopJobSubmitter;
import org.apache.gravitino.maintenance.optimizer.recommender.statistics.GravitinoStatisticsProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.strategy.GravitinoStrategyProvider;
import org.apache.gravitino.maintenance.optimizer.recommender.table.GravitinoTableMetadataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestProviderUtils {

  @Test
  public void testCreateStrategyProviderInstance() {
    StrategyProvider strategyProvider =
        ProviderUtils.createStrategyProviderInstance(GravitinoStrategyProvider.NAME);
    Assertions.assertNotNull(strategyProvider);
    Assertions.assertTrue(strategyProvider instanceof GravitinoStrategyProvider);
  }

  @Test
  public void testCreateJobSubmitterInstance() {
    JobSubmitter jobSubmitter =
        ProviderUtils.createJobSubmitterInstance(GravitinoJobSubmitter.NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof GravitinoJobSubmitter);

    jobSubmitter = ProviderUtils.createJobSubmitterInstance(NoopJobSubmitter.NAME);
    Assertions.assertNotNull(jobSubmitter);
    Assertions.assertTrue(jobSubmitter instanceof NoopJobSubmitter);
  }

  @Test
  public void testCreateStatisticsProviderInstance() {
    StatisticsProvider statisticsProvider =
        ProviderUtils.createStatisticsProviderInstance(GravitinoStatisticsProvider.NAME);
    Assertions.assertNotNull(statisticsProvider);
    Assertions.assertTrue(statisticsProvider instanceof GravitinoStatisticsProvider);
  }

  @Test
  public void testCreateTableMetadataProviderInstance() {
    TableMetadataProvider tableMetadataProvider =
        ProviderUtils.createTableMetadataProviderInstance(GravitinoTableMetadataProvider.NAME);
    Assertions.assertNotNull(tableMetadataProvider);
    Assertions.assertTrue(tableMetadataProvider instanceof GravitinoTableMetadataProvider);
  }

  @Test
  public void testCreateMonitorProviders() {
    Assertions.assertTrue(
        ProviderUtils.createMetricsProviderInstance(MetricsProviderForTest.NAME)
            instanceof MetricsProviderForTest);
    Assertions.assertTrue(
        ProviderUtils.createTableJobRelationProviderInstance(TableJobRelationProviderForTest.NAME)
            instanceof TableJobRelationProviderForTest);
    DummyJobProvider dummyJobProvider =
        (DummyJobProvider)
            ProviderUtils.createTableJobRelationProviderInstance(DummyJobProvider.NAME);
    Assertions.assertNotNull(dummyJobProvider);
    Assertions.assertTrue(
        dummyJobProvider.jobIdentifiers(NameIdentifier.parse("catalog.db.table")).isEmpty());
    Assertions.assertTrue(
        ProviderUtils.createMonitorCallbackInstance(ConsoleMonitorCallback.NAME)
            instanceof ConsoleMonitorCallback);
    Assertions.assertTrue(
        ProviderUtils.createMonitorCallbackInstance(MonitorCallbackForTest.NAME)
            instanceof MonitorCallbackForTest);
  }
}
