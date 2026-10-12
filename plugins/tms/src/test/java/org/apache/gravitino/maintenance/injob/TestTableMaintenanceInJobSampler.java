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

package org.apache.gravitino.maintenance.injob;

import org.apache.gravitino.maintenance.storage.po.TableSnapshotMetricsPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceInJobSampler extends TestJDBCBackend {

  private static final String METRICS_JSON = "{\"mse\":3}";

  private TableMaintenanceInJobSampler sampler;
  private TableMaintenanceJobMetaService jobMetaService;
  private TableSnapshotMetricsMetaService metricsMetaService;

  @BeforeEach
  void setUp() {
    jobMetaService = TableMaintenanceJobMetaService.getInstance();
    metricsMetaService = TableSnapshotMetricsMetaService.getInstance();
    sampler = new TableMaintenanceInJobSampler(metricsMetaService, jobMetaService);
  }

  @TestTemplate
  void testCronJobSamplePersistsAndContinuesWhenNoDrift() {
    long jobId = 1001L;
    jobMetaService.insertJob(jobId, 1L, 2L);
    InJobGateOutcome outcome =
        sampler.sampleAndGateCronJob(jobId, 1L, 2L, 100L, METRICS_JSON, () -> 100L);

    Assertions.assertTrue(outcome.continueMaintenance());
    Assertions.assertNull(outcome.stopReason());
    TableSnapshotMetricsPO stored = metricsMetaService.getMetrics(1L, 2L, 100L);
    Assertions.assertNotNull(stored);
    Assertions.assertEquals(METRICS_JSON, stored.metricsValue());
    Assertions.assertEquals(100L, jobMetaService.getJob(jobId).beforeSnapshotId());
  }

  @TestTemplate
  void testCronJobEndsEarlyWhenSnapshotDriftsAfterSample() {
    long jobId = 1002L;
    jobMetaService.insertJob(jobId, 3L, 4L);
    InJobGateOutcome outcome =
        sampler.sampleAndGateCronJob(jobId, 3L, 4L, 200L, METRICS_JSON, () -> 201L);

    Assertions.assertFalse(outcome.continueMaintenance());
    Assertions.assertEquals(InJobGateOutcome.StopReason.SNAPSHOT_DRIFT, outcome.stopReason());
    Assertions.assertEquals(200L, outcome.sampledSnapshotId());
    Assertions.assertEquals(201L, outcome.headSnapshotId());
    Assertions.assertNotNull(metricsMetaService.getMetrics(3L, 4L, 200L));
  }

  @TestTemplate
  void testCronJobPutMetricsOverwritesExistingRow() {
    long jobId = 1003L;
    jobMetaService.insertJob(jobId, 5L, 6L);
    metricsMetaService.insertMetrics(5L, 6L, 300L, "{\"mse\":1}");
    InJobGateOutcome outcome =
        sampler.sampleAndGateCronJob(jobId, 5L, 6L, 300L, "{\"mse\":2}", () -> 300L);
    Assertions.assertTrue(outcome.continueMaintenance());
    Assertions.assertEquals(
        "{\"mse\":2}", metricsMetaService.getMetrics(5L, 6L, 300L).metricsValue());
  }

  @TestTemplate
  void testCommitChainTypeGateWithoutReSample() {
    InJobGateOutcome ok = sampler.gateCommitChainType(300L, () -> 300L);
    Assertions.assertTrue(ok.continueMaintenance());

    InJobGateOutcome drift = sampler.gateCommitChainType(300L, () -> 301L);
    Assertions.assertFalse(drift.continueMaintenance());
    Assertions.assertEquals(InJobGateOutcome.StopReason.SNAPSHOT_DRIFT, drift.stopReason());
  }
}
