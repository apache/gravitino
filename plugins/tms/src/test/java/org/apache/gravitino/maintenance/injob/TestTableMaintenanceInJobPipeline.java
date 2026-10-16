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

import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceInJobPipeline extends TestJDBCBackend {

  private static final String METRICS_JSON = "{\"mse\":1}";

  private TableMaintenanceActionMetaService actionMetaService;
  private TableMaintenanceInJobPipeline maintainPipeline;
  private TableMaintenanceInJobPipeline skipPipeline;

  @BeforeEach
  void setUp() {
    actionMetaService = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceInJobSampler sampler =
        new TableMaintenanceInJobSampler(
            TableSnapshotMetricsMetaService.getInstance(), actionMetaService);
    maintainPipeline =
        new TableMaintenanceInJobPipeline(
            sampler, (tableId, policyId, snapshotId, metricsJson) -> RecommendDecision.MAINTAIN);
    skipPipeline =
        new TableMaintenanceInJobPipeline(
            sampler, (tableId, policyId, snapshotId, metricsJson) -> RecommendDecision.SKIP);
  }

  @TestTemplate
  void testContinuesWhenNoDriftAndRecommenderMaintains() {
    long jobId = 2001L;
    insertBoundAction(jobId, 10L, 20L);
    InJobGateOutcome outcome =
        maintainPipeline.gateCronJob(jobId, 10L, 20L, 100L, METRICS_JSON, () -> 100L);
    Assertions.assertTrue(outcome.continueMaintenance());
    Assertions.assertNull(outcome.stopReason());
  }

  @TestTemplate
  void testStopsOnDriftBeforeRecommend() {
    long jobId = 2002L;
    insertBoundAction(jobId, 11L, 21L);
    InJobGateOutcome outcome =
        maintainPipeline.gateCronJob(jobId, 11L, 21L, 200L, METRICS_JSON, () -> 201L);
    Assertions.assertFalse(outcome.continueMaintenance());
    Assertions.assertEquals(InJobGateOutcome.StopReason.SNAPSHOT_DRIFT, outcome.stopReason());
  }

  @TestTemplate
  void testStopsWhenRecommenderSkips() {
    long jobId = 2003L;
    insertBoundAction(jobId, 12L, 22L);
    InJobGateOutcome outcome =
        skipPipeline.gateCronJob(jobId, 12L, 22L, 300L, METRICS_JSON, () -> 300L);
    Assertions.assertFalse(outcome.continueMaintenance());
    Assertions.assertEquals(InJobGateOutcome.StopReason.RECOMMENDER_SKIP, outcome.stopReason());
  }

  @TestTemplate
  void testCommitChainTypeRecommendGate() {
    InJobGateOutcome maintain =
        maintainPipeline.gateCommitChainType(1L, 2L, 400L, METRICS_JSON, () -> 400L);
    Assertions.assertTrue(maintain.continueMaintenance());

    InJobGateOutcome skip =
        skipPipeline.gateCommitChainType(1L, 2L, 401L, METRICS_JSON, () -> 401L);
    Assertions.assertEquals(InJobGateOutcome.StopReason.RECOMMENDER_SKIP, skip.stopReason());
  }

  private void insertBoundAction(long jobId, long tableId, long policyId) {
    actionMetaService.bindJobId(
        actionMetaService.insertInFlight(
            tableId, policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS),
        jobId);
  }
}
