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

package org.apache.gravitino.maintenance.job;

import org.apache.gravitino.maintenance.storage.service.TableMaintenanceJobMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceInJobPipeline extends TestJDBCBackend {

  private TableMaintenanceJobMetaService jobMetaService;
  private TableMaintenanceInJobPipeline pipeline;

  @BeforeEach
  void setUp() {
    jobMetaService = TableMaintenanceJobMetaService.getInstance();
    TableMaintenanceInJobSampler sampler =
        new TableMaintenanceInJobSampler(
            TableSnapshotMetricsMetaService.getInstance(), jobMetaService);
    pipeline = new TableMaintenanceInJobPipeline(sampler, new FileCountThresholdRecommender(5));
  }

  @TestTemplate
  void testContinuesWhenNoDriftAndRecommenderMaintains() {
    long jobId = 2001L;
    jobMetaService.insertJob(jobId, 10L, 20L);
    InJobSampleOutcome outcome =
        pipeline.sampleDriftAndRecommendCronJob(
            jobId, 10L, 20L, 100L, "{\"fileCount\":5}", () -> 100L);
    Assertions.assertTrue(outcome.continueMaintenance());
    Assertions.assertNull(outcome.stopReason());
  }

  @TestTemplate
  void testStopsOnDriftBeforeRecommend() {
    long jobId = 2002L;
    jobMetaService.insertJob(jobId, 11L, 21L);
    InJobSampleOutcome outcome =
        pipeline.sampleDriftAndRecommendCronJob(
            jobId, 11L, 21L, 200L, "{\"fileCount\":100}", () -> 201L);
    Assertions.assertFalse(outcome.continueMaintenance());
    Assertions.assertEquals(InJobSampleOutcome.StopReason.SNAPSHOT_DRIFT, outcome.stopReason());
  }

  @TestTemplate
  void testStopsWhenRecommenderSkips() {
    long jobId = 2003L;
    jobMetaService.insertJob(jobId, 12L, 22L);
    InJobSampleOutcome outcome =
        pipeline.sampleDriftAndRecommendCronJob(
            jobId, 12L, 22L, 300L, "{\"fileCount\":2}", () -> 300L);
    Assertions.assertFalse(outcome.continueMaintenance());
    Assertions.assertEquals(InJobSampleOutcome.StopReason.RECOMMENDER_SKIP, outcome.stopReason());
  }

  @TestTemplate
  void testCommitChainTypeRecommendGate() {
    InJobSampleOutcome maintain =
        pipeline.gateCommitChainTypeAfterSampleAndRecommend(
            1L, 2L, 400L, "{\"fileCount\":9}", () -> 400L);
    Assertions.assertTrue(maintain.continueMaintenance());

    InJobSampleOutcome skip =
        pipeline.gateCommitChainTypeAfterSampleAndRecommend(
            1L, 2L, 401L, "{\"fileCount\":1}", () -> 401L);
    Assertions.assertEquals(InJobSampleOutcome.StopReason.RECOMMENDER_SKIP, skip.stopReason());
  }
}
