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

package org.apache.gravitino.maintenance.storage.service;

import java.util.Optional;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestTableMaintenanceActionMetaService extends TestJDBCBackend {

  @TestTemplate
  void testInsertBindSampleFinishAndDeleteKeepsMetrics() {
    TableMaintenanceActionMetaService actionService =
        TableMaintenanceActionMetaService.getInstance();
    TableSnapshotMetricsMetaService metricsService = TableSnapshotMetricsMetaService.getInstance();

    long before = System.currentTimeMillis();
    OccupancyClaim claim =
        actionService.insertInFlight(
            10L, 20L, TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10));
    long after = System.currentTimeMillis();
    Assertions.assertNull(actionService.getById(claim.id()).jobId());
    Assertions.assertEquals(0L, actionService.getById(claim.id()).finishedAt());
    Assertions.assertTrue(claim.createdAt() >= before);
    Assertions.assertTrue(claim.createdAt() <= after);
    Assertions.assertEquals(claim.createdAt(), actionService.getById(claim.id()).createdAt());

    long jobId = 999L;
    actionService.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actionService.getByJobId(jobId).jobId());
    Assertions.assertEquals(0L, actionService.getByJobId(jobId).finishedAt());

    actionService.recordBeforeSnapshot(jobId, 100L);
    Assertions.assertEquals(100L, actionService.getByJobId(jobId).beforeSnapshotId());
    Assertions.assertThrows(
        IllegalStateException.class, () -> actionService.recordBeforeSnapshot(jobId, 101L));

    metricsService.insertMetrics(10L, 20L, 100L, "{\"mse\":1}");
    actionService.markFinished(jobId, 200L, System.currentTimeMillis());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.markFinished(jobId, 201L, System.currentTimeMillis()));
    metricsService.insertMetrics(10L, 20L, 200L, "{\"mse\":2}");

    actionService.deleteByJobId(jobId);

    Assertions.assertNull(actionService.getByJobId(jobId));
    Assertions.assertEquals(
        "{\"mse\":1}", metricsService.getMetrics(10L, 20L, 100L).metricsValue());
    Assertions.assertEquals(
        "{\"mse\":2}", metricsService.getMetrics(10L, 20L, 200L).metricsValue());
  }

  @TestTemplate
  void testTryAcquireRejectsLiveUnboundAndReclaimsExpired() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 11L;
    long policyId = 21L;
    long timeoutMs = 60_000L;
    long t0 = 1_700_000_300_000L;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, t0, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Assertions.assertEquals(t0, first.get().createdAt());
    Assertions.assertEquals(t0, actions.getById(first.get().id()).createdAt());
    Assertions.assertEquals(0L, actions.getById(first.get().id()).finishedAt());
    Assertions.assertTrue(
        actions.tryAcquireInFlight(tableId, policyId, t0 + 1_000L, timeoutMs).isEmpty());

    long reclaimAt = t0 + timeoutMs;
    Optional<OccupancyClaim> reclaimed =
        actions.tryAcquireInFlight(tableId, policyId, reclaimAt, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertEquals(first.get().id(), reclaimed.get().id());
    Assertions.assertEquals(reclaimAt, reclaimed.get().createdAt());
    Assertions.assertEquals(reclaimAt, actions.getById(reclaimed.get().id()).createdAt());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().id()).finishedAt());

    // Stale holder must bind with the claim from acquire, not a re-read of created_at.
    Assertions.assertThrows(
        IllegalStateException.class, () -> actions.bindJobId(first.get(), 1001L));
    actions.bindJobId(reclaimed.get(), 1002L);
    Assertions.assertEquals(1002L, actions.getById(reclaimed.get().id()).jobId());
  }
}
