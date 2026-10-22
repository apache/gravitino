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
    // DB clock may skew slightly from the JVM; allow a small window.
    Assertions.assertTrue(claim.createdAt() >= before - 5_000L);
    Assertions.assertTrue(claim.createdAt() <= after + 5_000L);
    Assertions.assertEquals(claim.createdAt(), actionService.getById(claim.id()).createdAt());

    long jobId = 999L;
    long tableId = 10L;
    actionService.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actionService.getByJobIdAndTableId(jobId, tableId).jobId());
    Assertions.assertEquals(0L, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());

    actionService.recordBeforeSnapshot(jobId, tableId, 100L);
    Assertions.assertEquals(
        100L, actionService.getByJobIdAndTableId(jobId, tableId).beforeSnapshotId());
    // Same S0 is idempotent (in-job / cron retry after a partial sample).
    actionService.recordBeforeSnapshot(jobId, tableId, 100L);
    Assertions.assertEquals(
        100L, actionService.getByJobIdAndTableId(jobId, tableId).beforeSnapshotId());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.recordBeforeSnapshot(jobId, tableId, 101L));

    metricsService.insertMetrics(tableId, 20L, 100L, "{\"mse\":1}");
    actionService.markFinished(jobId, 200L, System.currentTimeMillis());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.markFinished(jobId, 201L, System.currentTimeMillis()));
    metricsService.insertMetrics(tableId, 20L, 200L, "{\"mse\":2}");

    actionService.deleteByJobId(jobId);

    Assertions.assertTrue(actionService.listByJobId(jobId).isEmpty());
    Assertions.assertEquals(
        "{\"mse\":1}", metricsService.getMetrics(tableId, 20L, 100L).metricsValue());
    Assertions.assertEquals(
        "{\"mse\":2}", metricsService.getMetrics(tableId, 20L, 200L).metricsValue());
  }

  @TestTemplate
  void testOneJobBindsMultipleTablesAndMarkFinishedClosesAll() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long jobId = 777L;
    OccupancyClaim claimA = actions.insertInFlight(101L, 201L, config);
    OccupancyClaim claimB = actions.insertInFlight(102L, 202L, config);
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);

    Assertions.assertEquals(2, actions.listByJobId(jobId).size());
    Assertions.assertThrows(IllegalStateException.class, () -> actions.getByJobId(jobId));

    actions.recordBeforeSnapshot(jobId, 101L, 11L);
    actions.recordBeforeSnapshot(jobId, 102L, 22L);
    Assertions.assertEquals(11L, actions.getByJobIdAndTableId(jobId, 101L).beforeSnapshotId());
    Assertions.assertEquals(22L, actions.getByJobIdAndTableId(jobId, 102L).beforeSnapshotId());

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(jobId, null, finishedAt);
    Assertions.assertEquals(finishedAt, actions.getByJobIdAndTableId(jobId, 101L).finishedAt());
    Assertions.assertEquals(finishedAt, actions.getByJobIdAndTableId(jobId, 102L).finishedAt());
    // Idempotent when all rows already finished with null after snapshot.
    actions.markFinished(jobId, null, finishedAt + 1);
  }

  @TestTemplate
  void testMarkFinishedBackfillsAfterSnapshotWhenAlreadyClosed() {
    TableMaintenanceActionMetaService actionService =
        TableMaintenanceActionMetaService.getInstance();
    long jobId = 1999L;
    long tableId = 12L;
    OccupancyClaim claim =
        actionService.insertInFlight(
            tableId, 22L, TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10));
    actionService.bindJobId(claim, jobId);
    actionService.recordBeforeSnapshot(jobId, tableId, 100L);

    long reclaimFinishedAt = System.currentTimeMillis();
    // Failsafe reclaim closes with null S1.
    actionService.markFinished(jobId, null, reclaimFinishedAt);
    Assertions.assertEquals(
        reclaimFinishedAt, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());
    Assertions.assertNull(actionService.getByJobIdAndTableId(jobId, tableId).afterSnapshotId());

    // Late finish listener still records the real terminal snapshot.
    actionService.markFinished(jobId, 200L, System.currentTimeMillis());
    Assertions.assertEquals(
        200L, actionService.getByJobIdAndTableId(jobId, tableId).afterSnapshotId());
    Assertions.assertEquals(
        reclaimFinishedAt, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.markFinished(jobId, 201L, System.currentTimeMillis()));
  }

  @TestTemplate
  void testTryAcquireRejectsLiveUnboundAndReclaimsExpired() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 11L;
    long policyId = 21L;
    long timeoutMs = 200L;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Assertions.assertEquals(0L, actions.getById(first.get().id()).finishedAt());
    Assertions.assertTrue(actions.tryAcquireInFlight(tableId, policyId, timeoutMs).isEmpty());

    Thread.sleep(timeoutMs + 50L);

    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertEquals(first.get().id(), reclaimed.get().id());
    Assertions.assertTrue(reclaimed.get().createdAt() >= first.get().createdAt());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().id()).finishedAt());

    Assertions.assertThrows(
        IllegalStateException.class, () -> actions.bindJobId(first.get(), 1001L));
    actions.bindJobId(reclaimed.get(), 1002L);
    Assertions.assertEquals(1002L, actions.getById(reclaimed.get().id()).jobId());
  }
}
