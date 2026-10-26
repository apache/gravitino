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

import java.util.List;
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
    Assertions.assertTrue(claim.createdAt() >= before - 5_000L);
    Assertions.assertTrue(claim.createdAt() <= after + 5_000L);
    Assertions.assertEquals(claim.createdAt(), actionService.getById(claim.id()).createdAt());

    long jobId = 999L;
    long tableId = 10L;
    long policyId = 20L;
    actionService.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actionService.getByJobIdAndTableId(jobId, tableId).jobId());
    Assertions.assertEquals(0L, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());

    actionService.recordBeforeSnapshot(jobId, tableId, policyId, 100L);
    Assertions.assertEquals(
        100L, actionService.getByJobIdAndTableId(jobId, tableId).beforeSnapshotId());
    actionService.recordBeforeSnapshot(jobId, tableId, policyId, 100L);
    Assertions.assertEquals(
        100L, actionService.getByJobIdAndTableId(jobId, tableId).beforeSnapshotId());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> actionService.recordBeforeSnapshot(jobId, tableId, policyId, 101L));

    metricsService.insertMetrics(tableId, policyId, 100L, "{\"mse\":1}");
    actionService.markFinished(jobId, tableId, policyId, 200L, System.currentTimeMillis());
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            actionService.markFinished(jobId, tableId, policyId, 201L, System.currentTimeMillis()));
    metricsService.insertMetrics(tableId, policyId, 200L, "{\"mse\":2}");

    actionService.deleteByJobId(jobId);

    Assertions.assertTrue(actionService.listByJobId(jobId).isEmpty());
    Assertions.assertEquals(
        "{\"mse\":1}", metricsService.getMetrics(tableId, policyId, 100L).metricsValue());
    Assertions.assertEquals(
        "{\"mse\":2}", metricsService.getMetrics(tableId, policyId, 200L).metricsValue());
  }

  @TestTemplate
  void testOneJobBindsMultipleTablesAndMarkFinishedClosesPerRow() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long jobId = 777L;
    OccupancyClaim claimA = actions.insertInFlight(101L, 201L, config);
    OccupancyClaim claimB = actions.insertInFlight(102L, 202L, config);
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);

    Assertions.assertEquals(2, actions.listByJobId(jobId).size());
    Assertions.assertThrows(IllegalStateException.class, () -> actions.getByJobId(jobId));

    actions.recordBeforeSnapshot(jobId, 101L, 201L, 11L);
    actions.recordBeforeSnapshot(jobId, 102L, 202L, 22L);

    long finishedAtA = System.currentTimeMillis();
    actions.markFinished(jobId, 101L, 201L, null, finishedAtA);
    Assertions.assertEquals(finishedAtA, actions.getByJobIdAndTableId(jobId, 101L).finishedAt());
    Assertions.assertEquals(0L, actions.getByJobIdAndTableId(jobId, 102L).finishedAt());

    long finishedAtB = finishedAtA + 10;
    actions.markFinished(jobId, 102L, 202L, 33L, finishedAtB);
    Assertions.assertEquals(finishedAtB, actions.getByJobIdAndTableId(jobId, 102L).finishedAt());
    Assertions.assertEquals(33L, actions.getByJobIdAndTableId(jobId, 102L).afterSnapshotId());
    // Idempotent null-after finish for already closed A.
    actions.markFinished(jobId, 101L, 201L, null, finishedAtA + 1);
  }

  @TestTemplate
  void testMarkFinishedBackfillsAfterSnapshotWhenAlreadyClosed() {
    TableMaintenanceActionMetaService actionService =
        TableMaintenanceActionMetaService.getInstance();
    long jobId = 1999L;
    long tableId = 12L;
    long policyId = 22L;
    OccupancyClaim claim =
        actionService.insertInFlight(
            tableId, policyId, TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10));
    actionService.bindJobId(claim, jobId);
    actionService.recordBeforeSnapshot(jobId, tableId, policyId, 100L);

    long reclaimFinishedAt = System.currentTimeMillis();
    actionService.markFinished(jobId, tableId, policyId, null, reclaimFinishedAt);
    Assertions.assertEquals(
        reclaimFinishedAt, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());
    Assertions.assertNull(actionService.getByJobIdAndTableId(jobId, tableId).afterSnapshotId());

    actionService.markFinished(jobId, tableId, policyId, 200L, System.currentTimeMillis());
    Assertions.assertEquals(
        200L, actionService.getByJobIdAndTableId(jobId, tableId).afterSnapshotId());
    Assertions.assertEquals(
        reclaimFinishedAt, actionService.getByJobIdAndTableId(jobId, tableId).finishedAt());
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            actionService.markFinished(jobId, tableId, policyId, 201L, System.currentTimeMillis()));
  }

  @TestTemplate
  void testBindJobIdIdempotentWhenAlreadyBoundToSameJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 31L;
    long policyId = 41L;
    long jobId = 5100L;
    OccupancyClaim claim = actions.insertInFlight(tableId, policyId, config);
    actions.bindJobId(claim, jobId);
    // Spark lazy-bound first; submit-side bind must not fail.
    actions.bindJobId(claim, jobId);
    Assertions.assertEquals(jobId, actions.getByJobIdAndTableId(jobId, tableId).jobId());
  }

  @TestTemplate
  void testBindJobIdsIdempotentWhenAlreadyBoundToSameJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 37L;
    long policyA = 48L;
    long policyB = 49L;
    long jobId = 5700L;
    OccupancyClaim claimA = actions.insertInFlight(tableId, policyA, config);
    OccupancyClaim claimB = actions.insertInFlight(tableId, policyB, config);
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);
    // Spark may lazy-bind before commit-chain submit-side bindJobIds.
    actions.bindJobIds(List.of(claimA, claimB), jobId);
    Assertions.assertEquals(jobId, actions.getById(claimA.id()).jobId());
    Assertions.assertEquals(jobId, actions.getById(claimB.id()).jobId());
  }

  @TestTemplate
  void testBindJobIdFailsWhenBoundToDifferentJob() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    OccupancyClaim claim = actions.insertInFlight(32L, 42L, config);
    actions.bindJobId(claim, 5200L);
    Assertions.assertThrows(IllegalStateException.class, () -> actions.bindJobId(claim, 5201L));
  }

  @TestTemplate
  void testBindJobIdIfUnboundLazyBindsSoleClaim() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 33L;
    long policyId = 43L;
    long jobId = 5300L;
    actions.insertInFlight(tableId, policyId, config);

    Assertions.assertTrue(actions.bindJobIdIfUnbound(tableId, policyId, jobId));
    Assertions.assertEquals(
        jobId, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId).jobId());
    // Already bound to same job — still reports success via idempotent bind.
    Assertions.assertTrue(actions.bindJobIdIfUnbound(tableId, policyId, jobId));
  }

  @TestTemplate
  void testBindJobIdIfUnboundDisambiguatesPoliciesOnSameTable() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 34L;
    long policyA = 44L;
    long policyB = 45L;
    long jobId = 5400L;
    // Two policies → two soft slots on the same table; policyId selects which claim to bind.
    actions.insertInFlight(tableId, policyA, config);
    actions.insertInFlight(tableId, policyB, config);

    Assertions.assertTrue(actions.bindJobIdIfUnbound(tableId, policyA, jobId));
    Assertions.assertEquals(
        jobId, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyA).jobId());
    Assertions.assertNull(actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB));
    Assertions.assertTrue(actions.bindJobIdIfUnbound(tableId, policyB, jobId));
    Assertions.assertEquals(
        jobId, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB).jobId());
  }

  @TestTemplate
  void testOneJobMultiplePoliciesOnSameTableRecordAndFinishPerPolicy() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    TableMaintenanceConfig config = TableMaintenanceConfig.forTest(1, 1, 1, 500L, 1_000L, 4, 10);
    long tableId = 36L;
    long policyA = 46L;
    long policyB = 47L;
    long jobId = 5600L;
    OccupancyClaim claimA = actions.insertInFlight(tableId, policyA, config);
    OccupancyClaim claimB = actions.insertInFlight(tableId, policyB, config);
    actions.bindJobId(claimA, jobId);
    actions.bindJobId(claimB, jobId);

    actions.recordBeforeSnapshot(jobId, tableId, policyA, 11L);
    actions.recordBeforeSnapshot(jobId, tableId, policyB, 22L);
    Assertions.assertEquals(
        11L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyA).beforeSnapshotId());
    Assertions.assertEquals(
        22L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB).beforeSnapshotId());

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(jobId, tableId, policyA, null, finishedAt);
    Assertions.assertEquals(
        finishedAt, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyA).finishedAt());
    Assertions.assertEquals(
        0L, actions.getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyB).finishedAt());
  }

  @TestTemplate
  void testBindJobIdIfUnboundReturnsFalseWhenNoClaim() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    Assertions.assertFalse(actions.bindJobIdIfUnbound(35L, 55L, 5500L));
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

  @TestTemplate
  void testDeleteUnboundClaimFencesAgainstReclaim() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 12L;
    long policyId = 22L;
    long timeoutMs = 200L;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Assertions.assertTrue(actions.deleteUnboundClaim(first.get()));
    Assertions.assertNull(actions.getById(first.get().id()));

    Optional<OccupancyClaim> second = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(second.isPresent());
    Thread.sleep(timeoutMs + 50L);
    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertEquals(second.get().id(), reclaimed.get().id());
    Assertions.assertTrue(reclaimed.get().createdAt() >= second.get().createdAt());

    // Stale submitter cleanup must not delete the reclaimed live claim.
    Assertions.assertFalse(actions.deleteUnboundClaim(second.get()));
    Assertions.assertNotNull(actions.getById(reclaimed.get().id()));
    Assertions.assertNull(actions.getById(reclaimed.get().id()).jobId());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().id()).finishedAt());

    Assertions.assertTrue(actions.deleteUnboundClaim(reclaimed.get()));
    Assertions.assertNull(actions.getById(reclaimed.get().id()));
  }

  @TestTemplate
  void testDeleteClaimsFencesAgainstReclaim() throws Exception {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 38L;
    long policyId = 50L;
    long timeoutMs = 200L;

    Optional<OccupancyClaim> first = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(first.isPresent());
    Thread.sleep(timeoutMs + 50L);
    Optional<OccupancyClaim> reclaimed = actions.tryAcquireInFlight(tableId, policyId, timeoutMs);
    Assertions.assertTrue(reclaimed.isPresent());
    Assertions.assertEquals(first.get().id(), reclaimed.get().id());
    Assertions.assertTrue(reclaimed.get().createdAt() >= first.get().createdAt());

    // Failed submit cleanup uses deleteClaims; stale created_at must not remove live claim.
    actions.deleteClaims(List.of(first.get()));
    Assertions.assertNotNull(actions.getById(reclaimed.get().id()));
    Assertions.assertNull(actions.getById(reclaimed.get().id()).jobId());
    Assertions.assertEquals(0L, actions.getById(reclaimed.get().id()).finishedAt());

    actions.deleteClaims(List.of(reclaimed.get()));
    Assertions.assertNull(actions.getById(reclaimed.get().id()));
  }

  @TestTemplate
  void testTryAcquireBatchClaimsAllPoliciesOrNone() {
    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long tableId = 31L;
    long policyA = 41L;
    long policyB = 42L;
    long timeoutMs = 60_000L;

    Optional<List<OccupancyClaim>> batch =
        actions.tryAcquireInFlightBatch(tableId, List.of(policyA, policyB), timeoutMs);
    Assertions.assertTrue(batch.isPresent());
    Assertions.assertEquals(2, batch.get().size());
    Assertions.assertEquals(0L, actions.getById(batch.get().get(0).id()).finishedAt());
    Assertions.assertEquals(0L, actions.getById(batch.get().get(1).id()).finishedAt());

    // Partial hold blocks the whole chain.
    Assertions.assertTrue(
        actions.tryAcquireInFlightBatch(tableId, List.of(policyA, policyB), timeoutMs).isEmpty());
    Assertions.assertTrue(actions.tryAcquireInFlight(tableId, policyA, timeoutMs).isEmpty());

    long jobId = 3100L;
    actions.bindJobIds(batch.get(), jobId);
    Assertions.assertEquals(jobId, actions.getById(batch.get().get(0).id()).jobId());
    Assertions.assertEquals(jobId, actions.getById(batch.get().get(1).id()).jobId());

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(jobId, tableId, policyA, 500L, finishedAt);
    actions.markFinished(jobId, tableId, policyB, 501L, finishedAt + 1);
    Assertions.assertTrue(actions.getById(batch.get().get(0).id()).finishedAt() > 0);
    Assertions.assertTrue(actions.getById(batch.get().get(1).id()).finishedAt() > 0);
  }
}
